package mongo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Broker represents a Redis broker
type Broker struct {
	common.Broker

	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup

	client *mongo.Client
	dtc    *mongo.Collection
	ptc    *mongo.Collection
	once   sync.Once
}

// New creates new Broker instance
func New(cnf *config.Config) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.once = sync.Once{}
	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (result bool, err error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = 1
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Connect the server to make sure connection is live
	err = b.connectOnce()
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.GetRetry() {
			return b.GetRetry(), err
		}
		return b.GetRetry(), errs.ErrConsumerStopped
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan *tasks.Signature, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {
		log.INFO.Print("[StartConsuming] next pending task")
		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				if taskProcessor.PreConsumeHandler() {
					task, err := b.nextTask()
					earilyReturn := false
					if err != nil {
						log.ERROR.Printf("[StartConsuming] failed to get next pending task, err: %+v", err)
						earilyReturn = true
					}
					if task.MongoID.IsZero() {
						// log.ERROR.Print("[StartConsuming] empty pending task")
						earilyReturn = true
					}
					if earilyReturn {
						pool <- struct{}{}
						continue
					}
					deliveries <- task
				}
				pool <- struct{}{}
			}
		}
	}()

	// Prevent too much pressure on mongo
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()
		log.INFO.Print("[StartConsuming] next delayed task")
		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			case <-ticker.C:
				signature, err := b.nextDelayedTask()
				if err != nil {
					log.ERROR.Printf("[StartConsuming] failed to get next delayed task, err: %+v", err)
					continue
				}
				if signature.MongoID.IsZero() {
					// log.ERROR.Print("[StartConsuming] empty delayed task")
					continue
				}
				if err := b.Publish(ctx, signature); err != nil {
					log.ERROR.Printf("[StartConsuming] failed to publish task: %+v, err: %+v", signature, err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	b.delayedWG.Wait()   // Waiting for the delayed tasks goroutine to have stopped
	b.consumingWG.Wait() // Waiting for consumption to finish
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) (err error) {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	err = b.connectOnce()
	if err != nil {
		return fmt.Errorf("failed to connect to mongo: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()
		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			var collection *mongo.Collection
			collection, err = b.delayedTasksCollection()
			if err != nil {
				log.ERROR.Printf("[Publish] failed to get delayed task collection, err: %+v", err)
				return
			}
			_, err = collection.InsertOne(ctx, signatureWithScore{
				Signature: signature,
				Score:     score,
			})
			return
		}
	}

	collection, err := b.pendingTasksCollection()
	if err != nil {
		log.ERROR.Printf("[Publish] failed to get pending task collection, err: %+v", err)
		return
	}
	_, err = collection.InsertOne(ctx, signature)
	if err != nil {
		log.ERROR.Printf("[Publish] failed to insert task: %+v, err: %+v", signature, err)
	}
	return
}

// TODO: add paging
// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) (results []*tasks.Signature, err error) {
	results = []*tasks.Signature{}
	middleResults := []signatureWithScore{}
	collection, err := b.pendingTasksCollection()
	if err != nil {
		log.ERROR.Printf("[GetPendingTasks] failed to get pending task collection, err: %+v", err)
		return
	}
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.ERROR.Printf("[GetPendingTasks] failed to find pending task, err: %+v", err)
		return
	}
	err = cursor.All(ctx, &middleResults)
	if err != nil {
		log.ERROR.Printf("[GetPendingTasks] failed to list all pending task, err: %+v", err)
		return
	}
	for index := range middleResults {
		results = append(results, middleResults[index].Signature)
	}
	log.INFO.Printf("[GetPendingTasks] results: %+v", results)
	return
}

// TODO: add paging
// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() (results []*tasks.Signature, err error) {
	results = []*tasks.Signature{}
	collection, err := b.delayedTasksCollection()
	if err != nil {
		log.ERROR.Printf("[GetDelayedTasks] failed to get delayed task collection, err: %+v", err)
		return
	}
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.ERROR.Printf("[GetDelayedTasks] failed to find delayed task, err: %+v", err)
		return
	}
	err = cursor.All(ctx, &results)
	if err != nil {
		log.ERROR.Printf("[GetDelayedTasks] failed to list all delayed task, err: %+v", err)
		return
	}
	log.INFO.Printf("[GetDelayedTasks] results: %+v", results)
	return
}

func (b *Broker) delayedTasksCollection() (result *mongo.Collection, err error) {
	err = b.connectOnce()
	if err != nil {
		log.ERROR.Printf("[delayedTasksCollection] failed to connect once, err: %+v", err)
		return
	}
	result = b.dtc
	return
}

func (b *Broker) pendingTasksCollection() (result *mongo.Collection, err error) {
	err = b.connectOnce()
	if err != nil {
		log.ERROR.Printf("[pendingTasksCollection] failed to connect once, err: %+v", err)
		return
	}
	result = b.ptc
	return
}

func (b *Broker) connectOnce() (err error) {
	b.once.Do(func() {
		err = b.connect()
	})
	if err != nil {
		log.ERROR.Printf("[connectOnce] failed to connect, err: %+v", err)
	}
	return
}

// connect creates the underlying mgo connection if it doesn't exist
// creates required indexes for our collections
func (b *Broker) connect() (err error) {
	client, err := b.dial()
	if err != nil {
		log.ERROR.Printf("[connect] failed to dial, err: %+v", err)
		return
	}
	b.client = client

	database := databaseMachinery
	if b.GetConfig().MongoDB != nil {
		database = b.GetConfig().MongoDB.Database
	}

	b.dtc = b.client.Database(database).Collection(collectionDelayedTasks)
	b.ptc = b.client.Database(database).Collection(collectionPendingTasks)

	err = b.createMongoIndexes(database)
	if err != nil {
		log.ERROR.Printf("[connect] failed to create index, err: %+v", err)
	}
	return
}

// dial connects to mongo with TLSConfig if provided
// else connects via Broker uri
func (b *Broker) dial() (client *mongo.Client, err error) {
	if b.GetConfig().MongoDB != nil && b.GetConfig().MongoDB.Client != nil {
		client = b.GetConfig().MongoDB.Client
		return
	}

	uri := b.GetConfig().Broker
	if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
		uri = fmt.Sprintf("mongodb://%s", uri)
	}

	client, err = mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.ERROR.Printf("[dial] failed to create mongo client, err: %+v", err)
		return
	}

	cancelCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = client.Connect(cancelCtx)
	if err != nil {
		log.ERROR.Printf("[dial] client failed to connect, err: %+v", err)
	}
	return
}

// TODO: add proper index
// createMongoIndexes ensures all indexes are in place
func (b *Broker) createMongoIndexes(database string) (err error) {
	return
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask() (result *tasks.Signature, err error) {
	result = &tasks.Signature{}
	collection, err := b.pendingTasksCollection()
	if err != nil {
		log.ERROR.Printf("[nextTask] failed to get pending task collection, err: %+v", err)
		return
	}
	err = collection.FindOneAndDelete(ctx, bson.M{}).Decode(result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			err = nil
		} else {
			log.ERROR.Printf("[nextTask] failed to find one and delete, err: %+v", err)
		}
		return
	}
	// log.INFO.Printf("[nextTask] result: %+v", result)
	return
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask() (result *tasks.Signature, err error) {
	middleResult := &signatureWithScore{}
	result = &tasks.Signature{}
	collection, err := b.delayedTasksCollection()
	if err != nil {
		log.ERROR.Printf("[nextDelayedTask] failed to get delayed task collection, err: %+v", err)
		return
	}
	findOneAndDeleteOpt := options.FindOneAndDelete()
	findOneAndDeleteOpt.SetSort(bson.D{
		bson.E{
			Key:   "score",
			Value: 1,
		},
	})
	err = collection.FindOneAndDelete(ctx, bson.M{}, findOneAndDeleteOpt).Decode(middleResult)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			err = nil
		} else {
			log.ERROR.Printf("[nextDelayedTask] failed to find one and delete, err: %+v", err)
		}
		return
	}
	result = middleResult.Signature
	// log.INFO.Printf("[nextDelayedTask] result: %+v", result)
	return
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan *tasks.Signature, concurrency int, taskProcessor iface.TaskProcessor) (err error) {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					log.ERROR.Printf("[consume] failed to consume one, err: %+v", err)
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(signature *tasks.Signature, taskProcessor iface.TaskProcessor) (err error) {
	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		if signature.IgnoreWhenTaskNotRegistered {
			return
		}
		// TODO: requeue signature
		log.INFO.Printf("[consumeOne] task not registered with this worker. requeing message: %+v", signature)
		return
	}
	log.INFO.Printf("[consumeOne] received new message: %+v", signature)
	return taskProcessor.Process(signature)
}
