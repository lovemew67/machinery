package mongo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/metric"
	"github.com/RichardKnop/machinery/v1/tasks"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

// Broker represents a Redis broker
type Broker struct {
	common.Broker

	host     string
	username string
	password string
	authDB   string
	database string

	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup

	client *mongo.Client
	dtc    *mongo.Collection
	ptc    *mongo.Collection
	once   sync.Once
}

// New creates new Broker instance
func New(cnf *config.Config, host, username, password, authDB, database string) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.username = username
	b.password = password
	b.authDB = authDB
	b.database = database
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

	metric.BrokerConnUsage.WithLabelValues("StartConsuming", consumerTag).Inc()
	defer metric.BrokerConnUsage.WithLabelValues("StartConsuming", consumerTag).Dec()
	defer metric.BrokerConnUsage.DeleteLabelValues("StartConsuming", consumerTag)

	// Connect the server to make sure connection is live
	err = b.connectActively()
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
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				if taskProcessor.PreConsumeHandler() {
					task, _ := b.nextTask("default")
					if len(task) > 0 {
						deliveries <- task
					}
				}

				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask("default")
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshaTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.ERROR.Print(err)
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

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	log.INFO.Printf("%+v\n", msg)

	err = b.connectActively()
	if err != nil {
		return fmt.Errorf("failed to connect to mongo: %s", err)
	}

	metric.BrokerConnUsage.WithLabelValues("Publish", signature.UUID).Inc()
	defer metric.BrokerConnUsage.WithLabelValues("Publish", signature.UUID).Dec()
	defer metric.BrokerConnUsage.DeleteLabelValues("Publish", signature.UUID)

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			// score := signature.ETA.UnixNano()
			// TODO: insert delayed task
			// _, err = conn.Do("ZADD", redisDelayedTasksKey, score, msg)
			return
		}
	}

	// TODO: insert pending task
	// _, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) (results []*tasks.Signature, err error) {
	results = []*tasks.Signature{}
	ctx := context.Background()
	cursor, err := b.pendingTasksCollection().Find(ctx, bson.M{})
	if err != nil {
		return
	}
	err = cursor.All(ctx, &results)
	return
}

// GetDelayedTasks returns a slice of task signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() (results []*tasks.Signature, err error) {
	results = []*tasks.Signature{}
	ctx := context.Background()
	cursor, err := b.delayedTasksCollection().Find(ctx, bson.M{})
	if err != nil {
		return
	}
	err = cursor.All(ctx, &results)
	return
}

func (b *Broker) delayedTasksCollection() *mongo.Collection {
	b.once.Do(func() {
		_ = b.connect()
	})
	return b.dtc
}

func (b *Broker) pendingTasksCollection() *mongo.Collection {
	b.once.Do(func() {
		_ = b.connect()
	})
	return b.ptc
}

func (b *Broker) connectActively() (err error) {
	b.once.Do(func() {
		err = b.connect()
	})
	return
}

// connect creates the underlying mgo connection if it doesn't exist
// creates required indexes for our collections
func (b *Broker) connect() (err error) {
	client, err := b.dial()
	if err != nil {
		return err
	}
	b.client = client

	database := databaseMachinery
	if b.GetConfig().MongoDB != nil {
		database = b.GetConfig().MongoDB.Database
	}

	b.dtc = b.client.Database(database).Collection(collectionDelayedTasks)
	b.ptc = b.client.Database(database).Collection(collectionPendingTasks)

	err = b.createMongoIndexes(database)
	return
}

// dial connects to mongo with TLSConfig if provided
// else connects via Broker uri
func (b *Broker) dial() (client *mongo.Client, err error) {
	if b.GetConfig().MongoDB != nil && b.GetConfig().MongoDB.Client != nil {
		return b.GetConfig().MongoDB.Client, nil
	}

	uri := b.GetConfig().Broker
	if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
		uri = fmt.Sprintf("mongodb://%s", uri)
	}

	client, err = mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	return
}

// createMongoIndexes ensures all indexes are in place
func (b *Broker) createMongoIndexes(database string) (err error) {
	return
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask(queue string) (result []byte, err error) {
	return
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask(key string) (result []byte, err error) {
	return
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) (err error) {
	return
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) (err error) {
	return
}
