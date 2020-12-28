package mongo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
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
	return
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) (err error) {
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
