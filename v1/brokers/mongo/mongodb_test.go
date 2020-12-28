package mongo

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
)

const (
	user   = "username"
	pass   = "password"
	authDB = "admin"
)

var (
	err       error
	mongoPort string
	broker    iface.Broker
)

var (
	dockerPool    *dockertest.Pool
	mongoResource *dockertest.Resource
)

func beforeTest() {
	dockerPool, err = dockertest.NewPool("")
	if err != nil {
		panic("docker test init fail, error:" + err.Error())
	}
	mongoResource, err = dockerPool.Run("mongo", "4.2", []string{
		fmt.Sprintf("MONGO_INITDB_ROOT_USERNAME=%s", user),
		fmt.Sprintf("MONGO_INITDB_ROOT_PASSWORD=%s", pass),
	})
	if err != nil {
		panic("mongodb docker init fail, error:" + err.Error())
	}
	err = mongoResource.Expire(600)
	if err != nil {
		panic("set mongodb expiration fail, error:" + err.Error())
	}
	mongoPort = mongoResource.GetPort("27017/tcp")
}

func TestMain(m *testing.M) {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags)
	beforeTest()
	defer afterTest()
	os.Exit(m.Run())
}

func afterTest() {}

func newBroker() (iface.Broker, error) {
	cnf := &config.Config{
		Broker: os.Getenv("MONGODB_URL"),
	}
	backend := New(
		cnf,
		"localhost",
		user,
		pass,
		authDB,
		databaseMachinery,
	)
	return backend, nil
}

func Test_NewBroker(t *testing.T) {
	os.Setenv("MONGODB_URL", fmt.Sprintf("mongodb://%s:%s@localhost:%s/%s?authSource=%s", user, pass, mongoPort, databaseMachinery, authDB))

	broker, err = newBroker()
	if assert.NoError(t, err) {
		assert.NotNil(t, broker)
	}
}

func Test_GetPendingTasks(t *testing.T) {
	if broker == nil {
		t.Skip("nil broker")
	}

	results, err := broker.GetPendingTasks("default")
	if assert.NoError(t, err) {
		assert.NotNil(t, results)
	}
}

func Test_GetDelayedTasks(t *testing.T) {
	if broker == nil {
		t.Skip("nil broker")
	}

	results, err := broker.GetDelayedTasks()
	if assert.NoError(t, err) {
		assert.NotNil(t, results)
	}
}
