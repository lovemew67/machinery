package mongo

import (
	"context"

	"github.com/RichardKnop/machinery/v1/tasks"
)

const (
	databaseMachinery = "machinery"
)

const (
	collectionDelayedTasks = "delayed_tasks"
	collectionPendingTasks = "pending_tasks"
)

var (
	ctx = context.Background()
)

type SignatureWithScore struct {
	tasks.Signature

	// mimic redis's behavior
	Score int64 `bson:"score"`
}
