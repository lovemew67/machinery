package tasks

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string      `bson:"name"`
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from interface{} to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

// Signature represents a single task invocation
type Signature struct {
	MongoID        primitive.ObjectID `bson:"_id,omitempty"`
	UUID           string             `bson:"uuid,omitempty"`
	Name           string             `bson:"name,omitempty"`
	RoutingKey     string             `bson:"routing_key,omitempty"`
	ETA            *time.Time         `bson:"eta,omitempty"`
	GroupUUID      string             `bson:"group_uuid,omitempty"`
	GroupTaskCount int                `bson:"group_task_count"`
	Args           []Arg              `bson:"args,omitempty"`
	Headers        Headers            `bson:"headers,omitempty"`
	Priority       uint8              `bson:"priority"`
	Immutable      bool               `bson:"immutable"`
	RetryCount     int                `bson:"retry_count"`
	RetryTimeout   int                `bson:"retry_timeout"`
	OnSuccess      []*Signature       `bson:"on_success,omitempty"`
	OnError        []*Signature       `bson:"on_error,omitempty"`
	ChordCallback  *Signature         `bson:"chord_callback,omitempty"`
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string `bson:"broker_message_group_id,omitempty"`
	//ReceiptHandle of SQS Message
	SQSReceiptHandle string `bson:"sqs_receipt_handle,omitempty"`
	// StopTaskDeletionOnError used with sqs when we want to send failed messages to dlq,
	// and don't want machinery to delete from source queue
	StopTaskDeletionOnError bool `bson:"stop_task_deletion_on_error"`
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool `bson:"ignore_when_task_not_registered"`
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID: fmt.Sprintf("task_%v", signatureID),
		Name: name,
		Args: args,
	}, nil
}
