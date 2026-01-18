// Package contracts defines the interfaces for the messaging system.
package contracts

import (
	"context"
)

// Event represents a publishable event
type Event interface {
	// EventType returns the event type identifier (e.g., "StudentEnrolled")
	EventType() string
	// Payload returns the event data as a map
	Payload() map[string]interface{}
}

// MessagingDriver defines the interface for messaging drivers
type MessagingDriver interface {
	// Publish sends an event through the messaging system
	Publish(ctx context.Context, event Event) error
	// PublishRaw sends a raw message with explicit event type and payload
	PublishRaw(ctx context.Context, eventType string, payload map[string]interface{}) error
	// Name returns the driver name
	Name() string
	// IsAvailable checks if the driver is available for the given event type
	IsAvailable(ctx context.Context, eventType string) (bool, error)
}

// Consumer defines the interface for message consumers
type Consumer interface {
	// ReceiveMessages polls for messages from the queue
	ReceiveMessages(ctx context.Context, maxMessages int, waitTime int) ([]Message, error)
	// DeleteMessage acknowledges and removes a message from the queue
	DeleteMessage(ctx context.Context, receiptHandle string) error
	// ChangeVisibilityTimeout extends the visibility timeout for a message
	ChangeVisibilityTimeout(ctx context.Context, receiptHandle string, timeout int) error
	// GetQueueDepth returns the approximate number of messages in the queue
	GetQueueDepth(ctx context.Context) (int64, error)
}

// Message represents a received message from the queue
type Message struct {
	// MessageID is the unique message identifier
	MessageID string
	// ReceiptHandle is used to delete or modify the message
	ReceiptHandle string
	// Body is the raw message body
	Body string
	// Attributes contains message attributes
	Attributes map[string]string
	// MessageAttributes contains custom message attributes
	MessageAttributes map[string]MessageAttribute
}

// MessageAttribute represents a message attribute
type MessageAttribute struct {
	DataType string
	Value    string
}

// Publisher defines the interface for message publishers
type Publisher interface {
	// Publish sends a single message to the queue
	Publish(ctx context.Context, queueName, eventType string, payload map[string]interface{}) (string, error)
	// PublishBatch sends multiple messages to the queue
	PublishBatch(ctx context.Context, queueName string, messages []BatchMessage) ([]BatchResult, error)
}

// BatchMessage represents a message for batch publishing
type BatchMessage struct {
	ID        string
	EventType string
	Payload   map[string]interface{}
}

// BatchResult represents the result of a batch publish operation
type BatchResult struct {
	ID        string
	MessageID string
	Error     error
}

// QueueResolver defines the interface for queue URL resolution
type QueueResolver interface {
	// Resolve returns the queue URL, creating the queue if necessary
	Resolve(ctx context.Context, queueName string) (string, error)
	// QueueExists checks if a queue exists without creating it
	QueueExists(ctx context.Context, queueName string) (bool, error)
	// CreateQueue creates a new queue with the given name
	CreateQueue(ctx context.Context, queueName string) (string, error)
	// CreateQueueWithDLQ creates a queue with an associated Dead Letter Queue
	CreateQueueWithDLQ(ctx context.Context, queueName string) (string, error)
}

// IdempotencyStore defines the interface for idempotency checking
type IdempotencyStore interface {
	// IsProcessed checks if an event has already been processed
	IsProcessed(ctx context.Context, idempotencyKey string) (bool, error)
	// MarkProcessing marks an event as currently being processed
	MarkProcessing(ctx context.Context, idempotencyKey string) error
	// MarkProcessed marks an event as successfully processed
	MarkProcessed(ctx context.Context, idempotencyKey, eventType, service string) error
	// ClearProcessing removes the processing lock
	ClearProcessing(ctx context.Context, idempotencyKey string) error
	// Cleanup removes old processed event records
	Cleanup(ctx context.Context, olderThanDays int) (int64, error)
}

// MetricsService defines the interface for metrics collection
type MetricsService interface {
	// PutMetric sends a single metric
	PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error
	// Increment increments a counter metric
	Increment(ctx context.Context, name string, dimensions map[string]string) error
	// RecordDuration records a duration metric
	RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error
}

// EventHandler defines the function signature for event handlers
type EventHandler func(ctx context.Context, payload map[string]interface{}) error

// EventRegistry manages event type to handler mappings
type EventRegistry interface {
	// Register registers a handler for an event type
	Register(eventType string, handler EventHandler)
	// GetHandler returns the handler for an event type
	GetHandler(eventType string) (EventHandler, bool)
	// ListEventTypes returns all registered event types
	ListEventTypes() []string
}
