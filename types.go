package sqsmessaging

import (
	"context"
	"errors"
)

// Context keys for message metadata
type contextKey string

const (
	// ContextKeyQueueName is the context key for the queue name
	ContextKeyQueueName contextKey = "sqsmessaging.queue_name"
	// ContextKeyMessageID is the context key for the message ID
	ContextKeyMessageID contextKey = "sqsmessaging.message_id"
	// ContextKeyEventType is the context key for the event type
	ContextKeyEventType contextKey = "sqsmessaging.event_type"
	// ContextKeyTraceID is the context key for the trace ID
	ContextKeyTraceID contextKey = "sqsmessaging.trace_id"
	// ContextKeySourceService is the context key for the source service that published the message
	ContextKeySourceService contextKey = "sqsmessaging.source_service"
)

// QueueNameFromContext returns the queue name from the context.
// Returns empty string if not set.
func QueueNameFromContext(ctx context.Context) string {
	if v := ctx.Value(ContextKeyQueueName); v != nil {
		return v.(string)
	}
	return ""
}

// MessageIDFromContext returns the message ID from the context.
// Returns empty string if not set.
func MessageIDFromContext(ctx context.Context) string {
	if v := ctx.Value(ContextKeyMessageID); v != nil {
		return v.(string)
	}
	return ""
}

// EventTypeFromContext returns the event type from the context.
// Returns empty string if not set.
func EventTypeFromContext(ctx context.Context) string {
	if v := ctx.Value(ContextKeyEventType); v != nil {
		return v.(string)
	}
	return ""
}

// TraceIDFromContext returns the trace ID from the context.
// Returns empty string if not set.
func TraceIDFromContext(ctx context.Context) string {
	if v := ctx.Value(ContextKeyTraceID); v != nil {
		return v.(string)
	}
	return ""
}

// SourceServiceFromContext returns the source service that published the message.
// Returns empty string if not set.
func SourceServiceFromContext(ctx context.Context) string {
	if v := ctx.Value(ContextKeySourceService); v != nil {
		return v.(string)
	}
	return ""
}

// Handler is a function that handles incoming messages.
// It receives the context and the message payload.
// The context contains message metadata accessible via helper functions:
//   - SourceServiceFromContext(ctx) - the service that published the message
//   - EventTypeFromContext(ctx) - the event type
//   - TraceIDFromContext(ctx) - the trace ID for distributed tracing
//   - MessageIDFromContext(ctx) - the SQS message ID
//   - QueueNameFromContext(ctx) - the queue name the message was received from
//
// Return nil for success, or an error to indicate failure.
type Handler func(ctx context.Context, payload map[string]any) error

// Event represents a publishable event.
type Event interface {
	// EventType returns the event type identifier (e.g., "UserCreated")
	EventType() string
	// Payload returns the event data as a map
	Payload() map[string]any
}

// Message represents a received message from SQS.
type Message struct {
	// MessageID is the unique message identifier
	MessageID string
	// ReceiptHandle is used to delete or modify the message
	ReceiptHandle string
	// Body is the raw message body (JSON)
	Body string
	// Attributes contains message attributes
	Attributes map[string]string
	// EventType is extracted from the message envelope
	EventType string
	// Payload is the parsed message payload
	Payload map[string]any
	// IdempotencyKey is the unique key for deduplication
	IdempotencyKey string
	// TraceID for distributed tracing
	TraceID string
}

// BatchMessage represents a message for batch publishing.
type BatchMessage struct {
	// ID is a unique identifier for this message in the batch
	ID string
	// EventType is the type of event
	EventType string
	// Payload is the message data
	Payload map[string]any
}

// BatchResult represents the result of a batch publish operation.
type BatchResult struct {
	// ID is the identifier from the BatchMessage
	ID string
	// MessageID is the SQS message ID (empty if failed)
	MessageID string
	// Error is set if this message failed to publish
	Error error
}

// QueueStatus contains information about a queue's current state.
type QueueStatus struct {
	// QueueName is the name of the queue
	QueueName string
	// QueueURL is the full SQS queue URL
	QueueURL string
	// MessageCount is the approximate number of messages
	MessageCount int64
	// DLQMessageCount is the approximate number of messages in the DLQ
	DLQMessageCount int64
}

// Common errors
var (
	// ErrClientClosed is returned when operations are attempted on a closed client
	ErrClientClosed = errors.New("sqsmessaging: client is closed")

	// ErrRedisRequired is returned when Redis is not configured
	// Redis is required for queue URL caching and is a mandatory dependency
	ErrRedisRequired = errors.New("sqsmessaging: Redis is required - use WithRedis() or WithRedisClient() option")

	// ErrRedisConnectionFailed is returned when Redis connection cannot be established
	ErrRedisConnectionFailed = errors.New("sqsmessaging: failed to connect to Redis")

	// ErrIdempotencyNotConfigured is returned when idempotency operations are
	// attempted without Redis and database configured
	ErrIdempotencyNotConfigured = errors.New("sqsmessaging: idempotency store not configured (requires Redis and database)")

	// ErrNoHandler is returned when no handler is registered for an event type
	ErrNoHandler = errors.New("sqsmessaging: no handler registered for event type")

	// ErrQueueNotSet is returned when queue operations are attempted before setting a queue
	ErrQueueNotSet = errors.New("sqsmessaging: queue not set")

	// ErrInvalidEnvelope is returned when a message has an invalid envelope format
	ErrInvalidEnvelope = errors.New("sqsmessaging: invalid message envelope")

	// ErrAlreadyProcessed is returned when a message has already been processed
	ErrAlreadyProcessed = errors.New("sqsmessaging: message already processed")

	// ErrBatchTooLarge is returned when batch size exceeds 10 messages
	ErrBatchTooLarge = errors.New("sqsmessaging: batch size exceeds maximum of 10 messages")
)

// ErrorType represents the classification of an error
type ErrorType int

const (
	// ErrorTypeUnknown is an unclassified error
	ErrorTypeUnknown ErrorType = iota
	// ErrorTypeValidation is a validation error (message will be deleted)
	ErrorTypeValidation
	// ErrorTypeTransient is a transient error (message will be retried)
	ErrorTypeTransient
	// ErrorTypePermanent is a permanent error (message will be deleted)
	ErrorTypePermanent
)

// ValidationError represents a validation error that should not be retried.
// Messages with validation errors are immediately deleted.
type ValidationError struct {
	Message string
	Cause   error
}

func (e *ValidationError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *ValidationError) Unwrap() error {
	return e.Cause
}

// TransientError represents a transient error that should be retried.
// Messages with transient errors are left in the queue for retry.
type TransientError struct {
	Message string
	Cause   error
}

func (e *TransientError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *TransientError) Unwrap() error {
	return e.Cause
}

// PermanentError represents a permanent error that should not be retried.
// Messages with permanent errors are deleted and may trigger alerts.
type PermanentError struct {
	Message string
	Cause   error
}

func (e *PermanentError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *PermanentError) Unwrap() error {
	return e.Cause
}

// NewValidationError creates a new validation error.
func NewValidationError(msg string, cause error) *ValidationError {
	return &ValidationError{Message: msg, Cause: cause}
}

// NewTransientError creates a new transient error.
func NewTransientError(msg string, cause error) *TransientError {
	return &TransientError{Message: msg, Cause: cause}
}

// NewPermanentError creates a new permanent error.
func NewPermanentError(msg string, cause error) *PermanentError {
	return &PermanentError{Message: msg, Cause: cause}
}

// IsValidationError checks if an error is a validation error.
func IsValidationError(err error) bool {
	var valErr *ValidationError
	return errors.As(err, &valErr)
}

// IsTransientError checks if an error is a transient error.
func IsTransientError(err error) bool {
	var transErr *TransientError
	return errors.As(err, &transErr)
}

// IsPermanentError checks if an error is a permanent error.
func IsPermanentError(err error) bool {
	var permErr *PermanentError
	return errors.As(err, &permErr)
}

// ClassifyError returns the error type for the given error.
func ClassifyError(err error) ErrorType {
	if IsValidationError(err) {
		return ErrorTypeValidation
	}
	if IsTransientError(err) {
		return ErrorTypeTransient
	}
	if IsPermanentError(err) {
		return ErrorTypePermanent
	}
	return ErrorTypeUnknown
}
