package sqsmessaging

import (
	"context"
	"errors"
	"testing"
)

func TestContextHelpers(t *testing.T) {
	t.Run("QueueNameFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Empty context should return empty string
		if got := QueueNameFromContext(ctx); got != "" {
			t.Errorf("expected empty string, got %q", got)
		}

		// Context with value should return the value
		ctx = context.WithValue(ctx, ContextKeyQueueName, "order-events")
		if got := QueueNameFromContext(ctx); got != "order-events" {
			t.Errorf("expected 'order-events', got %q", got)
		}
	})

	t.Run("MessageIDFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Empty context should return empty string
		if got := MessageIDFromContext(ctx); got != "" {
			t.Errorf("expected empty string, got %q", got)
		}

		// Context with value should return the value
		ctx = context.WithValue(ctx, ContextKeyMessageID, "msg-123")
		if got := MessageIDFromContext(ctx); got != "msg-123" {
			t.Errorf("expected 'msg-123', got %q", got)
		}
	})

	t.Run("EventTypeFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Empty context should return empty string
		if got := EventTypeFromContext(ctx); got != "" {
			t.Errorf("expected empty string, got %q", got)
		}

		// Context with value should return the value
		ctx = context.WithValue(ctx, ContextKeyEventType, "OrderCreated")
		if got := EventTypeFromContext(ctx); got != "OrderCreated" {
			t.Errorf("expected 'OrderCreated', got %q", got)
		}
	})

	t.Run("TraceIDFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Empty context should return empty string
		if got := TraceIDFromContext(ctx); got != "" {
			t.Errorf("expected empty string, got %q", got)
		}

		// Context with value should return the value
		ctx = context.WithValue(ctx, ContextKeyTraceID, "trace-abc-123")
		if got := TraceIDFromContext(ctx); got != "trace-abc-123" {
			t.Errorf("expected 'trace-abc-123', got %q", got)
		}
	})

	t.Run("SourceServiceFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Empty context should return empty string
		if got := SourceServiceFromContext(ctx); got != "" {
			t.Errorf("expected empty string, got %q", got)
		}

		// Context with value should return the value
		ctx = context.WithValue(ctx, ContextKeySourceService, "payment-service")
		if got := SourceServiceFromContext(ctx); got != "payment-service" {
			t.Errorf("expected 'payment-service', got %q", got)
		}
	})

	t.Run("AllContextValues", func(t *testing.T) {
		ctx := context.Background()
		ctx = context.WithValue(ctx, ContextKeyQueueName, "order-events")
		ctx = context.WithValue(ctx, ContextKeyMessageID, "msg-456")
		ctx = context.WithValue(ctx, ContextKeyEventType, "PaymentProcessed")
		ctx = context.WithValue(ctx, ContextKeyTraceID, "trace-xyz")
		ctx = context.WithValue(ctx, ContextKeySourceService, "order-service")

		if got := QueueNameFromContext(ctx); got != "order-events" {
			t.Errorf("QueueName: expected 'order-events', got %q", got)
		}
		if got := MessageIDFromContext(ctx); got != "msg-456" {
			t.Errorf("MessageID: expected 'msg-456', got %q", got)
		}
		if got := EventTypeFromContext(ctx); got != "PaymentProcessed" {
			t.Errorf("EventType: expected 'PaymentProcessed', got %q", got)
		}
		if got := TraceIDFromContext(ctx); got != "trace-xyz" {
			t.Errorf("TraceID: expected 'trace-xyz', got %q", got)
		}
		if got := SourceServiceFromContext(ctx); got != "order-service" {
			t.Errorf("SourceService: expected 'order-service', got %q", got)
		}
	})
}

func TestValidationError(t *testing.T) {
	t.Run("error message without cause", func(t *testing.T) {
		err := NewValidationError("invalid input", nil)
		if err.Error() != "invalid input" {
			t.Errorf("expected 'invalid input', got '%s'", err.Error())
		}
	})

	t.Run("error message with cause", func(t *testing.T) {
		cause := errors.New("missing field")
		err := NewValidationError("invalid input", cause)
		expected := "invalid input: missing field"
		if err.Error() != expected {
			t.Errorf("expected '%s', got '%s'", expected, err.Error())
		}
	})

	t.Run("unwrap returns cause", func(t *testing.T) {
		cause := errors.New("missing field")
		err := NewValidationError("invalid input", cause)
		if err.Unwrap() != cause {
			t.Error("Unwrap should return the cause")
		}
	})

	t.Run("unwrap returns nil when no cause", func(t *testing.T) {
		err := NewValidationError("invalid input", nil)
		if err.Unwrap() != nil {
			t.Error("Unwrap should return nil when no cause")
		}
	})
}

func TestTransientError(t *testing.T) {
	t.Run("error message without cause", func(t *testing.T) {
		err := NewTransientError("connection failed", nil)
		if err.Error() != "connection failed" {
			t.Errorf("expected 'connection failed', got '%s'", err.Error())
		}
	})

	t.Run("error message with cause", func(t *testing.T) {
		cause := errors.New("timeout")
		err := NewTransientError("connection failed", cause)
		expected := "connection failed: timeout"
		if err.Error() != expected {
			t.Errorf("expected '%s', got '%s'", expected, err.Error())
		}
	})

	t.Run("unwrap returns cause", func(t *testing.T) {
		cause := errors.New("timeout")
		err := NewTransientError("connection failed", cause)
		if err.Unwrap() != cause {
			t.Error("Unwrap should return the cause")
		}
	})
}

func TestPermanentError(t *testing.T) {
	t.Run("error message without cause", func(t *testing.T) {
		err := NewPermanentError("not found", nil)
		if err.Error() != "not found" {
			t.Errorf("expected 'not found', got '%s'", err.Error())
		}
	})

	t.Run("error message with cause", func(t *testing.T) {
		cause := errors.New("record deleted")
		err := NewPermanentError("not found", cause)
		expected := "not found: record deleted"
		if err.Error() != expected {
			t.Errorf("expected '%s', got '%s'", expected, err.Error())
		}
	})

	t.Run("unwrap returns cause", func(t *testing.T) {
		cause := errors.New("record deleted")
		err := NewPermanentError("not found", cause)
		if err.Unwrap() != cause {
			t.Error("Unwrap should return the cause")
		}
	})
}

func TestIsValidationError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"validation error", NewValidationError("test", nil), true},
		{"transient error", NewTransientError("test", nil), false},
		{"permanent error", NewPermanentError("test", nil), false},
		{"standard error", errors.New("test"), false},
		{"nil error", nil, false},
		{"wrapped validation error", errors.Join(errors.New("wrap"), NewValidationError("test", nil)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidationError(tt.err); got != tt.expected {
				t.Errorf("IsValidationError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"transient error", NewTransientError("test", nil), true},
		{"validation error", NewValidationError("test", nil), false},
		{"permanent error", NewPermanentError("test", nil), false},
		{"standard error", errors.New("test"), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientError(tt.err); got != tt.expected {
				t.Errorf("IsTransientError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsPermanentError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"permanent error", NewPermanentError("test", nil), true},
		{"validation error", NewValidationError("test", nil), false},
		{"transient error", NewTransientError("test", nil), false},
		{"standard error", errors.New("test"), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanentError(tt.err); got != tt.expected {
				t.Errorf("IsPermanentError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected ErrorType
	}{
		{"validation error", NewValidationError("test", nil), ErrorTypeValidation},
		{"transient error", NewTransientError("test", nil), ErrorTypeTransient},
		{"permanent error", NewPermanentError("test", nil), ErrorTypePermanent},
		{"standard error", errors.New("test"), ErrorTypeUnknown},
		{"nil error", nil, ErrorTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != tt.expected {
				t.Errorf("ClassifyError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestErrorTypeConstants(t *testing.T) {
	// Ensure error type constants have distinct values
	types := []ErrorType{
		ErrorTypeUnknown,
		ErrorTypeValidation,
		ErrorTypeTransient,
		ErrorTypePermanent,
	}

	seen := make(map[ErrorType]bool)
	for _, et := range types {
		if seen[et] {
			t.Errorf("duplicate error type value: %v", et)
		}
		seen[et] = true
	}
}

func TestCommonErrors(t *testing.T) {
	// Ensure common errors are defined and have meaningful messages
	errors := []struct {
		err     error
		name    string
		wantMsg string
	}{
		{ErrClientClosed, "ErrClientClosed", "sqsmessaging: client is closed"},
		{ErrIdempotencyNotConfigured, "ErrIdempotencyNotConfigured", "sqsmessaging: idempotency store not configured (requires Redis and database)"},
		{ErrNoHandler, "ErrNoHandler", "sqsmessaging: no handler registered for event type"},
		{ErrQueueNotSet, "ErrQueueNotSet", "sqsmessaging: queue not set"},
		{ErrInvalidEnvelope, "ErrInvalidEnvelope", "sqsmessaging: invalid message envelope"},
		{ErrAlreadyProcessed, "ErrAlreadyProcessed", "sqsmessaging: message already processed"},
		{ErrBatchTooLarge, "ErrBatchTooLarge", "sqsmessaging: batch size exceeds maximum of 10 messages"},
	}

	for _, tt := range errors {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Errorf("%s should not be nil", tt.name)
			}
			if tt.err.Error() != tt.wantMsg {
				t.Errorf("%s.Error() = %q, want %q", tt.name, tt.err.Error(), tt.wantMsg)
			}
		})
	}
}
