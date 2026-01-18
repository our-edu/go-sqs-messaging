package sqsmessaging

import (
	"errors"
	"testing"
)

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
