package sqsmessaging

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// mockNetError implements net.Error for testing isTransientErr
type mockNetError struct {
	msg string
}

func (e *mockNetError) Error() string   { return e.msg }
func (e *mockNetError) Timeout() bool   { return false }
func (e *mockNetError) Temporary() bool { return true }

var _ net.Error = (*mockNetError)(nil)

// ----- calculateBackoffDelay -----

func TestCalculateBackoffDelay(t *testing.T) {
	tests := []struct {
		name              string
		consecutiveErrors int
		initialDelay      time.Duration
		maxDelay          time.Duration
		multiplier        float64
		want              time.Duration
	}{
		{
			name:              "zero errors returns no delay",
			consecutiveErrors: 0,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        2.0,
			want:              0,
		},
		{
			name:              "negative errors returns no delay",
			consecutiveErrors: -1,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        2.0,
			want:              0,
		},
		{
			name:              "one error returns initial delay",
			consecutiveErrors: 1,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        2.0,
			want:              time.Second,
		},
		{
			name:              "two errors doubles the delay",
			consecutiveErrors: 2,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        2.0,
			want:              2 * time.Second,
		},
		{
			name:              "three errors quadruples the delay",
			consecutiveErrors: 3,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        2.0,
			want:              4 * time.Second,
		},
		{
			name:              "capped at max delay",
			consecutiveErrors: 10,
			initialDelay:      time.Second,
			maxDelay:          5 * time.Second,
			multiplier:        2.0,
			want:              5 * time.Second,
		},
		{
			name:              "custom multiplier 3x",
			consecutiveErrors: 2,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        3.0,
			want:              3 * time.Second,
		},
		{
			name:              "multiplier 1 keeps constant delay",
			consecutiveErrors: 5,
			initialDelay:      time.Second,
			maxDelay:          30 * time.Second,
			multiplier:        1.0,
			want:              time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateBackoffDelay(tt.consecutiveErrors, tt.initialDelay, tt.maxDelay, tt.multiplier)
			if got != tt.want {
				t.Errorf("calculateBackoffDelay(%d, %v, %v, %.1f) = %v, want %v",
					tt.consecutiveErrors, tt.initialDelay, tt.maxDelay, tt.multiplier, got, tt.want)
			}
		})
	}
}

// ----- waitWithBackoff -----

func TestWaitWithBackoff_ZeroDelay(t *testing.T) {
	ctx := context.Background()
	err := waitWithBackoff(ctx, 0)
	if err != nil {
		t.Errorf("expected nil error for zero delay, got %v", err)
	}
}

func TestWaitWithBackoff_NegativeDelay(t *testing.T) {
	ctx := context.Background()
	err := waitWithBackoff(ctx, -time.Second)
	if err != nil {
		t.Errorf("expected nil error for negative delay, got %v", err)
	}
}

func TestWaitWithBackoff_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitWithBackoff(ctx, 10*time.Second)
	if err == nil {
		t.Error("expected error when context is already cancelled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestWaitWithBackoff_ShortDelay_Completes(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	err := waitWithBackoff(ctx, 20*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if elapsed < 20*time.Millisecond {
		t.Errorf("expected to wait at least 20ms, waited %v", elapsed)
	}
}

func TestWaitWithBackoff_ContextCancelledDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := waitWithBackoff(ctx, 10*time.Second)
	if err == nil {
		t.Error("expected error when context is cancelled during wait")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// ----- isTransientErr -----

func TestIsTransientErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "connection refused",
			err:  errors.New("connection refused"),
			want: true,
		},
		{
			name: "request timeout",
			err:  errors.New("request timeout"),
			want: true,
		},
		{
			name: "temporary failure",
			err:  errors.New("temporary failure"),
			want: true,
		},
		{
			name: "service unavailable",
			err:  errors.New("service unavailable"),
			want: true,
		},
		{
			name: "please retry",
			err:  errors.New("please retry later"),
			want: true,
		},
		{
			name: "throttled",
			err:  errors.New("request throttled"),
			want: true,
		},
		{
			name: "throttling",
			err:  errors.New("ThrottlingException"),
			want: true,
		},
		{
			name: "case insensitive TIMEOUT",
			err:  errors.New("TIMEOUT exceeded"),
			want: true,
		},
		{
			name: "net.Error satisfies interface",
			err:  &mockNetError{msg: "network error"},
			want: true,
		},
		{
			name: "permanent - invalid format",
			err:  errors.New("invalid payload format"),
			want: false,
		},
		{
			name: "permanent - not found",
			err:  errors.New("record not found"),
			want: false,
		},
		{
			name: "permanent - business logic",
			err:  errors.New("user already exists"),
			want: false,
		},
		{
			name: "permanent - permission denied",
			err:  errors.New("permission denied"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTransientErr(tt.err)
			if got != tt.want {
				t.Errorf("isTransientErr(%q) = %v, want %v", tt.err.Error(), got, tt.want)
			}
		})
	}
}

// ----- isNonExistentQueueErr -----

func TestIsNonExistentQueueErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error returns false",
			err:  nil,
			want: false,
		},
		{
			name: "AWS NonExistentQueue code",
			err:  errors.New("AWS.SimpleQueueService.NonExistentQueue"),
			want: true,
		},
		{
			name: "queue does not exist phrase",
			err:  errors.New("The queue does not exist"),
			want: true,
		},
		{
			name: "partial match NonExistentQueue",
			err:  errors.New("error: NonExistentQueue specified"),
			want: true,
		},
		{
			name: "connection timeout - unrelated",
			err:  errors.New("connection timeout"),
			want: false,
		},
		{
			name: "access denied - unrelated",
			err:  errors.New("access denied"),
			want: false,
		},
		{
			name: "generic sqs error",
			err:  errors.New("SendMessage failed"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNonExistentQueueErr(tt.err)
			if got != tt.want {
				t.Errorf("isNonExistentQueueErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// ----- convertMessageAttributes -----

func TestConvertMessageAttributes_Single(t *testing.T) {
	client := &Client{}

	strVal := "hello"
	dataType := "String"

	attrs := map[string]sqstypes.MessageAttributeValue{
		"key1": {
			StringValue: aws.String(strVal),
			DataType:    aws.String(dataType),
		},
	}

	result := client.convertMessageAttributes(attrs)

	if len(result) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(result))
	}
	attr, ok := result["key1"]
	if !ok {
		t.Fatal("expected key1 to exist in result")
	}
	if attr.Value != "hello" {
		t.Errorf("expected Value='hello', got '%s'", attr.Value)
	}
	if attr.DataType != "String" {
		t.Errorf("expected DataType='String', got '%s'", attr.DataType)
	}
}

func TestConvertMessageAttributes_Empty(t *testing.T) {
	client := &Client{}

	result := client.convertMessageAttributes(nil)
	if len(result) != 0 {
		t.Errorf("expected empty result for nil input, got %d items", len(result))
	}

	result = client.convertMessageAttributes(map[string]sqstypes.MessageAttributeValue{})
	if len(result) != 0 {
		t.Errorf("expected empty result for empty map, got %d items", len(result))
	}
}

func TestConvertMessageAttributes_Multiple(t *testing.T) {
	client := &Client{}

	attrs := map[string]sqstypes.MessageAttributeValue{
		"attr1": {StringValue: aws.String("value1"), DataType: aws.String("String")},
		"attr2": {StringValue: aws.String("value2"), DataType: aws.String("String")},
		"attr3": {StringValue: aws.String("123"), DataType: aws.String("Number")},
	}

	result := client.convertMessageAttributes(attrs)

	if len(result) != 3 {
		t.Fatalf("expected 3 attributes, got %d", len(result))
	}
	for _, key := range []string{"attr1", "attr2", "attr3"} {
		if _, ok := result[key]; !ok {
			t.Errorf("expected key '%s' to exist in result", key)
		}
	}
	if result["attr3"].DataType != "Number" {
		t.Errorf("expected DataType='Number' for attr3, got '%s'", result["attr3"].DataType)
	}
}

func TestConvertMessageAttributes_NilPointers(t *testing.T) {
	client := &Client{}

	// nil string pointers should convert to empty string via aws.ToString
	attrs := map[string]sqstypes.MessageAttributeValue{
		"key1": {
			StringValue: nil,
			DataType:    nil,
		},
	}

	result := client.convertMessageAttributes(attrs)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(result))
	}
	if result["key1"].Value != "" {
		t.Errorf("expected empty Value for nil StringValue, got '%s'", result["key1"].Value)
	}
	if result["key1"].DataType != "" {
		t.Errorf("expected empty DataType for nil DataType, got '%s'", result["key1"].DataType)
	}
}
