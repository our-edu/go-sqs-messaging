package sqsmessaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

// setupTestRedisForClient starts a miniredis and returns a configured redis.Client.
// Cleanup is registered automatically via t.Cleanup.
func setupTestRedisForClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		client.Close()
		mr.Close()
	})
	return mr, client
}

// newClientForTest creates a fully initialized Client using miniredis.
// Cleanup (Close) is registered via t.Cleanup.
func newClientForTest(t *testing.T, extraOpts ...Option) *Client {
	t.Helper()
	_, redisClient := setupTestRedisForClient(t)

	baseOpts := []Option{
		WithRedisClient(redisClient),
		WithAWSRegion("us-east-1"),
		WithService("test-service"),
	}
	client, err := New(append(baseOpts, extraOpts...)...)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// ----- New() validation -----

func TestNew_NoRedis_ReturnsErrRedisRequired(t *testing.T) {
	_, err := New()
	if err == nil {
		t.Fatal("expected error when no Redis is configured")
	}
	if !errors.Is(err, ErrRedisRequired) {
		t.Errorf("expected ErrRedisRequired, got %v", err)
	}
}

func TestNew_BadRedis_ReturnsErrRedisConnectionFailed(t *testing.T) {
	badClient := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // unreachable port
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	defer badClient.Close()

	_, err := New(WithRedisClient(badClient))
	if err == nil {
		t.Fatal("expected error with bad Redis connection")
	}
	if !errors.Is(err, ErrRedisConnectionFailed) {
		t.Errorf("expected ErrRedisConnectionFailed, got %v", err)
	}
}

func TestNew_Success_ReturnsNonNilClient(t *testing.T) {
	client := newClientForTest(t)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNew_DefaultServiceName(t *testing.T) {
	_, redisClient := setupTestRedisForClient(t)

	// No WithService — should default to "default-service"
	client, err := New(
		WithRedisClient(redisClient),
		WithAWSRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer client.Close()

	if client.serviceName != "default-service" {
		t.Errorf("expected serviceName='default-service', got '%s'", client.serviceName)
	}
}

func TestNew_CustomServiceName(t *testing.T) {
	client := newClientForTest(t, WithService("payment-service"))
	if client.serviceName != "payment-service" {
		t.Errorf("expected serviceName='payment-service', got '%s'", client.serviceName)
	}
}

// ----- Close() -----

func TestClient_Close_FirstCallSucceeds(t *testing.T) {
	_, redisClient := setupTestRedisForClient(t)
	client, err := New(
		WithRedisClient(redisClient),
		WithAWSRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

func TestClient_Close_Idempotent(t *testing.T) {
	_, redisClient := setupTestRedisForClient(t)
	client, err := New(
		WithRedisClient(redisClient),
		WithAWSRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("first Close() returned error: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("second Close() returned error: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("third Close() returned error: %v", err)
	}
}

// ----- Operations on closed client -----

func TestClient_Publish_ClosedClient(t *testing.T) {
	client := newClientForTest(t)
	client.Close()

	err := client.Publish(context.Background(), "TestEvent", map[string]any{"key": "value"})
	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected ErrClientClosed, got %v", err)
	}
}

func TestClient_PublishToQueue_ClosedClient(t *testing.T) {
	client := newClientForTest(t)
	client.Close()

	_, err := client.PublishToQueue(context.Background(), "test-queue", "TestEvent", map[string]any{})
	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected ErrClientClosed, got %v", err)
	}
}

func TestClient_PublishBatch_ClosedClient(t *testing.T) {
	client := newClientForTest(t)
	client.Close()

	_, err := client.PublishBatch(context.Background(), "test-queue", []BatchMessage{
		{ID: "1", EventType: "TestEvent", Payload: map[string]any{}},
	})
	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected ErrClientClosed, got %v", err)
	}
}

func TestClient_StartConsumer_ClosedClient(t *testing.T) {
	client := newClientForTest(t)
	client.Close()

	err := client.StartConsumer(context.Background(), "test-queue")
	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected ErrClientClosed, got %v", err)
	}
}

func TestClient_StartMultiConsumer_ClosedClient(t *testing.T) {
	client := newClientForTest(t)
	client.Close()

	err := client.StartMultiConsumer(context.Background(), []string{"queue1", "queue2"})
	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected ErrClientClosed, got %v", err)
	}
}

func TestClient_StartMultiConsumer_EmptyQueues(t *testing.T) {
	client := newClientForTest(t)

	err := client.StartMultiConsumer(context.Background(), []string{})
	if err == nil {
		t.Error("expected error for empty queue list")
	}
}

// ----- RegisterHandler / RegisterHandlers -----

func TestClient_RegisterHandler(t *testing.T) {
	client := newClientForTest(t)

	called := false
	client.RegisterHandler("TestEvent", func(ctx context.Context, payload map[string]any) error {
		called = true
		return nil
	})

	handler, ok := client.eventRegistry.GetHandler("TestEvent")
	if !ok {
		t.Fatal("expected handler to be registered")
	}
	if err := handler(context.Background(), nil); err != nil {
		t.Errorf("unexpected error from handler: %v", err)
	}
	if !called {
		t.Error("expected handler to be called")
	}
}

func TestClient_RegisterHandler_Overwrite(t *testing.T) {
	client := newClientForTest(t)

	callCount := 0
	client.RegisterHandler("TestEvent", func(ctx context.Context, payload map[string]any) error {
		callCount = 1
		return nil
	})
	client.RegisterHandler("TestEvent", func(ctx context.Context, payload map[string]any) error {
		callCount = 2
		return nil
	})

	handler, _ := client.eventRegistry.GetHandler("TestEvent")
	handler(context.Background(), nil)

	if callCount != 2 {
		t.Errorf("expected second handler to be active (callCount=2), got %d", callCount)
	}
}

func TestClient_RegisterHandlers_Multiple(t *testing.T) {
	client := newClientForTest(t)

	client.RegisterHandlers(map[string]Handler{
		"Event1": func(ctx context.Context, payload map[string]any) error { return nil },
		"Event2": func(ctx context.Context, payload map[string]any) error { return nil },
		"Event3": func(ctx context.Context, payload map[string]any) error { return nil },
	})

	for _, eventType := range []string{"Event1", "Event2", "Event3"} {
		if _, ok := client.eventRegistry.GetHandler(eventType); !ok {
			t.Errorf("expected handler for '%s' to be registered", eventType)
		}
	}
}

// ----- CleanupProcessedEvents -----

func TestClient_CleanupProcessedEvents_NoIdempotency(t *testing.T) {
	// No DB configured → idempotencyStore is nil
	client := newClientForTest(t)

	_, err := client.CleanupProcessedEvents(context.Background(), 30)
	if !errors.Is(err, ErrIdempotencyNotConfigured) {
		t.Errorf("expected ErrIdempotencyNotConfigured, got %v", err)
	}
}

// ----- SetTargetQueue / SetDefaultTargetQueue -----

func TestClient_SetTargetQueue(t *testing.T) {
	client := newClientForTest(t)

	client.SetTargetQueue("OrderCreated", "order-queue")

	if queue, ok := client.config.TargetQueue.Mappings["OrderCreated"]; !ok || queue != "order-queue" {
		t.Errorf("expected mapping OrderCreated -> order-queue, got '%s' (ok=%v)", queue, ok)
	}
}

func TestClient_SetDefaultTargetQueue(t *testing.T) {
	client := newClientForTest(t)

	client.SetDefaultTargetQueue("default-queue")

	if client.config.TargetQueue.Default != "default-queue" {
		t.Errorf("expected Default='default-queue', got '%s'", client.config.TargetQueue.Default)
	}
}

// ----- Prometheus / MetricsProvider -----

func TestClient_PrometheusEnabled_FalseByDefault(t *testing.T) {
	client := newClientForTest(t)

	if client.PrometheusEnabled() {
		t.Error("expected Prometheus to be disabled by default")
	}
}

func TestClient_MetricsProvider_NotNil(t *testing.T) {
	client := newClientForTest(t)

	if provider := client.MetricsProvider(); provider == nil {
		t.Error("expected non-nil metrics provider")
	}
}

func TestClient_PrometheusCollectors_NilWhenDisabled(t *testing.T) {
	client := newClientForTest(t)

	collectors := client.PrometheusCollectors()
	if collectors != nil {
		t.Errorf("expected nil Prometheus collectors when disabled, got %d", len(collectors))
	}
}

func TestClient_PrometheusHandler_NilWhenDisabled(t *testing.T) {
	client := newClientForTest(t)

	if h := client.PrometheusHandler(); h != nil {
		t.Error("expected nil HTTP handler when Prometheus is disabled")
	}
}

func TestClient_PrometheusEnabled_TrueWhenConfigured(t *testing.T) {
	client := newClientForTest(t, WithPrometheusMetrics(true, "test_ns"))

	if !client.PrometheusEnabled() {
		t.Error("expected Prometheus to be enabled when configured")
	}
}

func TestClient_PrometheusHandler_NonNilWhenEnabled(t *testing.T) {
	client := newClientForTest(t, WithPrometheusMetrics(true, "test_handler_ns"))

	if h := client.PrometheusHandler(); h == nil {
		t.Error("expected non-nil HTTP handler when Prometheus is enabled")
	}
}

func TestClient_PrometheusCollectors_NonNilWhenEnabled(t *testing.T) {
	client := newClientForTest(t, WithPrometheusMetrics(true, "test_collectors_ns"))

	collectors := client.PrometheusCollectors()
	if len(collectors) == 0 {
		t.Error("expected non-empty Prometheus collectors when enabled")
	}
}
