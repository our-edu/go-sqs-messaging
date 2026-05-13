package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	"github.com/our-edu/go-sqs-messaging/pkg/config"
	"github.com/rs/zerolog"
)

// ---- test doubles ----

type mockEvent struct {
	eventType string
	payload   map[string]any
}

func (e *mockEvent) EventType() string       { return e.eventType }
func (e *mockEvent) Payload() map[string]any { return e.payload }

type mockDriver struct {
	driverName    string
	publishErr    error
	publishRawErr error
	published     []string
	publishedRaw  []string
}

func (m *mockDriver) Publish(ctx context.Context, event contracts.Event) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, event.EventType())
	return nil
}

func (m *mockDriver) PublishRaw(ctx context.Context, eventType string, payload map[string]any) error {
	if m.publishRawErr != nil {
		return m.publishRawErr
	}
	m.publishedRaw = append(m.publishedRaw, eventType)
	return nil
}

func (m *mockDriver) Name() string { return m.driverName }

func (m *mockDriver) IsAvailable(ctx context.Context, eventType string) (bool, error) {
	return true, nil
}

// ---- helpers ----

func newTestMessagingService() *Service {
	cfg := config.DefaultConfig()
	return NewService(cfg, zerolog.Nop())
}

// ----- NewService -----

func TestNewService(t *testing.T) {
	s := newTestMessagingService()
	if s == nil {
		t.Fatal("expected non-nil service")
	}
	if s.drivers == nil {
		t.Error("expected drivers map to be initialized")
	}
}

// ----- RegisterDriver / GetAvailableDrivers -----

func TestService_RegisterDriver_StoresDriver(t *testing.T) {
	s := newTestMessagingService()
	driver := &mockDriver{driverName: "sqs"}

	s.RegisterDriver(config.DriverSQS, driver)

	s.mutex.RLock()
	d, ok := s.drivers[config.DriverSQS]
	s.mutex.RUnlock()

	if !ok {
		t.Fatal("expected driver to be registered")
	}
	if d != driver {
		t.Error("registered driver does not match")
	}
}

func TestService_GetAvailableDrivers_Empty(t *testing.T) {
	s := newTestMessagingService()
	if drivers := s.GetAvailableDrivers(); len(drivers) != 0 {
		t.Errorf("expected 0 drivers, got %d", len(drivers))
	}
}

func TestService_GetAvailableDrivers_ReturnsNames(t *testing.T) {
	s := newTestMessagingService()
	s.RegisterDriver(config.DriverSQS, &mockDriver{driverName: "sqs"})

	drivers := s.GetAvailableDrivers()
	if len(drivers) != 1 {
		t.Fatalf("expected 1 driver, got %d", len(drivers))
	}
	if drivers[0] != string(config.DriverSQS) {
		t.Errorf("expected driver name '%s', got '%s'", config.DriverSQS, drivers[0])
	}
}

// ----- GetDriver / IsSQS / IsRabbitMQ -----

func TestService_GetDriver_DefaultIsSQS(t *testing.T) {
	s := newTestMessagingService()
	if s.GetDriver() != config.DriverSQS {
		t.Errorf("expected default driver %s, got %s", config.DriverSQS, s.GetDriver())
	}
}

func TestService_IsSQS_TrueForDefaultConfig(t *testing.T) {
	s := newTestMessagingService()
	if !s.IsSQS() {
		t.Error("expected IsSQS() = true for default config")
	}
}

func TestService_IsRabbitMQ_FalseForDefaultConfig(t *testing.T) {
	s := newTestMessagingService()
	if s.IsRabbitMQ() {
		t.Error("expected IsRabbitMQ() = false for default config")
	}
}

func TestService_IsRabbitMQ_TrueWhenDriverSet(t *testing.T) {
	s := newTestMessagingService()
	s.config.Messaging.Driver = config.DriverRabbitMQ

	if !s.IsRabbitMQ() {
		t.Error("expected IsRabbitMQ() = true when driver is RabbitMQ")
	}
}

// ----- PublishRaw -----

func TestService_PublishRaw_NoDriverReturnsError(t *testing.T) {
	s := newTestMessagingService()

	err := s.PublishRaw(context.Background(), "TestEvent", map[string]any{})
	if err == nil {
		t.Error("expected error when no driver is registered")
	}
}

func TestService_PublishRaw_RoutesToActiveDriver(t *testing.T) {
	s := newTestMessagingService()
	driver := &mockDriver{driverName: "sqs"}
	s.RegisterDriver(config.DriverSQS, driver)

	err := s.PublishRaw(context.Background(), "OrderCreated", map[string]any{"id": "1"})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(driver.publishedRaw) != 1 || driver.publishedRaw[0] != "OrderCreated" {
		t.Errorf("expected 'OrderCreated' to be published, got %v", driver.publishedRaw)
	}
}

func TestService_PublishRaw_PropagatesDriverError(t *testing.T) {
	s := newTestMessagingService()
	s.RegisterDriver(config.DriverSQS, &mockDriver{
		driverName:    "sqs",
		publishRawErr: errors.New("publish failed"),
	})

	if err := s.PublishRaw(context.Background(), "TestEvent", nil); err == nil {
		t.Error("expected error from driver to propagate")
	}
}

// ----- Publish (single-driver, no dual-write) -----

func TestService_Publish_RoutesToActiveDriver(t *testing.T) {
	s := newTestMessagingService()
	driver := &mockDriver{driverName: "sqs"}
	s.RegisterDriver(config.DriverSQS, driver)

	event := &mockEvent{eventType: "UserCreated"}
	if err := s.Publish(context.Background(), event); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(driver.published) != 1 || driver.published[0] != "UserCreated" {
		t.Errorf("expected 'UserCreated' to be published, got %v", driver.published)
	}
}

func TestService_Publish_NoDriverReturnsError(t *testing.T) {
	s := newTestMessagingService()

	err := s.Publish(context.Background(), &mockEvent{eventType: "OrderCreated"})
	if err == nil {
		t.Error("expected error when no driver is registered")
	}
}

// ----- GetEventHandler -----

func TestService_GetEventHandler_NoRegistry(t *testing.T) {
	s := newTestMessagingService()

	_, ok := s.GetEventHandler("TestEvent")
	if ok {
		t.Error("expected GetEventHandler to return false when registry is nil")
	}
}

func TestService_GetEventHandler_ReturnsRegisteredHandler(t *testing.T) {
	s := newTestMessagingService()

	registry := NewEventRegistry()
	registry.Register("TestEvent", func(ctx context.Context, payload map[string]any) error {
		return nil
	})
	s.RegisterEventRegistry(registry)

	h, ok := s.GetEventHandler("TestEvent")
	if !ok {
		t.Error("expected GetEventHandler to return true")
	}
	if h == nil {
		t.Error("expected non-nil handler")
	}
}

func TestService_GetEventHandler_UnregisteredEvent(t *testing.T) {
	s := newTestMessagingService()
	s.RegisterEventRegistry(NewEventRegistry())

	_, ok := s.GetEventHandler("NoSuchEvent")
	if ok {
		t.Error("expected GetEventHandler to return false for unregistered event")
	}
}

// ----- Dual-write mode -----

func TestService_PublishDualWrite_BothSucceed(t *testing.T) {
	s := newTestMessagingService()
	s.config.Messaging.DualWrite = true

	sqsDriver := &mockDriver{driverName: "sqs"}
	rabbitDriver := &mockDriver{driverName: "rabbitmq"}
	s.RegisterDriver(config.DriverSQS, sqsDriver)
	s.RegisterDriver(config.DriverRabbitMQ, rabbitDriver)

	event := &mockEvent{eventType: "DualEvent"}
	if err := s.Publish(context.Background(), event); err != nil {
		t.Errorf("expected no error in dual-write mode, got %v", err)
	}

	if len(sqsDriver.published) != 1 {
		t.Errorf("expected SQS to receive 1 event, got %d", len(sqsDriver.published))
	}
	if len(rabbitDriver.published) != 1 {
		t.Errorf("expected RabbitMQ to receive 1 event, got %d", len(rabbitDriver.published))
	}
}

func TestService_PublishDualWrite_BothFail_ReturnsError(t *testing.T) {
	s := newTestMessagingService()
	s.config.Messaging.DualWrite = true

	s.RegisterDriver(config.DriverSQS, &mockDriver{
		driverName: "sqs", publishErr: errors.New("sqs failed"),
	})
	s.RegisterDriver(config.DriverRabbitMQ, &mockDriver{
		driverName: "rabbitmq", publishErr: errors.New("rabbit failed"),
	})

	err := s.Publish(context.Background(), &mockEvent{eventType: "DualEvent"})
	if err == nil {
		t.Error("expected error when both drivers fail in dual-write mode")
	}
}

func TestService_PublishDualWrite_OneSuceeds_NoError(t *testing.T) {
	s := newTestMessagingService()
	s.config.Messaging.DualWrite = true

	// SQS succeeds, RabbitMQ fails — should still return nil
	s.RegisterDriver(config.DriverSQS, &mockDriver{driverName: "sqs"})
	s.RegisterDriver(config.DriverRabbitMQ, &mockDriver{
		driverName: "rabbitmq", publishErr: errors.New("rabbit down"),
	})

	err := s.Publish(context.Background(), &mockEvent{eventType: "DualEvent"})
	if err != nil {
		t.Errorf("expected no error when at least one driver succeeds, got %v", err)
	}
}

// ----- Fallback mode -----

func TestService_Publish_FallsBackToRabbitMQ_OnSQSFailure(t *testing.T) {
	s := newTestMessagingService()
	s.config.Messaging.FallbackToRabbitMQ = true // SQS is already default driver

	sqsDriver := &mockDriver{driverName: "sqs", publishErr: errors.New("sqs down")}
	rabbitDriver := &mockDriver{driverName: "rabbitmq"}
	s.RegisterDriver(config.DriverSQS, sqsDriver)
	s.RegisterDriver(config.DriverRabbitMQ, rabbitDriver)

	event := &mockEvent{eventType: "FallbackEvent"}
	if err := s.Publish(context.Background(), event); err != nil {
		t.Errorf("expected no error with RabbitMQ fallback, got %v", err)
	}
	if len(rabbitDriver.published) != 1 {
		t.Errorf("expected RabbitMQ to receive 1 event via fallback, got %d", len(rabbitDriver.published))
	}
}

func TestService_Publish_FallbackWithNoRabbitDriver_ReturnsError(t *testing.T) {
	s := newTestMessagingService()
	s.config.Messaging.FallbackToRabbitMQ = true

	s.RegisterDriver(config.DriverSQS, &mockDriver{
		driverName: "sqs", publishErr: errors.New("sqs down"),
	})
	// No RabbitMQ driver registered

	err := s.Publish(context.Background(), &mockEvent{eventType: "FallbackEvent"})
	if err == nil {
		t.Error("expected error when RabbitMQ fallback is unavailable")
	}
}
