// Package messaging provides the main messaging service with strategy pattern
package messaging

import (
	"context"
	"fmt"
	"sync"

	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	"github.com/rs/zerolog"
)

// Service is the main messaging service that manages drivers and publishing
type Service struct {
	config        *config.Config
	logger        zerolog.Logger
	drivers       map[config.DriverType]contracts.MessagingDriver
	eventRegistry contracts.EventRegistry
	mutex         sync.RWMutex
}

// NewService creates a new messaging service
func NewService(cfg *config.Config, logger zerolog.Logger) *Service {
	return &Service{
		config:  cfg,
		logger:  logger,
		drivers: make(map[config.DriverType]contracts.MessagingDriver),
	}
}

// RegisterDriver registers a messaging driver
func (s *Service) RegisterDriver(driverType config.DriverType, driver contracts.MessagingDriver) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.drivers[driverType] = driver
	s.logger.Info().Str("driver", string(driverType)).Msg("Registered messaging driver")
}

// RegisterEventRegistry registers the event registry for handler lookups
func (s *Service) RegisterEventRegistry(registry contracts.EventRegistry) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.eventRegistry = registry
}

// GetDriver returns the current active driver name
func (s *Service) GetDriver() config.DriverType {
	return s.config.Messaging.Driver
}

// IsSQS returns true if the active driver is SQS
func (s *Service) IsSQS() bool {
	return s.config.Messaging.Driver == config.DriverSQS
}

// IsRabbitMQ returns true if the active driver is RabbitMQ
func (s *Service) IsRabbitMQ() bool {
	return s.config.Messaging.Driver == config.DriverRabbitMQ
}

// GetAvailableDrivers returns a list of registered driver names
func (s *Service) GetAvailableDrivers() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	drivers := make([]string, 0, len(s.drivers))
	for name := range s.drivers {
		drivers = append(drivers, string(name))
	}
	return drivers
}

// Publish sends an event through the messaging system
func (s *Service) Publish(ctx context.Context, event contracts.Event) error {
	eventType := event.EventType()

	// Dual write mode: publish to both drivers
	if s.config.Messaging.DualWrite {
		return s.publishDualWrite(ctx, event)
	}

	// Get active driver
	s.mutex.RLock()
	driver, ok := s.drivers[s.config.Messaging.Driver]
	s.mutex.RUnlock()
	if !ok {
		return fmt.Errorf("driver %s not registered", s.config.Messaging.Driver)
	}

	// Publish to active driver
	err := driver.Publish(ctx, event)
	if err != nil {
		s.logger.Error().
			Str("driver", string(s.config.Messaging.Driver)).
			Str("event_type", eventType).
			Err(err).
			Msg("Failed to publish to primary driver")

		// Fallback to RabbitMQ if enabled
		if s.config.Messaging.FallbackToRabbitMQ && s.config.Messaging.Driver == config.DriverSQS {
			return s.publishWithFallback(ctx, event, err)
		}
		return err
	}

	s.logger.Info().
		Str("driver", string(s.config.Messaging.Driver)).
		Str("event_type", eventType).
		Msg("Published event")
	return nil
}

// PublishRaw sends a raw message through the messaging system
func (s *Service) PublishRaw(ctx context.Context, eventType string, payload map[string]interface{}) error {
	// Get active driver
	s.mutex.RLock()
	driver, ok := s.drivers[s.config.Messaging.Driver]
	s.mutex.RUnlock()
	if !ok {
		return fmt.Errorf("driver %s not registered", s.config.Messaging.Driver)
	}

	return driver.PublishRaw(ctx, eventType, payload)
}

func (s *Service) publishDualWrite(ctx context.Context, event contracts.Event) error {
	eventType := event.EventType()
	var sqsErr, rabbitErr error

	s.mutex.RLock()
	sqsDriver := s.drivers[config.DriverSQS]
	rabbitDriver := s.drivers[config.DriverRabbitMQ]
	s.mutex.RUnlock()

	// Publish to SQS
	if sqsDriver != nil {
		sqsErr = sqsDriver.Publish(ctx, event)
		if sqsErr != nil {
			s.logger.Error().
				Str("event_type", eventType).
				Err(sqsErr).
				Msg("Failed to publish to SQS in dual-write mode")
		}
	}

	// Publish to RabbitMQ
	if rabbitDriver != nil {
		rabbitErr = rabbitDriver.Publish(ctx, event)
		if rabbitErr != nil {
			s.logger.Error().
				Str("event_type", eventType).
				Err(rabbitErr).
				Msg("Failed to publish to RabbitMQ in dual-write mode")
		}
	}

	// Return error if both failed
	if sqsErr != nil && rabbitErr != nil {
		return fmt.Errorf("dual-write failed: SQS: %v, RabbitMQ: %v", sqsErr, rabbitErr)
	}

	s.logger.Info().
		Str("event_type", eventType).
		Bool("sqs_success", sqsErr == nil).
		Bool("rabbitmq_success", rabbitErr == nil).
		Msg("Published event in dual-write mode")

	return nil
}

func (s *Service) publishWithFallback(ctx context.Context, event contracts.Event, originalErr error) error {
	eventType := event.EventType()

	s.mutex.RLock()
	rabbitDriver := s.drivers[config.DriverRabbitMQ]
	s.mutex.RUnlock()

	if rabbitDriver == nil {
		return fmt.Errorf("fallback failed: RabbitMQ driver not registered. Original error: %w", originalErr)
	}

	s.logger.Warn().
		Str("event_type", eventType).
		Err(originalErr).
		Msg("Falling back to RabbitMQ")

	err := rabbitDriver.Publish(ctx, event)
	if err != nil {
		return fmt.Errorf("fallback to RabbitMQ failed: %w. Original error: %v", err, originalErr)
	}

	s.logger.Info().
		Str("event_type", eventType).
		Msg("Published event via RabbitMQ fallback")
	return nil
}

// GetEventHandler returns the handler for an event type
func (s *Service) GetEventHandler(eventType string) (contracts.EventHandler, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.eventRegistry == nil {
		return nil, false
	}
	return s.eventRegistry.GetHandler(eventType)
}
