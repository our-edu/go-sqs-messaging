package sqs

import (
	"context"

	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	"github.com/rs/zerolog"
)

// Driver implements the MessagingDriver interface for SQS
type Driver struct {
	adapter  *PublisherAdapter
	resolver *Resolver
	config   *config.Config
	logger   zerolog.Logger
}

// NewDriver creates a new SQS messaging driver
func NewDriver(adapter *PublisherAdapter, resolver *Resolver, cfg *config.Config, logger zerolog.Logger) *Driver {
	return &Driver{
		adapter:  adapter,
		resolver: resolver,
		config:   cfg,
		logger:   logger,
	}
}

// Publish sends an event through SQS
func (d *Driver) Publish(ctx context.Context, event contracts.Event) error {
	_, err := d.adapter.Publish(ctx, event)
	return err
}

// PublishRaw sends a raw message with explicit event type and payload
func (d *Driver) PublishRaw(ctx context.Context, eventType string, payload map[string]interface{}) error {
	_, err := d.adapter.PublishRaw(ctx, eventType, payload)
	return err
}

// Name returns the driver name
func (d *Driver) Name() string {
	return string(config.DriverSQS)
}

// IsAvailable checks if the driver is available for the given event type
func (d *Driver) IsAvailable(ctx context.Context, eventType string) (bool, error) {
	targetQueue := d.adapter.targetResolver.Resolve(eventType)
	return d.resolver.QueueExists(ctx, targetQueue)
}

// Ensure interface compliance
var _ contracts.MessagingDriver = (*Driver)(nil)
