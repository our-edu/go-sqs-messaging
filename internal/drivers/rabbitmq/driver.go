// Package rabbitmq provides RabbitMQ driver for rollback support
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/our-edu/go-sqs-messaging/pkg/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

// Driver implements the MessagingDriver interface for RabbitMQ
type Driver struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *config.Config
	logger  zerolog.Logger
}

// RabbitMQConfig holds RabbitMQ connection settings
type RabbitMQConfig struct {
	URL          string
	Exchange     string
	ExchangeType string
	Durable      bool
}

// NewDriver creates a new RabbitMQ messaging driver
func NewDriver(cfg *config.Config, rabbitCfg RabbitMQConfig, logger zerolog.Logger) (*Driver, error) {
	conn, err := amqp.Dial(rabbitCfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare exchange
	err = channel.ExchangeDeclare(
		rabbitCfg.Exchange,
		rabbitCfg.ExchangeType,
		rabbitCfg.Durable,
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	return &Driver{
		conn:    conn,
		channel: channel,
		config:  cfg,
		logger:  logger,
	}, nil
}

// Publish sends an event through RabbitMQ
func (d *Driver) Publish(ctx context.Context, event contracts.Event) error {
	return d.PublishRaw(ctx, event.EventType(), event.Payload())
}

// PublishRaw sends a raw message with explicit event type and payload
func (d *Driver) PublishRaw(ctx context.Context, eventType string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Use event type as routing key
	routingKey := eventType

	err = d.channel.PublishWithContext(
		ctx,
		"",         // exchange (use default)
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers: amqp.Table{
				"event_type": eventType,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	d.logger.Info().
		Str("event_type", eventType).
		Str("routing_key", routingKey).
		Msg("Published message to RabbitMQ")

	return nil
}

// Name returns the driver name
func (d *Driver) Name() string {
	return string(config.DriverRabbitMQ)
}

// IsAvailable checks if the driver is available
func (d *Driver) IsAvailable(ctx context.Context, eventType string) (bool, error) {
	// Check if connection is still open
	if d.conn == nil || d.conn.IsClosed() {
		return false, nil
	}
	return true, nil
}

// Close closes the RabbitMQ connection
func (d *Driver) Close() error {
	if d.channel != nil {
		d.channel.Close()
	}
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// Ensure interface compliance
var _ contracts.MessagingDriver = (*Driver)(nil)
