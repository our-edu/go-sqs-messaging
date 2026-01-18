package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	"github.com/our-edu/go-sqs-messaging/pkg/envelope"
	"github.com/rs/zerolog"
)

// Publisher handles publishing messages to SQS queues
type Publisher struct {
	client   *sqs.Client
	resolver *Resolver
	config   *config.Config
	logger   zerolog.Logger
	service  string
}

// NewPublisher creates a new SQS publisher
func NewPublisher(client *sqs.Client, resolver *Resolver, cfg *config.Config, logger zerolog.Logger, service string) *Publisher {
	return &Publisher{
		client:   client,
		resolver: resolver,
		config:   cfg,
		logger:   logger,
		service:  service,
	}
}

// Publish sends a single message to the queue
func (p *Publisher) Publish(ctx context.Context, queueName, eventType string, payload map[string]any) (string, error) {
	// Resolve queue URL
	queueURL, err := p.resolver.Resolve(ctx, queueName)
	if err != nil {
		return "", fmt.Errorf("failed to resolve queue URL: %w", err)
	}

	// Create envelope
	env := envelope.Wrap(eventType, payload, p.service)
	messageBody, err := env.ToJSON()
	if err != nil {
		return "", fmt.Errorf("failed to serialize envelope: %w", err)
	}

	// Send message
	result, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(messageBody),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"EventType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(eventType),
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to send message to SQS: %w", err)
	}

	p.logger.Info().
		Str("queue", queueName).
		Str("event_type", eventType).
		Str("message_id", *result.MessageId).
		Str("idempotency_key", env.IdempotencyKey).
		Msg("Published message to SQS")

	return *result.MessageId, nil
}

// PublishBatch sends multiple messages to the queue (max 10)
func (p *Publisher) PublishBatch(ctx context.Context, queueName string, messages []contracts.BatchMessage) ([]contracts.BatchResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	if len(messages) > 10 {
		return nil, fmt.Errorf("batch size exceeds maximum of 10 messages")
	}

	// Resolve queue URL
	queueURL, err := p.resolver.Resolve(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve queue URL: %w", err)
	}

	// Build batch entries
	entries := make([]types.SendMessageBatchRequestEntry, len(messages))
	for i, msg := range messages {
		env := envelope.Wrap(msg.EventType, msg.Payload, p.service)
		messageBody, err := env.ToJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize envelope for message %s: %w", msg.ID, err)
		}

		entries[i] = types.SendMessageBatchRequestEntry{
			Id:          aws.String(msg.ID),
			MessageBody: aws.String(messageBody),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"EventType": {
					DataType:    aws.String("String"),
					StringValue: aws.String(msg.EventType),
				},
			},
		}
	}

	// Send batch
	result, err := p.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send batch to SQS: %w", err)
	}

	// Build results
	results := make([]contracts.BatchResult, 0, len(messages))
	for _, successful := range result.Successful {
		results = append(results, contracts.BatchResult{
			ID:        *successful.Id,
			MessageID: *successful.MessageId,
		})
	}
	for _, failed := range result.Failed {
		results = append(results, contracts.BatchResult{
			ID:    *failed.Id,
			Error: fmt.Errorf("%s: %s", *failed.Code, *failed.Message),
		})
	}

	p.logger.Info().
		Str("queue", queueName).
		Int("successful", len(result.Successful)).
		Int("failed", len(result.Failed)).
		Msg("Published batch to SQS")

	return results, nil
}

// PublisherAdapter adapts legacy event interfaces to SQS publishing
type PublisherAdapter struct {
	publisher      *Publisher
	targetResolver *TargetQueueResolver
	logger         zerolog.Logger
}

// NewPublisherAdapter creates a new publisher adapter
func NewPublisherAdapter(publisher *Publisher, targetResolver *TargetQueueResolver, logger zerolog.Logger) *PublisherAdapter {
	return &PublisherAdapter{
		publisher:      publisher,
		targetResolver: targetResolver,
		logger:         logger,
	}
}

// Publish publishes an event to the appropriate queue
func (a *PublisherAdapter) Publish(ctx context.Context, event contracts.Event) (string, error) {
	eventType := event.EventType()
	targetQueue := a.targetResolver.Resolve(eventType)
	return a.publisher.Publish(ctx, targetQueue, eventType, event.Payload())
}

// PublishRaw publishes raw event data
func (a *PublisherAdapter) PublishRaw(ctx context.Context, eventType string, payload map[string]any) (string, error) {
	targetQueue := a.targetResolver.Resolve(eventType)
	return a.publisher.Publish(ctx, targetQueue, eventType, payload)
}
