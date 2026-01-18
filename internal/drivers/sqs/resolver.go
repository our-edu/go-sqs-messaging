// Package sqs provides the AWS SQS driver implementation for the messaging system.
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/rs/zerolog"
)

// Resolver handles queue URL resolution with caching and lazy creation
type Resolver struct {
	client *sqs.Client
	config *config.Config
	logger zerolog.Logger
	cache  map[string]string
	mutex  sync.RWMutex
}

// NewResolver creates a new SQS queue resolver
func NewResolver(client *sqs.Client, cfg *config.Config, logger zerolog.Logger) *Resolver {
	return &Resolver{
		client: client,
		config: cfg,
		logger: logger,
		cache:  make(map[string]string),
	}
}

// Resolve returns the queue URL, creating the queue if necessary
func (r *Resolver) Resolve(ctx context.Context, queueName string) (string, error) {
	prefixedName := r.config.GetPrefixedQueueName(queueName)

	// Check cache first
	r.mutex.RLock()
	if url, ok := r.cache[prefixedName]; ok {
		r.mutex.RUnlock()
		return url, nil
	}
	r.mutex.RUnlock()

	// Try to get existing queue URL
	result, err := r.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(prefixedName),
	})
	if err == nil {
		r.cacheURL(prefixedName, *result.QueueUrl)
		return *result.QueueUrl, nil
	}

	// Queue doesn't exist, create it with DLQ
	r.logger.Info().Str("queue", prefixedName).Msg("Queue not found, creating with DLQ")
	return r.CreateQueueWithDLQ(ctx, queueName)
}

// QueueExists checks if a queue exists without creating it
func (r *Resolver) QueueExists(ctx context.Context, queueName string) (bool, error) {
	prefixedName := r.config.GetPrefixedQueueName(queueName)

	_, err := r.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(prefixedName),
	})
	if err != nil {
		return false, nil
	}
	return true, nil
}

// CreateQueue creates a new queue with the given name
func (r *Resolver) CreateQueue(ctx context.Context, queueName string) (string, error) {
	prefixedName := r.config.GetPrefixedQueueName(queueName)

	result, err := r.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(prefixedName),
		Attributes: map[string]string{
			"VisibilityTimeout":             strconv.Itoa(r.config.SQS.VisibilityTimeout),
			"ReceiveMessageWaitTimeSeconds": strconv.Itoa(r.config.SQS.LongPollingWait),
			"MessageRetentionPeriod":        strconv.Itoa(r.config.SQS.MessageRetention * 24 * 60 * 60),
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create queue %s: %w", prefixedName, err)
	}

	r.cacheURL(prefixedName, *result.QueueUrl)
	r.logger.Info().Str("queue", prefixedName).Str("url", *result.QueueUrl).Msg("Created queue")
	return *result.QueueUrl, nil
}

// CreateQueueWithDLQ creates a queue with an associated Dead Letter Queue
func (r *Resolver) CreateQueueWithDLQ(ctx context.Context, queueName string) (string, error) {
	prefixedName := r.config.GetPrefixedQueueName(queueName)
	dlqName := r.config.GetDLQName(prefixedName)

	// Create DLQ first
	dlqResult, err := r.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
		Attributes: map[string]string{
			"MessageRetentionPeriod": strconv.Itoa(r.config.SQS.MessageRetention * 24 * 60 * 60),
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create DLQ %s: %w", dlqName, err)
	}
	r.logger.Info().Str("dlq", dlqName).Msg("Created DLQ")

	// Get DLQ ARN
	dlqAttrs, err := r.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqResult.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", fmt.Errorf("failed to get DLQ ARN: %w", err)
	}
	dlqArn := dlqAttrs.Attributes[string(types.QueueAttributeNameQueueArn)]

	// Create redrive policy
	redrivePolicy := map[string]interface{}{
		"deadLetterTargetArn": dlqArn,
		"maxReceiveCount":     r.config.SQS.DLQMaxReceiveCount,
	}
	redrivePolicyJSON, _ := json.Marshal(redrivePolicy)

	// Create main queue with redrive policy
	result, err := r.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(prefixedName),
		Attributes: map[string]string{
			"VisibilityTimeout":             strconv.Itoa(r.config.SQS.VisibilityTimeout),
			"ReceiveMessageWaitTimeSeconds": strconv.Itoa(r.config.SQS.LongPollingWait),
			"MessageRetentionPeriod":        strconv.Itoa(r.config.SQS.MessageRetention * 24 * 60 * 60),
			"RedrivePolicy":                 string(redrivePolicyJSON),
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create queue %s: %w", prefixedName, err)
	}

	r.cacheURL(prefixedName, *result.QueueUrl)
	r.logger.Info().
		Str("queue", prefixedName).
		Str("dlq", dlqName).
		Str("url", *result.QueueUrl).
		Msg("Created queue with DLQ")
	return *result.QueueUrl, nil
}

// GetDLQUrl returns the DLQ URL for a given queue
func (r *Resolver) GetDLQUrl(ctx context.Context, queueName string) (string, error) {
	prefixedName := r.config.GetPrefixedQueueName(queueName)
	dlqName := r.config.GetDLQName(prefixedName)

	result, err := r.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(dlqName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get DLQ URL for %s: %w", dlqName, err)
	}
	return *result.QueueUrl, nil
}

func (r *Resolver) cacheURL(name, url string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.cache[name] = url
}

// ClearCache clears the queue URL cache
func (r *Resolver) ClearCache() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.cache = make(map[string]string)
}

// TargetQueueResolver maps event types to target consumer queues
type TargetQueueResolver struct {
	config *config.Config
}

// NewTargetQueueResolver creates a new target queue resolver
func NewTargetQueueResolver(cfg *config.Config) *TargetQueueResolver {
	return &TargetQueueResolver{config: cfg}
}

// Resolve returns the target queue for an event type
func (r *TargetQueueResolver) Resolve(eventType string) string {
	return r.config.GetTargetQueue(eventType)
}
