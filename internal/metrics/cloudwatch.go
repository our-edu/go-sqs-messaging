// Package metrics provides CloudWatch metrics integration
package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/rs/zerolog"
)

// CloudWatchProvider handles sending metrics to AWS CloudWatch
type CloudWatchProvider struct {
	client    *cloudwatch.Client
	config    *config.Config
	logger    zerolog.Logger
	buffer    []types.MetricDatum
	mutex     sync.Mutex
	batchSize int
	enabled   bool
}

// CloudWatchConfig holds configuration for CloudWatch provider
type CloudWatchConfig struct {
	Enabled   bool
	Namespace string
}

// NewCloudWatchProvider creates a new CloudWatch metrics provider
func NewCloudWatchProvider(client *cloudwatch.Client, cfg *config.Config, logger zerolog.Logger) *CloudWatchProvider {
	return &CloudWatchProvider{
		client:    client,
		config:    cfg,
		logger:    logger,
		buffer:    make([]types.MetricDatum, 0),
		batchSize: 20, // CloudWatch max is 20 per request
		enabled:   cfg.SQS.CloudWatch.Enabled,
	}
}

// Ensure CloudWatchProvider implements Provider interface
var _ Provider = (*CloudWatchProvider)(nil)

// Name returns the provider name
func (s *CloudWatchProvider) Name() string {
	return string(ProviderTypeCloudWatch)
}

// Enabled returns whether CloudWatch metrics are enabled
func (s *CloudWatchProvider) Enabled() bool {
	return s.enabled
}

// PutMetric sends a single metric to CloudWatch
func (s *CloudWatchProvider) PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error {
	if !s.enabled {
		return nil
	}

	metric := s.createMetricDatum(name, value, unit, dimensions)

	_, err := s.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(s.config.SQS.CloudWatch.Namespace),
		MetricData: []types.MetricDatum{metric},
	})
	if err != nil {
		s.logger.Warn().
			Str("metric", name).
			Err(err).
			Msg("Failed to put CloudWatch metric")
		return err
	}

	s.logger.Debug().
		Str("metric", name).
		Float64("value", value).
		Msg("Put CloudWatch metric")
	return nil
}

// Increment increments a counter metric
func (s *CloudWatchProvider) Increment(ctx context.Context, name string, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, 1.0, "Count", dimensions)
}

// RecordDuration records a duration metric in milliseconds
func (s *CloudWatchProvider) RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, duration, "Milliseconds", dimensions)
}

// IncMessagesProcessed increments the messages processed counter
func (s *CloudWatchProvider) IncMessagesProcessed(ctx context.Context, queue, eventType, status string) {
	s.Increment(ctx, MetricMessagesProcessed, map[string]string{
		"queue":      queue,
		"event_type": eventType,
		"status":     status,
	})
}

// IncMessagesSuccess increments the success counter
func (s *CloudWatchProvider) IncMessagesSuccess(ctx context.Context, queue, eventType string) {
	s.Increment(ctx, MetricMessagesSuccess, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// IncValidationErrors increments validation errors counter
func (s *CloudWatchProvider) IncValidationErrors(ctx context.Context, queue, eventType string) {
	s.Increment(ctx, MetricValidationErrors, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// IncTransientErrors increments transient errors counter
func (s *CloudWatchProvider) IncTransientErrors(ctx context.Context, queue, eventType string) {
	s.Increment(ctx, MetricTransientErrors, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// IncPermanentErrors increments permanent errors counter
func (s *CloudWatchProvider) IncPermanentErrors(ctx context.Context, queue, eventType string) {
	s.Increment(ctx, MetricPermanentErrors, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// IncMessagesPublished increments the messages published counter
func (s *CloudWatchProvider) IncMessagesPublished(ctx context.Context, queue, eventType string) {
	s.Increment(ctx, MetricMessagesPublished, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// IncPublishErrors increments the publish errors counter
func (s *CloudWatchProvider) IncPublishErrors(ctx context.Context, queue, eventType string) {
	s.Increment(ctx, MetricPublishErrors, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// ObserveProcessingDuration records the processing duration
func (s *CloudWatchProvider) ObserveProcessingDuration(ctx context.Context, queue, eventType string, durationMs float64) {
	s.RecordDuration(ctx, MetricProcessingTime, durationMs, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// ObservePublishDuration records the publish duration
func (s *CloudWatchProvider) ObservePublishDuration(ctx context.Context, queue, eventType string, durationMs float64) {
	s.RecordDuration(ctx, MetricPublishDuration, durationMs, map[string]string{
		"queue":      queue,
		"event_type": eventType,
	})
}

// SetQueueDepth sets the current queue depth
func (s *CloudWatchProvider) SetQueueDepth(ctx context.Context, queue string, depth float64) {
	s.PutMetric(ctx, MetricQueueDepth, depth, "Count", map[string]string{
		"queue": queue,
	})
}

// SetDLQDepth sets the current DLQ depth
func (s *CloudWatchProvider) SetDLQDepth(ctx context.Context, queue string, depth float64) {
	s.PutMetric(ctx, MetricDLQDepth, depth, "Count", map[string]string{
		"queue": queue,
	})
}

// SetActiveConsumers sets the number of active consumers
func (s *CloudWatchProvider) SetActiveConsumers(ctx context.Context, queue string, count float64) {
	s.PutMetric(ctx, MetricActiveConsumers, count, "Count", map[string]string{
		"queue": queue,
	})
}

// IncActiveConsumers increments the active consumer count
func (s *CloudWatchProvider) IncActiveConsumers(ctx context.Context, queue string) {
	// CloudWatch doesn't support increment for gauges, so we just log
	s.logger.Debug().Str("queue", queue).Msg("IncActiveConsumers called - use SetActiveConsumers for CloudWatch")
}

// DecActiveConsumers decrements the active consumer count
func (s *CloudWatchProvider) DecActiveConsumers(ctx context.Context, queue string) {
	// CloudWatch doesn't support decrement for gauges, so we just log
	s.logger.Debug().Str("queue", queue).Msg("DecActiveConsumers called - use SetActiveConsumers for CloudWatch")
}

// BufferMetric adds a metric to the buffer for batch sending
func (s *CloudWatchProvider) BufferMetric(name string, value float64, unit string, dimensions map[string]string) {
	if !s.enabled {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	metric := s.createMetricDatum(name, value, unit, dimensions)
	s.buffer = append(s.buffer, metric)
}

// FlushBuffer sends all buffered metrics to CloudWatch
func (s *CloudWatchProvider) FlushBuffer(ctx context.Context) error {
	if !s.enabled {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.buffer) == 0 {
		return nil
	}

	// Send in batches of 20
	for i := 0; i < len(s.buffer); i += s.batchSize {
		end := i + s.batchSize
		if end > len(s.buffer) {
			end = len(s.buffer)
		}

		batch := s.buffer[i:end]
		_, err := s.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(s.config.SQS.CloudWatch.Namespace),
			MetricData: batch,
		})
		if err != nil {
			s.logger.Warn().
				Int("batch_size", len(batch)).
				Err(err).
				Msg("Failed to flush CloudWatch metrics batch")
			return err
		}
	}

	s.logger.Debug().Int("count", len(s.buffer)).Msg("Flushed CloudWatch metrics")
	s.buffer = make([]types.MetricDatum, 0)
	return nil
}

func (s *CloudWatchProvider) createMetricDatum(name string, value float64, unit string, dimensions map[string]string) types.MetricDatum {
	datum := types.MetricDatum{
		MetricName: aws.String(name),
		Value:      aws.Float64(value),
		Unit:       types.StandardUnit(unit),
		Timestamp:  aws.Time(time.Now()),
	}

	if len(dimensions) > 0 {
		cwDimensions := make([]types.Dimension, 0, len(dimensions))
		for k, v := range dimensions {
			// CloudWatch rejects empty dimension values, use "unknown" as fallback
			if v == "" {
				v = "unknown"
			}
			cwDimensions = append(cwDimensions, types.Dimension{
				Name:  aws.String(k),
				Value: aws.String(v),
			})
		}
		datum.Dimensions = cwDimensions
	}

	return datum
}

// Metrics constants for consistency
const (
	MetricMessagesProcessed = "sqs.messages.processed"
	MetricMessagesSuccess   = "sqs.messages.success"
	MetricValidationErrors  = "sqs.validation_errors"
	MetricTransientErrors   = "sqs.transient_errors"
	MetricPermanentErrors   = "sqs.permanent_errors"
	MetricProcessingTime    = "sqs.processing_time"
	MetricQueueDepth        = "sqs.queue_depth"
	MetricDLQDepth          = "sqs.dlq_depth"
	MetricMessagesPublished = "sqs.messages.published"
	MetricPublishErrors     = "sqs.publish_errors"
	MetricPublishDuration   = "sqs.publish_duration"
	MetricActiveConsumers   = "sqs.active_consumers"
)

// Legacy type alias for backward compatibility
// Deprecated: Use CloudWatchProvider instead
type CloudWatchService = CloudWatchProvider

// NewCloudWatchService creates a new CloudWatch metrics service
// Deprecated: Use NewCloudWatchProvider instead
func NewCloudWatchService(client *cloudwatch.Client, cfg *config.Config, logger zerolog.Logger) *CloudWatchService {
	return NewCloudWatchProvider(client, cfg, logger)
}
