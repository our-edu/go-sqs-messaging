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

// CloudWatchService handles sending metrics to AWS CloudWatch
type CloudWatchService struct {
	client    *cloudwatch.Client
	config    *config.Config
	logger    zerolog.Logger
	buffer    []types.MetricDatum
	mutex     sync.Mutex
	batchSize int
}

// NewCloudWatchService creates a new CloudWatch metrics service
func NewCloudWatchService(client *cloudwatch.Client, cfg *config.Config, logger zerolog.Logger) *CloudWatchService {
	return &CloudWatchService{
		client:    client,
		config:    cfg,
		logger:    logger,
		buffer:    make([]types.MetricDatum, 0),
		batchSize: 20, // CloudWatch max is 20 per request
	}
}

// PutMetric sends a single metric to CloudWatch
func (s *CloudWatchService) PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error {
	if !s.config.SQS.CloudWatch.Enabled {
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
func (s *CloudWatchService) Increment(ctx context.Context, name string, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, 1.0, "Count", dimensions)
}

// RecordDuration records a duration metric in milliseconds
func (s *CloudWatchService) RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, duration, "Milliseconds", dimensions)
}

// BufferMetric adds a metric to the buffer for batch sending
func (s *CloudWatchService) BufferMetric(name string, value float64, unit string, dimensions map[string]string) {
	if !s.config.SQS.CloudWatch.Enabled {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	metric := s.createMetricDatum(name, value, unit, dimensions)
	s.buffer = append(s.buffer, metric)
}

// FlushBuffer sends all buffered metrics to CloudWatch
func (s *CloudWatchService) FlushBuffer(ctx context.Context) error {
	if !s.config.SQS.CloudWatch.Enabled {
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

func (s *CloudWatchService) createMetricDatum(name string, value float64, unit string, dimensions map[string]string) types.MetricDatum {
	datum := types.MetricDatum{
		MetricName: aws.String(name),
		Value:      aws.Float64(value),
		Unit:       types.StandardUnit(unit),
		Timestamp:  aws.Time(time.Now()),
	}

	if len(dimensions) > 0 {
		cwDimensions := make([]types.Dimension, 0, len(dimensions))
		for k, v := range dimensions {
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
)
