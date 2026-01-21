package sqsmessaging

import (
	"github.com/our-edu/go-sqs-messaging/internal/metrics"
)

// Re-export metrics types for convenience

// MetricsProvider is the unified interface for all metrics providers.
type MetricsProvider = metrics.Provider

// HTTPMetricsProvider is an optional interface for providers that expose HTTP handlers.
type HTTPMetricsProvider = metrics.HTTPProvider

// CollectorMetricsProvider is an optional interface for providers that expose Prometheus collectors.
type CollectorMetricsProvider = metrics.CollectorProvider

// MetricsProviderType represents the type of metrics provider.
type MetricsProviderType = metrics.ProviderType

// Metrics provider type constants
const (
	MetricsProviderCloudWatch = metrics.ProviderTypeCloudWatch
	MetricsProviderPrometheus = metrics.ProviderTypePrometheus
	MetricsProviderNoop       = metrics.ProviderTypeNoop
	MetricsProviderComposite  = metrics.ProviderTypeComposite
)

// Metrics constants for consistency
const (
	MetricMessagesProcessed = metrics.MetricMessagesProcessed
	MetricMessagesSuccess   = metrics.MetricMessagesSuccess
	MetricValidationErrors  = metrics.MetricValidationErrors
	MetricTransientErrors   = metrics.MetricTransientErrors
	MetricPermanentErrors   = metrics.MetricPermanentErrors
	MetricProcessingTime    = metrics.MetricProcessingTime
	MetricQueueDepth        = metrics.MetricQueueDepth
	MetricDLQDepth          = metrics.MetricDLQDepth
	MetricMessagesPublished = metrics.MetricMessagesPublished
	MetricPublishErrors     = metrics.MetricPublishErrors
	MetricPublishDuration   = metrics.MetricPublishDuration
	MetricActiveConsumers   = metrics.MetricActiveConsumers
)
