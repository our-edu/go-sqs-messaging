// Package metrics provides metrics integration for SQS messaging
package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

// Provider defines the unified interface for all metrics providers.
// Implementations include CloudWatch, Prometheus, Noop, and Composite providers.
type Provider interface {
	// Core metrics methods
	PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error
	Increment(ctx context.Context, name string, dimensions map[string]string) error
	RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error

	// Convenience methods for common operations
	IncMessagesProcessed(ctx context.Context, queue, eventType, status string)
	IncMessagesSuccess(ctx context.Context, queue, eventType string)
	IncValidationErrors(ctx context.Context, queue, eventType string)
	IncTransientErrors(ctx context.Context, queue, eventType string)
	IncPermanentErrors(ctx context.Context, queue, eventType string)
	IncMessagesPublished(ctx context.Context, queue, eventType string)
	IncPublishErrors(ctx context.Context, queue, eventType string)

	// Duration recording
	ObserveProcessingDuration(ctx context.Context, queue, eventType string, durationMs float64)
	ObservePublishDuration(ctx context.Context, queue, eventType string, durationMs float64)

	// Gauge operations
	SetQueueDepth(ctx context.Context, queue string, depth float64)
	SetDLQDepth(ctx context.Context, queue string, depth float64)
	SetActiveConsumers(ctx context.Context, queue string, count float64)
	IncActiveConsumers(ctx context.Context, queue string)
	DecActiveConsumers(ctx context.Context, queue string)

	// Provider info
	Name() string
	Enabled() bool
}

// HTTPProvider is an optional interface for providers that expose HTTP handlers (e.g., Prometheus)
type HTTPProvider interface {
	Provider
	Handler() http.Handler
	HandlerFunc() http.HandlerFunc
}

// CollectorProvider is an optional interface for providers that expose Prometheus collectors
type CollectorProvider interface {
	Provider
	Collectors() []prometheus.Collector
	Register() error
}

// ProviderType represents the type of metrics provider
type ProviderType string

const (
	ProviderTypeCloudWatch ProviderType = "cloudwatch"
	ProviderTypePrometheus ProviderType = "prometheus"
	ProviderTypeNoop       ProviderType = "noop"
	ProviderTypeComposite  ProviderType = "composite"
)
