// Package metrics provides metrics integration for SQS messaging
package metrics

import (
	"context"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// PrometheusProvider handles exposing metrics to Prometheus
type PrometheusProvider struct {
	logger    zerolog.Logger
	namespace string
	subsystem string
	enabled   bool

	// Custom registry (if provided)
	registry prometheus.Registerer
	gatherer prometheus.Gatherer

	// Counters
	messagesProcessed *prometheus.CounterVec
	messagesSuccess   *prometheus.CounterVec
	validationErrors  *prometheus.CounterVec
	transientErrors   *prometheus.CounterVec
	permanentErrors   *prometheus.CounterVec
	messagesPublished *prometheus.CounterVec
	publishErrors     *prometheus.CounterVec

	// Gauges
	queueDepth    *prometheus.GaugeVec
	dlqDepth      *prometheus.GaugeVec
	consumerCount *prometheus.GaugeVec

	// Histograms
	processingDuration *prometheus.HistogramVec
	publishDuration    *prometheus.HistogramVec

	// Track if already registered
	registered bool
	mu         sync.Mutex
}

// PrometheusConfig holds configuration for Prometheus metrics
type PrometheusConfig struct {
	Enabled   bool                  // Whether Prometheus metrics are enabled
	Namespace string                // Metric namespace (e.g., "sqsmessaging")
	Subsystem string                // Metric subsystem (e.g., "consumer")
	Registry  prometheus.Registerer // Custom registry (optional, defaults to prometheus.DefaultRegisterer)
}

// DefaultPrometheusConfig returns the default Prometheus configuration
func DefaultPrometheusConfig() PrometheusConfig {
	return PrometheusConfig{
		Enabled:   true,
		Namespace: "sqsmessaging",
		Subsystem: "",
		Registry:  nil, // Will use default registerer
	}
}

// NewPrometheusProvider creates a new Prometheus metrics provider
func NewPrometheusProvider(logger zerolog.Logger, cfg PrometheusConfig) *PrometheusProvider {
	if cfg.Namespace == "" {
		cfg.Namespace = "sqsmessaging"
	}

	s := &PrometheusProvider{
		logger:    logger,
		namespace: cfg.Namespace,
		subsystem: cfg.Subsystem,
		registry:  cfg.Registry,
		enabled:   cfg.Enabled,
	}

	// If a custom registry is provided, try to get the gatherer for it
	if cfg.Registry != nil {
		if reg, ok := cfg.Registry.(*prometheus.Registry); ok {
			s.gatherer = reg
		}
	}

	s.initMetrics()
	return s
}

// Ensure PrometheusProvider implements Provider, HTTPProvider, and CollectorProvider interfaces
var _ Provider = (*PrometheusProvider)(nil)
var _ HTTPProvider = (*PrometheusProvider)(nil)
var _ CollectorProvider = (*PrometheusProvider)(nil)

// Name returns the provider name
func (s *PrometheusProvider) Name() string {
	return string(ProviderTypePrometheus)
}

// Enabled returns whether Prometheus metrics are enabled
func (s *PrometheusProvider) Enabled() bool {
	return s.enabled
}

func (s *PrometheusProvider) initMetrics() {
	// Counters
	s.messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "messages_processed_total",
			Help:      "Total number of messages processed",
		},
		[]string{"queue", "event_type", "status"},
	)

	s.messagesSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "messages_success_total",
			Help:      "Total number of messages processed successfully",
		},
		[]string{"queue", "event_type"},
	)

	s.validationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "validation_errors_total",
			Help:      "Total number of validation errors",
		},
		[]string{"queue", "event_type"},
	)

	s.transientErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "transient_errors_total",
			Help:      "Total number of transient errors (will be retried)",
		},
		[]string{"queue", "event_type"},
	)

	s.permanentErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "permanent_errors_total",
			Help:      "Total number of permanent errors (will not be retried)",
		},
		[]string{"queue", "event_type"},
	)

	s.messagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "messages_published_total",
			Help:      "Total number of messages published",
		},
		[]string{"queue", "event_type"},
	)

	s.publishErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "publish_errors_total",
			Help:      "Total number of publish errors",
		},
		[]string{"queue", "event_type"},
	)

	// Gauges
	s.queueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "queue_depth",
			Help:      "Current approximate number of messages in the queue",
		},
		[]string{"queue"},
	)

	s.dlqDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "dlq_depth",
			Help:      "Current approximate number of messages in the dead letter queue",
		},
		[]string{"queue"},
	)

	s.consumerCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "active_consumers",
			Help:      "Number of active consumers",
		},
		[]string{"queue"},
	)

	// Histograms
	s.processingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "processing_duration_milliseconds",
			Help:      "Message processing duration in milliseconds",
			Buckets:   []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		},
		[]string{"queue", "event_type"},
	)

	s.publishDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: s.namespace,
			Subsystem: s.subsystem,
			Name:      "publish_duration_milliseconds",
			Help:      "Message publish duration in milliseconds",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"queue", "event_type"},
	)
}

// Register registers all metrics with the Prometheus registry.
// If a custom registry was provided via PrometheusConfig.Registry, metrics
// will be registered there. Otherwise, metrics are registered with the
// default Prometheus registry.
func (s *PrometheusProvider) Register() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.registered {
		return nil
	}

	collectors := []prometheus.Collector{
		s.messagesProcessed,
		s.messagesSuccess,
		s.validationErrors,
		s.transientErrors,
		s.permanentErrors,
		s.messagesPublished,
		s.publishErrors,
		s.queueDepth,
		s.dlqDepth,
		s.consumerCount,
		s.processingDuration,
		s.publishDuration,
	}

	// Use custom registry if provided, otherwise use default
	registerer := s.registry
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	for _, c := range collectors {
		if err := registerer.Register(c); err != nil {
			// Ignore already registered errors
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				return err
			}
		}
	}

	s.registered = true
	s.logger.Info().Msg("Prometheus metrics registered")
	return nil
}

// Collectors returns all Prometheus collectors used by this service.
// This allows manual registration to a custom registry if needed.
func (s *PrometheusProvider) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		s.messagesProcessed,
		s.messagesSuccess,
		s.validationErrors,
		s.transientErrors,
		s.permanentErrors,
		s.messagesPublished,
		s.publishErrors,
		s.queueDepth,
		s.dlqDepth,
		s.consumerCount,
		s.processingDuration,
		s.publishDuration,
	}
}

// Handler returns an http.Handler for the /metrics endpoint.
// If a custom registry was provided, returns a handler for that registry.
// Otherwise, returns the default promhttp.Handler().
func (s *PrometheusProvider) Handler() http.Handler {
	// If we have a custom gatherer (from custom registry), use it
	if s.gatherer != nil {
		return promhttp.HandlerFor(s.gatherer, promhttp.HandlerOpts{})
	}
	// Otherwise use the default handler
	return promhttp.Handler()
}

// HandlerFunc returns an http.HandlerFunc for the /metrics endpoint.
func (s *PrometheusProvider) HandlerFunc() http.HandlerFunc {
	return s.Handler().ServeHTTP
}

// PutMetric implements the Provider interface
func (s *PrometheusProvider) PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error {
	if !s.enabled {
		return nil
	}

	queue := dimensions["queue"]
	eventType := dimensions["event_type"]

	switch name {
	case MetricMessagesProcessed:
		status := dimensions["status"]
		s.messagesProcessed.WithLabelValues(queue, eventType, status).Add(value)
	case MetricMessagesSuccess:
		s.messagesSuccess.WithLabelValues(queue, eventType).Add(value)
	case MetricValidationErrors:
		s.validationErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricTransientErrors:
		s.transientErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricPermanentErrors:
		s.permanentErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricMessagesPublished:
		s.messagesPublished.WithLabelValues(queue, eventType).Add(value)
	case MetricPublishErrors:
		s.publishErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricProcessingTime:
		s.processingDuration.WithLabelValues(queue, eventType).Observe(value)
	case MetricPublishDuration:
		s.publishDuration.WithLabelValues(queue, eventType).Observe(value)
	case MetricQueueDepth:
		s.queueDepth.WithLabelValues(queue).Set(value)
	case MetricDLQDepth:
		s.dlqDepth.WithLabelValues(queue).Set(value)
	case MetricActiveConsumers:
		s.consumerCount.WithLabelValues(queue).Set(value)
	}
	return nil
}

// Increment implements the Provider interface
func (s *PrometheusProvider) Increment(ctx context.Context, name string, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, 1.0, "Count", dimensions)
}

// RecordDuration implements the Provider interface
func (s *PrometheusProvider) RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, duration, "Milliseconds", dimensions)
}

// IncMessagesProcessed increments the messages processed counter
func (s *PrometheusProvider) IncMessagesProcessed(ctx context.Context, queue, eventType, status string) {
	if s.enabled {
		s.messagesProcessed.WithLabelValues(queue, eventType, status).Inc()
	}
}

// IncMessagesSuccess increments the messages success counter
func (s *PrometheusProvider) IncMessagesSuccess(ctx context.Context, queue, eventType string) {
	if s.enabled {
		s.messagesSuccess.WithLabelValues(queue, eventType).Inc()
	}
}

// IncValidationErrors increments the validation errors counter
func (s *PrometheusProvider) IncValidationErrors(ctx context.Context, queue, eventType string) {
	if s.enabled {
		s.validationErrors.WithLabelValues(queue, eventType).Inc()
	}
}

// IncTransientErrors increments the transient errors counter
func (s *PrometheusProvider) IncTransientErrors(ctx context.Context, queue, eventType string) {
	if s.enabled {
		s.transientErrors.WithLabelValues(queue, eventType).Inc()
	}
}

// IncPermanentErrors increments the permanent errors counter
func (s *PrometheusProvider) IncPermanentErrors(ctx context.Context, queue, eventType string) {
	if s.enabled {
		s.permanentErrors.WithLabelValues(queue, eventType).Inc()
	}
}

// IncMessagesPublished increments the messages published counter
func (s *PrometheusProvider) IncMessagesPublished(ctx context.Context, queue, eventType string) {
	if s.enabled {
		s.messagesPublished.WithLabelValues(queue, eventType).Inc()
	}
}

// IncPublishErrors increments the publish errors counter
func (s *PrometheusProvider) IncPublishErrors(ctx context.Context, queue, eventType string) {
	if s.enabled {
		s.publishErrors.WithLabelValues(queue, eventType).Inc()
	}
}

// ObserveProcessingDuration records the processing duration
func (s *PrometheusProvider) ObserveProcessingDuration(ctx context.Context, queue, eventType string, durationMs float64) {
	if s.enabled {
		s.processingDuration.WithLabelValues(queue, eventType).Observe(durationMs)
	}
}

// ObservePublishDuration records the publish duration
func (s *PrometheusProvider) ObservePublishDuration(ctx context.Context, queue, eventType string, durationMs float64) {
	if s.enabled {
		s.publishDuration.WithLabelValues(queue, eventType).Observe(durationMs)
	}
}

// SetQueueDepth sets the current queue depth
func (s *PrometheusProvider) SetQueueDepth(ctx context.Context, queue string, depth float64) {
	if s.enabled {
		s.queueDepth.WithLabelValues(queue).Set(depth)
	}
}

// SetDLQDepth sets the current DLQ depth
func (s *PrometheusProvider) SetDLQDepth(ctx context.Context, queue string, depth float64) {
	if s.enabled {
		s.dlqDepth.WithLabelValues(queue).Set(depth)
	}
}

// SetActiveConsumers sets the number of active consumers
func (s *PrometheusProvider) SetActiveConsumers(ctx context.Context, queue string, count float64) {
	if s.enabled {
		s.consumerCount.WithLabelValues(queue).Set(count)
	}
}

// IncActiveConsumers increments the active consumer count
func (s *PrometheusProvider) IncActiveConsumers(ctx context.Context, queue string) {
	if s.enabled {
		s.consumerCount.WithLabelValues(queue).Inc()
	}
}

// DecActiveConsumers decrements the active consumer count
func (s *PrometheusProvider) DecActiveConsumers(ctx context.Context, queue string) {
	if s.enabled {
		s.consumerCount.WithLabelValues(queue).Dec()
	}
}

// Legacy type alias for backward compatibility
// Deprecated: Use PrometheusProvider instead
type PrometheusService = PrometheusProvider

// NewPrometheusService creates a new Prometheus metrics service
// Deprecated: Use NewPrometheusProvider instead
func NewPrometheusService(logger zerolog.Logger, cfg PrometheusConfig) *PrometheusService {
	return NewPrometheusProvider(logger, cfg)
}
