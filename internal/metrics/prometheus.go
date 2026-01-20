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

// PrometheusService handles exposing metrics to Prometheus
type PrometheusService struct {
	logger    zerolog.Logger
	namespace string
	subsystem string

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
	Namespace string // Metric namespace (e.g., "sqsmessaging")
	Subsystem string // Metric subsystem (e.g., "consumer")
}

// DefaultPrometheusConfig returns the default Prometheus configuration
func DefaultPrometheusConfig() PrometheusConfig {
	return PrometheusConfig{
		Namespace: "sqsmessaging",
		Subsystem: "",
	}
}

// NewPrometheusService creates a new Prometheus metrics service
func NewPrometheusService(logger zerolog.Logger, cfg PrometheusConfig) *PrometheusService {
	if cfg.Namespace == "" {
		cfg.Namespace = "sqsmessaging"
	}

	s := &PrometheusService{
		logger:    logger,
		namespace: cfg.Namespace,
		subsystem: cfg.Subsystem,
	}

	s.initMetrics()
	return s
}

func (s *PrometheusService) initMetrics() {
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

// Register registers all metrics with the default Prometheus registry
func (s *PrometheusService) Register() error {
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

	for _, c := range collectors {
		if err := prometheus.Register(c); err != nil {
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

// Handler returns an http.Handler for the /metrics endpoint.
// Use this with any HTTP framework that accepts http.Handler.
//
// Example with net/http:
//
//	http.Handle("/metrics", client.PrometheusHandler())
//
// Example with Gin:
//
//	router.GET("/metrics", gin.WrapH(client.PrometheusHandler()))
//
// Example with Echo:
//
//	e.GET("/metrics", echo.WrapHandler(client.PrometheusHandler()))
//
// Example with Chi:
//
//	r.Handle("/metrics", client.PrometheusHandler())
func (s *PrometheusService) Handler() http.Handler {
	return promhttp.Handler()
}

// HandlerFunc returns an http.HandlerFunc for the /metrics endpoint.
// Use this when you need a HandlerFunc instead of Handler.
//
// Example:
//
//	http.HandleFunc("/metrics", client.PrometheusHandlerFunc())
func (s *PrometheusService) HandlerFunc() http.HandlerFunc {
	return promhttp.Handler().ServeHTTP
}

// PutMetric implements the MetricsService interface
func (s *PrometheusService) PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error {
	queue := dimensions["queue"]
	eventType := dimensions["event_type"]

	switch name {
	case MetricMessagesProcessed:
		s.messagesProcessed.WithLabelValues(queue, eventType, "processed").Add(value)
	case MetricMessagesSuccess:
		s.messagesSuccess.WithLabelValues(queue, eventType).Add(value)
	case MetricValidationErrors:
		s.validationErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricTransientErrors:
		s.transientErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricPermanentErrors:
		s.permanentErrors.WithLabelValues(queue, eventType).Add(value)
	case MetricProcessingTime:
		s.processingDuration.WithLabelValues(queue, eventType).Observe(value)
	case MetricQueueDepth:
		s.queueDepth.WithLabelValues(queue).Set(value)
	case MetricDLQDepth:
		s.dlqDepth.WithLabelValues(queue).Set(value)
	}
	return nil
}

// Increment implements the MetricsService interface
func (s *PrometheusService) Increment(ctx context.Context, name string, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, 1.0, "Count", dimensions)
}

// RecordDuration implements the MetricsService interface
func (s *PrometheusService) RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error {
	return s.PutMetric(ctx, name, duration, "Milliseconds", dimensions)
}

// IncMessagesPublished increments the messages published counter
func (s *PrometheusService) IncMessagesPublished(queue, eventType string) {
	s.messagesPublished.WithLabelValues(queue, eventType).Inc()
}

// IncPublishErrors increments the publish errors counter
func (s *PrometheusService) IncPublishErrors(queue, eventType string) {
	s.publishErrors.WithLabelValues(queue, eventType).Inc()
}

// ObservePublishDuration records the publish duration
func (s *PrometheusService) ObservePublishDuration(queue, eventType string, durationMs float64) {
	s.publishDuration.WithLabelValues(queue, eventType).Observe(durationMs)
}

// SetQueueDepth sets the current queue depth
func (s *PrometheusService) SetQueueDepth(queue string, depth float64) {
	s.queueDepth.WithLabelValues(queue).Set(depth)
}

// SetDLQDepth sets the current DLQ depth
func (s *PrometheusService) SetDLQDepth(queue string, depth float64) {
	s.dlqDepth.WithLabelValues(queue).Set(depth)
}

// SetActiveConsumers sets the number of active consumers
func (s *PrometheusService) SetActiveConsumers(queue string, count float64) {
	s.consumerCount.WithLabelValues(queue).Set(count)
}

// IncActiveConsumers increments the active consumer count
func (s *PrometheusService) IncActiveConsumers(queue string) {
	s.consumerCount.WithLabelValues(queue).Inc()
}

// DecActiveConsumers decrements the active consumer count
func (s *PrometheusService) DecActiveConsumers(queue string) {
	s.consumerCount.WithLabelValues(queue).Dec()
}
