package messaging

import (
	"sync"

	"github.com/our-edu/go-sqs-messaging/internal/contracts"
)

// EventRegistry manages event type to handler mappings
type EventRegistry struct {
	handlers map[string]contracts.EventHandler
	mutex    sync.RWMutex
}

// NewEventRegistry creates a new event registry
func NewEventRegistry() *EventRegistry {
	return &EventRegistry{
		handlers: make(map[string]contracts.EventHandler),
	}
}

// Register registers a handler for an event type
func (r *EventRegistry) Register(eventType string, handler contracts.EventHandler) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.handlers[eventType] = handler
}

// GetHandler returns the handler for an event type
func (r *EventRegistry) GetHandler(eventType string) (contracts.EventHandler, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	handler, ok := r.handlers[eventType]
	return handler, ok
}

// ListEventTypes returns all registered event types
func (r *EventRegistry) ListEventTypes() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// Ensure interface compliance
var _ contracts.EventRegistry = (*EventRegistry)(nil)
