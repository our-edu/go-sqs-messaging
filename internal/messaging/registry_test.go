package messaging

import (
	"context"
	"sync"
	"testing"
)

func TestNewEventRegistry(t *testing.T) {
	registry := NewEventRegistry()

	if registry == nil {
		t.Fatal("expected non-nil registry")
	}
	if registry.handlers == nil {
		t.Error("expected handlers map to be initialized")
	}
}

func TestEventRegistry_Register(t *testing.T) {
	registry := NewEventRegistry()
	called := false
	handler := func(ctx context.Context, payload map[string]any) error {
		called = true
		return nil
	}

	registry.Register("OrderCreated", handler)

	h, ok := registry.GetHandler("OrderCreated")
	if !ok {
		t.Fatal("expected handler to be registered")
	}

	err := h(context.Background(), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected handler to be called")
	}
}

func TestEventRegistry_GetHandler_NotFound(t *testing.T) {
	registry := NewEventRegistry()

	_, ok := registry.GetHandler("NonExistent")
	if ok {
		t.Error("expected handler not to be found")
	}
}

func TestEventRegistry_Register_Overwrite(t *testing.T) {
	registry := NewEventRegistry()
	callCount := 0

	handler1 := func(ctx context.Context, payload map[string]any) error {
		callCount = 1
		return nil
	}
	handler2 := func(ctx context.Context, payload map[string]any) error {
		callCount = 2
		return nil
	}

	registry.Register("OrderCreated", handler1)
	registry.Register("OrderCreated", handler2)

	h, _ := registry.GetHandler("OrderCreated")
	h(context.Background(), nil)

	if callCount != 2 {
		t.Errorf("expected handler2 to be called (callCount=2), got %d", callCount)
	}
}

func TestEventRegistry_ListEventTypes(t *testing.T) {
	registry := NewEventRegistry()
	handler := func(ctx context.Context, payload map[string]any) error {
		return nil
	}

	registry.Register("OrderCreated", handler)
	registry.Register("UserCreated", handler)
	registry.Register("PaymentProcessed", handler)

	types := registry.ListEventTypes()

	if len(types) != 3 {
		t.Errorf("expected 3 event types, got %d", len(types))
	}

	// Check all types are present
	typeSet := make(map[string]bool)
	for _, et := range types {
		typeSet[et] = true
	}

	expectedTypes := []string{"OrderCreated", "UserCreated", "PaymentProcessed"}
	for _, et := range expectedTypes {
		if !typeSet[et] {
			t.Errorf("expected event type '%s' to be in list", et)
		}
	}
}

func TestEventRegistry_ListEventTypes_Empty(t *testing.T) {
	registry := NewEventRegistry()

	types := registry.ListEventTypes()

	if len(types) != 0 {
		t.Errorf("expected 0 event types, got %d", len(types))
	}
}

func TestEventRegistry_ConcurrentAccess(t *testing.T) {
	registry := NewEventRegistry()
	handler := func(ctx context.Context, payload map[string]any) error {
		return nil
	}

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			eventType := "Event" + string(rune('A'+i%26))
			registry.Register(eventType, handler)
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			eventType := "Event" + string(rune('A'+i%26))
			registry.GetHandler(eventType)
		}(i)
	}
	wg.Wait()

	// Concurrent list
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			registry.ListEventTypes()
		}()
	}
	wg.Wait()

	// If we get here without a race condition panic, the test passes
}

func TestEventRegistry_ConcurrentReadWrite(t *testing.T) {
	registry := NewEventRegistry()
	handler := func(ctx context.Context, payload map[string]any) error {
		return nil
	}

	var wg sync.WaitGroup

	// Start readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				registry.GetHandler("OrderCreated")
			}
		}()
	}

	// Start writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				registry.Register("OrderCreated", handler)
			}
		}()
	}

	wg.Wait()
}
