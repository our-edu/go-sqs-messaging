package envelope

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestWrap(t *testing.T) {
	payload := map[string]any{
		"user_id": "123",
		"email":   "test@example.com",
	}

	env := Wrap("UserCreated", payload, "user-service")

	if env.EventType != "UserCreated" {
		t.Errorf("expected EventType 'UserCreated', got '%s'", env.EventType)
	}
	if env.Service != "user-service" {
		t.Errorf("expected Service 'user-service', got '%s'", env.Service)
	}
	if env.Payload["user_id"] != "123" {
		t.Errorf("expected payload user_id '123', got '%v'", env.Payload["user_id"])
	}
	if env.IdempotencyKey == "" {
		t.Error("expected IdempotencyKey to be set")
	}
	if env.TraceID == "" {
		t.Error("expected TraceID to be set")
	}
	if env.Timestamp == "" {
		t.Error("expected Timestamp to be set")
	}
	if env.Version != Version {
		t.Errorf("expected Version '%s', got '%s'", Version, env.Version)
	}
}

func TestWrap_ExtractsTraceID(t *testing.T) {
	payload := map[string]any{
		"user_id":  "123",
		"trace_id": "existing-trace-id",
	}

	env := Wrap("UserCreated", payload, "user-service")

	if env.TraceID != "existing-trace-id" {
		t.Errorf("expected TraceID 'existing-trace-id', got '%s'", env.TraceID)
	}
}

func TestWrap_ExtractsTraceIdCamelCase(t *testing.T) {
	payload := map[string]any{
		"user_id": "123",
		"traceId": "existing-trace-id-camel",
	}

	env := Wrap("UserCreated", payload, "user-service")

	if env.TraceID != "existing-trace-id-camel" {
		t.Errorf("expected TraceID 'existing-trace-id-camel', got '%s'", env.TraceID)
	}
}

func TestWrap_IdempotencyExcludesTemporaryFields(t *testing.T) {
	payload1 := map[string]any{
		"user_id":    "123",
		"timestamp":  "2024-01-01T00:00:00Z",
		"created_at": "2024-01-01T00:00:00Z",
	}
	payload2 := map[string]any{
		"user_id":    "123",
		"timestamp":  "2024-01-02T00:00:00Z",
		"created_at": "2024-01-02T00:00:00Z",
	}

	env1 := Wrap("UserCreated", payload1, "user-service")
	env2 := Wrap("UserCreated", payload2, "user-service")

	// Same user_id should produce same idempotency key regardless of timestamps
	if env1.IdempotencyKey != env2.IdempotencyKey {
		t.Errorf("expected same idempotency keys, got '%s' and '%s'", env1.IdempotencyKey, env2.IdempotencyKey)
	}
}

func TestWrap_DifferentPayloadsProduceDifferentKeys(t *testing.T) {
	payload1 := map[string]any{"user_id": "123"}
	payload2 := map[string]any{"user_id": "456"}

	env1 := Wrap("UserCreated", payload1, "user-service")
	env2 := Wrap("UserCreated", payload2, "user-service")

	if env1.IdempotencyKey == env2.IdempotencyKey {
		t.Error("expected different idempotency keys for different payloads")
	}
}

func TestWrap_DifferentEventTypesProduceDifferentKeys(t *testing.T) {
	payload := map[string]any{"user_id": "123"}

	env1 := Wrap("UserCreated", payload, "user-service")
	env2 := Wrap("UserUpdated", payload, "user-service")

	if env1.IdempotencyKey == env2.IdempotencyKey {
		t.Error("expected different idempotency keys for different event types")
	}
}

func TestUnwrap(t *testing.T) {
	payload := map[string]any{"user_id": "123"}
	env := Wrap("UserCreated", payload, "user-service")

	result := Unwrap(env)

	if result["user_id"] != "123" {
		t.Errorf("expected user_id '123', got '%v'", result["user_id"])
	}
}

func TestUnwrap_NilEnvelope(t *testing.T) {
	result := Unwrap(nil)

	if result != nil {
		t.Error("expected nil result for nil envelope")
	}
}

func TestToJSON(t *testing.T) {
	payload := map[string]any{"user_id": "123"}
	env := Wrap("UserCreated", payload, "user-service")

	jsonStr, err := env.ToJSON()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(jsonStr, "UserCreated") {
		t.Error("expected JSON to contain 'UserCreated'")
	}
	if !strings.Contains(jsonStr, "user-service") {
		t.Error("expected JSON to contain 'user-service'")
	}
}

func TestUnwrapJSON(t *testing.T) {
	payload := map[string]any{"user_id": "123"}
	env := Wrap("UserCreated", payload, "user-service")
	jsonStr, _ := env.ToJSON()

	parsed, err := UnwrapJSON(jsonStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.EventType != env.EventType {
		t.Errorf("expected EventType '%s', got '%s'", env.EventType, parsed.EventType)
	}
	if parsed.Service != env.Service {
		t.Errorf("expected Service '%s', got '%s'", env.Service, parsed.Service)
	}
	if parsed.IdempotencyKey != env.IdempotencyKey {
		t.Errorf("expected IdempotencyKey '%s', got '%s'", env.IdempotencyKey, parsed.IdempotencyKey)
	}
}

func TestUnwrapJSON_InvalidJSON(t *testing.T) {
	_, err := UnwrapJSON("invalid json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		envelope    *Envelope
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid envelope",
			envelope:    Wrap("UserCreated", map[string]any{"id": "1"}, "test-service"),
			expectError: false,
		},
		{
			name: "missing event_type",
			envelope: &Envelope{
				Service:        "test",
				Payload:        map[string]any{},
				IdempotencyKey: "key",
				TraceID:        "trace",
				Timestamp:      "2024-01-01",
				Version:        "1.0",
			},
			expectError: true,
			errorMsg:    "event_type",
		},
		{
			name: "missing service",
			envelope: &Envelope{
				EventType:      "test",
				Payload:        map[string]any{},
				IdempotencyKey: "key",
				TraceID:        "trace",
				Timestamp:      "2024-01-01",
				Version:        "1.0",
			},
			expectError: true,
			errorMsg:    "service",
		},
		{
			name: "missing payload",
			envelope: &Envelope{
				EventType:      "test",
				Service:        "test",
				IdempotencyKey: "key",
				TraceID:        "trace",
				Timestamp:      "2024-01-01",
				Version:        "1.0",
			},
			expectError: true,
			errorMsg:    "payload",
		},
		{
			name: "missing idempotency_key",
			envelope: &Envelope{
				EventType: "test",
				Service:   "test",
				Payload:   map[string]any{},
				TraceID:   "trace",
				Timestamp: "2024-01-01",
				Version:   "1.0",
			},
			expectError: true,
			errorMsg:    "idempotency_key",
		},
		{
			name: "missing trace_id",
			envelope: &Envelope{
				EventType:      "test",
				Service:        "test",
				Payload:        map[string]any{},
				IdempotencyKey: "key",
				Timestamp:      "2024-01-01",
				Version:        "1.0",
			},
			expectError: true,
			errorMsg:    "trace_id",
		},
		{
			name: "missing timestamp",
			envelope: &Envelope{
				EventType:      "test",
				Service:        "test",
				Payload:        map[string]any{},
				IdempotencyKey: "key",
				TraceID:        "trace",
				Version:        "1.0",
			},
			expectError: true,
			errorMsg:    "timestamp",
		},
		{
			name: "missing version",
			envelope: &Envelope{
				EventType:      "test",
				Service:        "test",
				Payload:        map[string]any{},
				IdempotencyKey: "key",
				TraceID:        "trace",
				Timestamp:      "2024-01-01",
			},
			expectError: true,
			errorMsg:    "version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.envelope.Validate()
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got nil")
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestGetEventType(t *testing.T) {
	env := Wrap("UserCreated", map[string]any{}, "test")

	if env.GetEventType() != "UserCreated" {
		t.Errorf("expected 'UserCreated', got '%s'", env.GetEventType())
	}
}

func TestGetTraceID(t *testing.T) {
	env := Wrap("UserCreated", map[string]any{}, "test")

	if env.GetTraceID() == "" {
		t.Error("expected non-empty trace ID")
	}
}

func TestGetIdempotencyKey(t *testing.T) {
	env := Wrap("UserCreated", map[string]any{}, "test")

	if env.GetIdempotencyKey() == "" {
		t.Error("expected non-empty idempotency key")
	}
}

func TestParseEnvelopeFromMessage(t *testing.T) {
	payload := map[string]any{"user_id": "123"}
	env := Wrap("UserCreated", payload, "user-service")
	jsonStr, _ := env.ToJSON()

	parsed, err := ParseEnvelopeFromMessage(jsonStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.EventType != "UserCreated" {
		t.Errorf("expected EventType 'UserCreated', got '%s'", parsed.EventType)
	}
}

func TestParseEnvelopeFromMessage_InvalidJSON(t *testing.T) {
	_, err := ParseEnvelopeFromMessage("invalid json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseEnvelopeFromMessage_InvalidEnvelope(t *testing.T) {
	// Valid JSON but missing required fields
	invalidEnvelope := `{"event_type": "test"}`
	_, err := ParseEnvelopeFromMessage(invalidEnvelope)
	if err == nil {
		t.Error("expected error for invalid envelope")
	}
}

func TestRemoveTemporaryFields(t *testing.T) {
	payload := map[string]any{
		"user_id":    "123",
		"timestamp":  "2024-01-01",
		"created_at": "2024-01-01",
		"updated_at": "2024-01-01",
		"deleted_at": "2024-01-01",
		"trace_id":   "trace",
		"request_id": "request",
		"nested": map[string]any{
			"field":     "value",
			"timestamp": "should-be-removed",
		},
	}

	result := removeTemporaryFields(payload)

	if _, exists := result["timestamp"]; exists {
		t.Error("expected timestamp to be removed")
	}
	if _, exists := result["created_at"]; exists {
		t.Error("expected created_at to be removed")
	}
	if result["user_id"] != "123" {
		t.Error("expected user_id to be preserved")
	}

	nested := result["nested"].(map[string]any)
	if _, exists := nested["timestamp"]; exists {
		t.Error("expected nested timestamp to be removed")
	}
	if nested["field"] != "value" {
		t.Error("expected nested field to be preserved")
	}
}

func TestSortMapRecursively(t *testing.T) {
	input := map[string]any{
		"z": "last",
		"a": "first",
		"nested": map[string]any{
			"z": "nested-last",
			"a": "nested-first",
		},
	}

	result := sortMapRecursively(input)

	// Verify the result can be marshaled to consistent JSON
	json1, _ := json.Marshal(result)
	json2, _ := json.Marshal(result)

	if string(json1) != string(json2) {
		t.Error("expected deterministic JSON output")
	}
}

func TestIdempotencyKeyDeterminism(t *testing.T) {
	// Same payload should always produce the same idempotency key
	payload := map[string]any{
		"z_field": "z",
		"a_field": "a",
		"m_field": "m",
	}

	key1 := generateIdempotencyKey("TestEvent", payload)
	key2 := generateIdempotencyKey("TestEvent", payload)

	if key1 != key2 {
		t.Errorf("expected same idempotency keys, got '%s' and '%s'", key1, key2)
	}
}

func TestIdempotencyKeyWithNestedArrays(t *testing.T) {
	payload := map[string]any{
		"items": []any{
			map[string]any{"id": "1", "name": "item1"},
			map[string]any{"id": "2", "name": "item2"},
		},
	}

	key1 := generateIdempotencyKey("TestEvent", payload)
	key2 := generateIdempotencyKey("TestEvent", payload)

	if key1 != key2 {
		t.Errorf("expected same idempotency keys for nested arrays, got '%s' and '%s'", key1, key2)
	}
}
