// Package envelope provides message envelope functionality for SQS messages.
// It implements the same envelope format as the Laravel PHP version for compatibility.
package envelope

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

const (
	// Version is the envelope format version
	Version = "1.0"
)

// Envelope represents a standardized message wrapper
type Envelope struct {
	EventType      string         `json:"event_type"`
	Service        string         `json:"service"`
	Payload        map[string]any `json:"payload"`
	IdempotencyKey string         `json:"idempotency_key"`
	TraceID        string         `json:"trace_id"`
	Timestamp      string         `json:"timestamp"`
	Version        string         `json:"version"`
}

// temporaryFields are fields that should be excluded from idempotency key generation
var temporaryFields = []string{
	"timestamp",
	"created_at",
	"updated_at",
	"deleted_at",
	"trace_id",
	"request_id",
}

// Wrap creates a new envelope for the given event type and payload
func Wrap(eventType string, payload map[string]any, service string) *Envelope {
	// Generate idempotency key
	cleanPayload := removeTemporaryFields(payload)
	idempotencyKey := generateIdempotencyKey(eventType, cleanPayload)

	// Generate or extract trace ID
	traceID := extractTraceID(payload)
	if traceID == "" {
		traceID = uuid.New().String()
	}

	return &Envelope{
		EventType:      eventType,
		Service:        service,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
		TraceID:        traceID,
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		Version:        Version,
	}
}

// Unwrap extracts the payload from an envelope
func Unwrap(envelope *Envelope) map[string]any {
	if envelope == nil {
		return nil
	}
	return envelope.Payload
}

// UnwrapJSON parses a JSON string into an Envelope
func UnwrapJSON(data string) (*Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal([]byte(data), &envelope); err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}
	return &envelope, nil
}

// ToJSON serializes the envelope to JSON
func (e *Envelope) ToJSON() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("failed to marshal envelope: %w", err)
	}
	return string(data), nil
}

// Validate checks if the envelope has all required fields
func (e *Envelope) Validate() error {
	if e.EventType == "" {
		return errors.New("envelope missing event_type")
	}
	if e.Service == "" {
		return errors.New("envelope missing service")
	}
	if e.Payload == nil {
		return errors.New("envelope missing payload")
	}
	if e.IdempotencyKey == "" {
		return errors.New("envelope missing idempotency_key")
	}
	if e.TraceID == "" {
		return errors.New("envelope missing trace_id")
	}
	if e.Timestamp == "" {
		return errors.New("envelope missing timestamp")
	}
	if e.Version == "" {
		return errors.New("envelope missing version")
	}
	return nil
}

// GetEventType returns the event type from the envelope
func (e *Envelope) GetEventType() string {
	return e.EventType
}

// GetTraceID returns the trace ID from the envelope
func (e *Envelope) GetTraceID() string {
	return e.TraceID
}

// GetIdempotencyKey returns the idempotency key from the envelope
func (e *Envelope) GetIdempotencyKey() string {
	return e.IdempotencyKey
}

// removeTemporaryFields removes fields that shouldn't affect idempotency
func removeTemporaryFields(payload map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range payload {
		if !slices.Contains(temporaryFields, k) {
			// Recursively clean nested maps
			if nested, ok := v.(map[string]any); ok {
				result[k] = removeTemporaryFields(nested)
			} else {
				result[k] = v
			}
		}
	}
	return result
}

// generateIdempotencyKey creates a deterministic hash from event type and payload
func generateIdempotencyKey(eventType string, payload map[string]any) string {
	sortedPayload := sortMapRecursively(payload)
	payloadJSON, _ := json.Marshal(sortedPayload)

	data := eventType + "|" + string(payloadJSON)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// sortMapRecursively sorts map keys recursively for deterministic serialization
func sortMapRecursively(m map[string]any) map[string]any {
	result := make(map[string]any)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := m[k]
		if nested, ok := v.(map[string]any); ok {
			result[k] = sortMapRecursively(nested)
		} else if arr, ok := v.([]any); ok {
			result[k] = sortArrayRecursively(arr)
		} else {
			result[k] = v
		}
	}
	return result
}

// sortArrayRecursively sorts arrays containing maps
func sortArrayRecursively(arr []any) []any {
	result := make([]any, len(arr))
	for i, v := range arr {
		if m, ok := v.(map[string]any); ok {
			result[i] = sortMapRecursively(m)
		} else {
			result[i] = v
		}
	}
	return result
}

// extractTraceID extracts an existing trace ID from the payload if present
func extractTraceID(payload map[string]any) string {
	if traceID, ok := payload["trace_id"].(string); ok {
		return traceID
	}
	if traceID, ok := payload["traceId"].(string); ok {
		return traceID
	}
	return ""
}

// ParseEnvelopeFromMessage parses an envelope from a raw message body
// It handles both envelope-wrapped messages and raw payloads
func ParseEnvelopeFromMessage(body string) (*Envelope, error) {
	envelope, err := UnwrapJSON(body)
	if err != nil {
		return nil, err
	}

	if err := envelope.Validate(); err != nil {
		return nil, fmt.Errorf("invalid envelope: %w", err)
	}

	return envelope, nil
}
