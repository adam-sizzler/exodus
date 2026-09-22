package shared

import (
	"bytes"
	"encoding/json"
)

// IsJSONNull checks if raw JSON data is either empty or the literal "null"
// without heap-allocating a string conversion.
func IsJSONNull(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return len(trimmed) == 0 || (len(trimmed) == 4 && trimmed[0] == 'n' && trimmed[1] == 'u' && trimmed[2] == 'l' && trimmed[3] == 'l')
}

type OptionalString struct {
	Set   bool
	Value *string
}

func (o *OptionalString) UnmarshalJSON(data []byte) error {
	o.Set = true
	if IsJSONNull(data) {
		o.Value = nil
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

type OptionalInt struct {
	Set   bool
	Value *int
}

func (o *OptionalInt) UnmarshalJSON(data []byte) error {
	o.Set = true
	if IsJSONNull(data) {
		o.Value = nil
		return nil
	}
	var value int
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

type OptionalInt64 struct {
	Set   bool
	Value *int64
}

func (o *OptionalInt64) UnmarshalJSON(data []byte) error {
	o.Set = true
	if IsJSONNull(data) {
		o.Value = nil
		return nil
	}
	var value int64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

type OptionalJSON struct {
	Set bool
	Raw json.RawMessage
}

func (o *OptionalJSON) UnmarshalJSON(data []byte) error {
	o.Set = true
	o.Raw = append(o.Raw[:0], data...)
	return nil
}
