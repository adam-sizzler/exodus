package redisqueue

import (
	"context"
	"testing"
)

func TestParseNodeID(t *testing.T) {
	tests := []struct {
		key     string
		wantID  int64
		wantErr bool
	}{
		{key: "node_user_usage:42", wantID: 42, wantErr: false},
		{key: "node_user_usage:1", wantID: 1, wantErr: false},
		{key: "node_user_usage:invalid", wantID: 0, wantErr: true},
		{key: "invalid_key", wantID: 0, wantErr: true},
		{key: "", wantID: 0, wantErr: true},
	}

	for _, tt := range tests {
		got, err := parseNodeID(tt.key)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseNodeID(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
			continue
		}
		if got != tt.wantID {
			t.Errorf("parseNodeID(%q) = %v, want %v", tt.key, got, tt.wantID)
		}
	}
}

func TestHandleRecordUserUsageNilWorker(t *testing.T) {
	var w *Worker
	err := w.handleRecordUserUsage(context.Background(), "node_user_usage:1")
	if err != nil {
		t.Fatalf("expected nil error for nil worker, got %v", err)
	}
}
