package connections

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestGeocheckJobSnapshotThreadSafety(t *testing.T) {
	job := &GeocheckJob{
		JobID:     uuid.NewString(),
		NodeUUID:  uuid.NewString(),
		CreatedAt: time.Now(),
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if j%2 == 0 {
					job.SetResult(true, false, &GeocheckResult{
						Success:  true,
						NodeUUID: job.NodeUUID,
					})
				} else {
					job.Snapshot()
				}
			}
		}(i)
	}
	wg.Wait()

	completed, failed, result := job.Snapshot()
	if !completed {
		t.Errorf("expected job to be completed")
	}
	if failed {
		t.Errorf("expected job not to be failed")
	}
	if result == nil || !result.Success {
		t.Errorf("expected result to be successful")
	}
}

func TestCleanupExpiredJobs(t *testing.T) {
	jobsMu.Lock()
	jobs = make(map[string]*GeocheckJob)
	jobsMu.Unlock()

	now := time.Now()

	// 1. Fresh completed job (5m old) -> keep
	j1 := &GeocheckJob{JobID: "j1", CreatedAt: now.Add(-5 * time.Minute), IsCompleted: true}
	// 2. Expired completed job (20m old) -> delete
	j2 := &GeocheckJob{JobID: "j2", CreatedAt: now.Add(-20 * time.Minute), IsCompleted: true}
	// 3. Expired failed job (16m old) -> delete
	j3 := &GeocheckJob{JobID: "j3", CreatedAt: now.Add(-16 * time.Minute), IsFailed: true}
	// 4. In-progress job (10m old) -> keep
	j4 := &GeocheckJob{JobID: "j4", CreatedAt: now.Add(-10 * time.Minute), IsCompleted: false, IsFailed: false}
	// 5. Stale hung job (35m old) -> delete
	j5 := &GeocheckJob{JobID: "j5", CreatedAt: now.Add(-35 * time.Minute), IsCompleted: false, IsFailed: false}

	jobsMu.Lock()
	jobs["j1"] = j1
	jobs["j2"] = j2
	jobs["j3"] = j3
	jobs["j4"] = j4
	jobs["j5"] = j5
	jobsMu.Unlock()

	cleanupExpiredJobs(now)

	jobsMu.RLock()
	defer jobsMu.RUnlock()

	if _, exists := jobs["j1"]; !exists {
		t.Errorf("expected j1 (5m old) to be retained")
	}
	if _, exists := jobs["j2"]; exists {
		t.Errorf("expected j2 (20m completed) to be cleaned up")
	}
	if _, exists := jobs["j3"]; exists {
		t.Errorf("expected j3 (16m failed) to be cleaned up")
	}
	if _, exists := jobs["j4"]; !exists {
		t.Errorf("expected j4 (10m in-progress) to be retained")
	}
	if _, exists := jobs["j5"]; exists {
		t.Errorf("expected j5 (35m hung) to be cleaned up")
	}
}

func TestGeocheckHandlerGetStatus(t *testing.T) {
	jobsMu.Lock()
	jobs = make(map[string]*GeocheckJob)
	jobID := "test-job-123"
	job := &GeocheckJob{
		JobID:       jobID,
		NodeUUID:    uuid.NewString(),
		CreatedAt:   time.Now(),
		IsCompleted: true,
		Result: &GeocheckResult{
			Success:  true,
			NodeUUID: "node-1",
		},
	}
	jobs[jobID] = job
	jobsMu.Unlock()

	handler := Handler(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/connections/geocheck/"+jobID, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if body == "" {
		t.Fatalf("expected non-empty response body")
	}
}
