package connections

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	monitor "exodus/internal/nodes"

	"github.com/google/uuid"
)

const geocheckJobTTL = 15 * time.Minute

type GeocheckImage struct {
	Format    string `json:"format"`
	MediaType string `json:"media_type"`
	Encoding  string `json:"encoding"`
	Data      string `json:"data"`
}

type GeocheckResult struct {
	Success   bool           `json:"success"`
	NodeUUID  string         `json:"nodeUuid"`
	Image     *GeocheckImage `json:"image"`
	RawReport map[string]any `json:"rawReport"`
	Message   *string        `json:"message"`
}

type GeocheckJob struct {
	mu          sync.RWMutex
	JobID       string
	NodeUUID    string
	IsCompleted bool
	IsFailed    bool
	Result      *GeocheckResult
	CreatedAt   time.Time
}

func (j *GeocheckJob) Snapshot() (isCompleted, isFailed bool, result *GeocheckResult) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.IsCompleted, j.IsFailed, j.Result
}

func (j *GeocheckJob) SetResult(isCompleted, isFailed bool, result *GeocheckResult) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.IsCompleted = isCompleted
	j.IsFailed = isFailed
	j.Result = result
}

var (
	jobsMu sync.RWMutex
	jobs   = make(map[string]*GeocheckJob)
)

func init() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		for range ticker.C {
			cleanupExpiredJobs(time.Now())
		}
	}()
}

func cleanupExpiredJobs(now time.Time) {
	jobsMu.Lock()
	defer jobsMu.Unlock()
	for id, job := range jobs {
		isCompleted, isFailed, _ := job.Snapshot()
		if (isCompleted || isFailed) && now.Sub(job.CreatedAt) > geocheckJobTTL {
			delete(jobs, id)
		} else if now.Sub(job.CreatedAt) > 2*geocheckJobTTL {
			delete(jobs, id)
		}
	}
}

func Handler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/connections")
		path = strings.Trim(path, "/")

		parts := strings.Split(path, "/")

		switch {
		case len(parts) >= 2 && (parts[0] == "geocheck" || parts[0] == "geocheck-by-node"):
			if r.Method == http.MethodPost {
				nodeUUID := parts[1]
				if _, err := uuid.Parse(nodeUUID); err != nil {
					shared.SendError(w, http.StatusBadRequest, "invalid node UUID", nil, cfg)
					return
				}

				var body struct {
					IP        string `json:"ip,omitempty"`
					Interface string `json:"interface,omitempty"`
				}
				if r.Body != nil {
					_ = json.NewDecoder(r.Body).Decode(&body)
				}

				jobID := uuid.NewString()
				job := &GeocheckJob{
					JobID:       jobID,
					NodeUUID:    nodeUUID,
					IsCompleted: false,
					IsFailed:    false,
					CreatedAt:   time.Now(),
				}

				jobsMu.Lock()
				jobs[jobID] = job
				jobsMu.Unlock()

				go runGeocheckJob(job, body.IP, body.Interface, cfg)

				shared.WriteJSON(w, http.StatusCreated, map[string]any{
					"response": map[string]any{
						"jobId": jobID,
					},
				})
				return
			} else if r.Method == http.MethodGet {
				jobID := parts[1]

				jobsMu.RLock()
				job, exists := jobs[jobID]
				jobsMu.RUnlock()

				if !exists {
					shared.WriteJSON(w, http.StatusOK, map[string]any{
						"response": map[string]any{
							"isCompleted": false,
							"isFailed":    false,
							"result":      nil,
						},
					})
					return
				}

				isCompleted, isFailed, result := job.Snapshot()
				shared.WriteJSON(w, http.StatusOK, map[string]any{
					"response": map[string]any{
						"isCompleted": isCompleted,
						"isFailed":    isFailed,
						"result":      result,
					},
				})
				return
			} else {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}

		case len(parts) >= 2 && parts[0] == "geocheck-by-node-result":
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			jobID := parts[1]

			jobsMu.RLock()
			job, exists := jobs[jobID]
			jobsMu.RUnlock()

			if !exists {
				shared.WriteJSON(w, http.StatusOK, map[string]any{
					"response": map[string]any{
						"isCompleted": false,
						"isFailed":    false,
						"result":      nil,
					},
				})
				return
			}

			isCompleted, isFailed, result := job.Snapshot()
			shared.WriteJSON(w, http.StatusOK, map[string]any{
				"response": map[string]any{
					"isCompleted": isCompleted,
					"isFailed":    isFailed,
					"result":      result,
				},
			})
			return

		case len(parts) >= 2 && (parts[0] == "by-user" || parts[0] == "users"):
			if r.Method == http.MethodPost {
				jobID := uuid.NewString()
				shared.WriteJSON(w, http.StatusCreated, map[string]any{
					"response": map[string]any{
						"jobId": jobID,
					},
				})
				return
			} else if r.Method == http.MethodGet {
				shared.WriteJSON(w, http.StatusOK, map[string]any{
					"response": map[string]any{
						"isCompleted": true,
						"isFailed":    false,
						"result": map[string]any{
							"connections": []any{},
						},
					},
				})
				return
			} else {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}

		case len(parts) >= 2 && parts[0] == "users-result":
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			shared.WriteJSON(w, http.StatusOK, map[string]any{
				"response": map[string]any{
					"isCompleted": true,
					"isFailed":    false,
					"result": map[string]any{
						"connections": []any{},
					},
				},
			})

		case len(parts) >= 2 && (parts[0] == "by-node" || parts[0] == "nodes"):
			if r.Method == http.MethodPost {
				jobID := uuid.NewString()
				shared.WriteJSON(w, http.StatusCreated, map[string]any{
					"response": map[string]any{
						"jobId": jobID,
					},
				})
				return
			} else if r.Method == http.MethodGet {
				shared.WriteJSON(w, http.StatusOK, map[string]any{
					"response": map[string]any{
						"isCompleted": true,
						"isFailed":    false,
						"result": map[string]any{
							"connections": []any{},
						},
					},
				})
				return
			} else {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}

		case len(parts) >= 2 && parts[0] == "nodes-result":
			if r.Method != http.MethodGet {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			shared.WriteJSON(w, http.StatusOK, map[string]any{
				"response": map[string]any{
					"isCompleted": true,
					"isFailed":    false,
					"result": map[string]any{
						"connections": []any{},
					},
				},
			})

		case path == "drop":
			if r.Method != http.MethodPost {
				shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			w.WriteHeader(http.StatusAccepted)

		default:
			http.NotFound(w, r)
		}
	}
}

func runGeocheckJob(job *GeocheckJob, ip string, iface string, cfg *config.BackendConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	outputJSON, err := monitor.RequestGeocheck(ctx, job.NodeUUID, ip, iface)

	if err != nil {
		msg := err.Error()
		job.SetResult(true, true, &GeocheckResult{
			Success:  false,
			NodeUUID: job.NodeUUID,
			Message:  &msg,
		})
		if cfg != nil && cfg.Logger != nil {
			cfg.Logger.Warn("Geocheck job failed", "job_id", job.JobID, "node_uuid", job.NodeUUID, "error", err)
		}
		return
	}

	var rawMap map[string]any
	if err := json.Unmarshal([]byte(outputJSON), &rawMap); err != nil {
		msg := "failed to parse geocheck JSON: " + err.Error()
		job.SetResult(true, true, &GeocheckResult{
			Success:  false,
			NodeUUID: job.NodeUUID,
			Message:  &msg,
		})
		return
	}

	var img *GeocheckImage
	if rawImg, ok := rawMap["image"].(map[string]any); ok {
		imgData, _ := rawImg["data"].(string)
		img = &GeocheckImage{
			Format:    "svg",
			MediaType: "image/svg+xml",
			Encoding:  "base64",
			Data:      imgData,
		}
		delete(rawMap, "image")
	}

	job.SetResult(true, false, &GeocheckResult{
		Success:   true,
		NodeUUID:  job.NodeUUID,
		Image:     img,
		RawReport: rawMap,
		Message:   nil,
	})
}
