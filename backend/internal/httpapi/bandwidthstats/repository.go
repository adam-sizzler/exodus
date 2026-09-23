package bandwidthstats

import (
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exodus/internal/httpapi/shared"
)

var palette = []string{
	"#3b82f6", "#06b6d4", "#22c55e", "#eab308", "#f97316", "#ef4444", "#8b5cf6", "#ec4899",
}

type nodeRealtimeUsage struct {
	NodeUUID         string `json:"nodeUuid"`
	NodeName         string `json:"nodeName"`
	CountryCode      string `json:"countryCode"`
	DownloadBytes    int64  `json:"downloadBytes"`
	UploadBytes      int64  `json:"uploadBytes"`
	TotalBytes       int64  `json:"totalBytes"`
	DownloadSpeedBps int64  `json:"downloadSpeedBps"`
	UploadSpeedBps   int64  `json:"uploadSpeedBps"`
	TotalSpeedBps    int64  `json:"totalSpeedBps"`
}

type usageSeries struct {
	UUID        string  `json:"uuid"`
	Name        string  `json:"name"`
	Color       string  `json:"color"`
	CountryCode string  `json:"countryCode"`
	Total       int64   `json:"total"`
	Data        []int64 `json:"data"`
}

type topNode struct {
	UUID        string `json:"uuid"`
	Name        string `json:"name"`
	Color       string `json:"color"`
	CountryCode string `json:"countryCode"`
	Total       int64  `json:"total"`
}

type topUser struct {
	Color    string `json:"color"`
	Username string `json:"username"`
	Total    int64  `json:"total"`
}

type legacyNodeUserUsage struct {
	Date     string `json:"date"`
	NodeUUID string `json:"nodeUuid"`
	UserUUID string `json:"userUuid"`
	Username string `json:"username"`
	Total    int64  `json:"total"`
}

type legacyUserUsage struct {
	Date        string `json:"date"`
	UserUUID    string `json:"userUuid"`
	NodeUUID    string `json:"nodeUuid"`
	NodeName    string `json:"nodeName"`
	CountryCode string `json:"countryCode"`
	Total       int64  `json:"total"`
}

type getNodesUsersUsageRequest struct {
	NodesUUIDs []string `json:"nodesUuids"`
}

func parseDateRange(w http.ResponseWriter, r *http.Request) (time.Time, time.Time, []string, bool) {
	start := strings.TrimSpace(r.URL.Query().Get("start"))
	end := strings.TrimSpace(r.URL.Query().Get("end"))
	if start == "" || end == "" {
		shared.WriteJSONError(w, http.StatusBadRequest, "start and end are required")
		return time.Time{}, time.Time{}, nil, false
	}

	startDate, err := time.Parse("2006-01-02", start)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid start date")
		return time.Time{}, time.Time{}, nil, false
	}
	endDate, err := time.Parse("2006-01-02", end)
	if err != nil {
		shared.WriteJSONError(w, http.StatusBadRequest, "invalid end date")
		return time.Time{}, time.Time{}, nil, false
	}
	if endDate.Before(startDate) {
		shared.WriteJSONError(w, http.StatusBadRequest, "end date must be >= start date")
		return time.Time{}, time.Time{}, nil, false
	}

	startDate = startDate.UTC().Truncate(24 * time.Hour)
	endDate = endDate.UTC().Truncate(24 * time.Hour).Add(24*time.Hour - time.Nanosecond)
	dates := dateRange(startDate, endDate)
	return startDate, endDate, dates, true
}

func dateRange(start, end time.Time) []string {
	out := make([]string, 0, int(end.Sub(start)/(24*time.Hour))+1)
	cur := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	last := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
	for !cur.After(last) {
		out = append(out, cur.Format("2006-01-02"))
		cur = cur.Add(24 * time.Hour)
	}
	return out
}

func parsePositiveIntWithDefault(raw string, fallback int) int {
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return fallback
	}
	return v
}

func colorFromUUID(id string) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(id))
	return palette[hasher.Sum32()%uint32(len(palette))]
}
