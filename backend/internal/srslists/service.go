package srslists

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
)

var fileNameAllowedRe = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

type Item struct {
	UUID           string
	Tags           []string
	Format         string
	URL            string
	UpdateInterval string
	Path           *string
	FileName       string
	ViewPosition   int
	IsEnabled      bool
	IsAvailable    bool
	LastCheckedAt  *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type NodeSyncItem struct {
	Tag            string `json:"tag"`
	Format         string `json:"format"`
	URL            string `json:"url"`
	UpdateInterval string `json:"update_interval"`
	Path           string `json:"path,omitempty"`
}

func DeriveFileNameFromURL(rawURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("url must start with http:// or https://")
	}
	name := path.Base(strings.TrimSpace(u.Path))
	if name == "" || name == "." || name == "/" {
		name = "ruleset"
	}
	name = strings.ToLower(strings.TrimSpace(name))
	name = fileNameAllowedRe.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-._")
	if name == "" {
		name = "ruleset"
	}
	if !strings.HasSuffix(name, ".srs") {
		name += ".srs"
	}
	return name, nil
}

func DeriveTagFromFileName(fileName string) string {
	fileName = strings.TrimSpace(fileName)
	if fileName == "" {
		return "ruleset"
	}
	tag := strings.TrimSuffix(fileName, path.Ext(fileName))
	tag = fileNameAllowedRe.ReplaceAllString(strings.ToLower(tag), "-")
	tag = strings.Trim(tag, "-._")
	if tag == "" {
		return "ruleset"
	}
	return tag
}

func LoadAll(ctx context.Context, dbConn db.DBTX) ([]Item, error) {
	if dbConn == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	rows, err := dbConn.Query(ctx, `
		SELECT uuid, tags, format, url, update_interval, path, file_name, view_position, is_enabled, is_available, last_checked_at, last_error, created_at, updated_at
		FROM srs_lists
		ORDER BY view_position ASC, created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Item, 0, 16)
	for rows.Next() {
		var item Item
		var tags []string
		if err := rows.Scan(
			&item.UUID,
			&tags,
			&item.Format,
			&item.URL,
			&item.UpdateInterval,
			&item.Path,
			&item.FileName,
			&item.ViewPosition,
			&item.IsEnabled,
			&item.IsAvailable,
			&item.LastCheckedAt,
			&item.LastError,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if tags == nil {
			tags = []string{}
		}
		item.Tags = tags
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}


func LoadNodeSyncItems(ctx context.Context, dbConn db.DBTX) ([]NodeSyncItem, error) {
	items, err := LoadAll(ctx, dbConn)
	if err != nil {
		return nil, err
	}
	result := make([]NodeSyncItem, 0, len(items))
	for _, item := range items {
		if !item.IsEnabled {
			continue
		}
		tag := DeriveTagFromFileName(item.FileName)
		pathValue := ""
		if item.Path != nil {
			pathValue = strings.TrimSpace(*item.Path)
		}
		if pathValue == "" {
			pathValue = item.FileName
		}
		result = append(result, NodeSyncItem{
			Tag:            tag,
			Format:         item.Format,
			URL:            item.URL,
			UpdateInterval: item.UpdateInterval,
			Path:           pathValue,
		})
	}
	return result, nil
}



var srsHTTPClient = &http.Client{
	Timeout: 25 * time.Second,
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          50,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

const srsCheckConcurrency = 5

type srsCheckResult struct {
	item Item
	err  error
}

func CheckOneURL(ctx context.Context, rawURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err == nil {
		resp, reqErr := srsHTTPClient.Do(req)
		if reqErr == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				return nil
			}
			if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotImplemented {
				return fmt.Errorf("status %d", resp.StatusCode)
			}
		}
	}

	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	getReq.Header.Set("Range", "bytes=0-1023")

	resp, err := srsHTTPClient.Do(getReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	buf := make([]byte, 1)
	n, readErr := resp.Body.Read(buf)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	if n == 0 {
		return fmt.Errorf("empty response body")
	}
	return nil
}

func CheckAndUpdateAvailability(ctx context.Context, dbConn db.DBTX, cfg *config.BackendConfig) (int, error) {
	items, err := LoadAll(ctx, dbConn)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}

	results := make([]srsCheckResult, len(items))
	sem := make(chan struct{}, srsCheckConcurrency)
	var wg sync.WaitGroup

	for i, item := range items {
		wg.Add(1)
		go func(idx int, it Item) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = srsCheckResult{item: it, err: ctx.Err()}
				return
			}

			results[idx] = srsCheckResult{item: it, err: CheckOneURL(ctx, it.URL)}
		}(i, item)
	}

	wg.Wait()

	updated := 0
	for _, res := range results {
		if ctx.Err() != nil {
			break
		}
		isAvailable := res.err == nil
		var errText any
		if res.err != nil {
			errText = res.err.Error()
		}

		_, writeErr := dbConn.Exec(ctx, `
			UPDATE srs_lists
			SET is_available = $1,
				last_checked_at = CURRENT_TIMESTAMP,
				last_error = $2,
				updated_at = CURRENT_TIMESTAMP
			WHERE uuid = $3
		`, isAvailable, errText, res.item.UUID)
		if writeErr != nil {
			if cfg != nil && cfg.Logger != nil {
				cfg.Logger.Warn("Failed to update SRS availability", "uuid", res.item.UUID, "error", writeErr)
			}
			continue
		}
		updated++
	}
	return updated, nil
}



