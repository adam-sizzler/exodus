package srslists

import (
	"context"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
	srscore "exodus/internal/srslists"
	"exodus/internal/util"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type srsListAPI struct {
	UUID           string     `json:"uuid"`
	Tags           []string   `json:"tags"`
	Format         string     `json:"format"`
	URL            string     `json:"url"`
	UpdateInterval string     `json:"updateInterval"`
	Path           *string    `json:"path"`
	FileName       string     `json:"fileName"`
	ShortName      string     `json:"shortName"`
	ViewPosition   int        `json:"viewPosition"`
	IsEnabled      bool       `json:"isEnabled"`
	IsAvailable    bool       `json:"isAvailable"`
	LastCheckedAt  *time.Time `json:"lastCheckedAt"`
	LastError      *string    `json:"lastError"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
}

type createSRSListsRequest struct {
	URL            string   `json:"url"`
	URLs           []string `json:"urls"`
	Tags           []string `json:"tags"`
	Format         string   `json:"format"`
	UpdateInterval string   `json:"updateInterval"`
	Path           string   `json:"path"`
	IsEnabled      *bool    `json:"isEnabled"`
}

type updateSRSListRequest struct {
	UUID           string   `json:"uuid"`
	URL            *string  `json:"url"`
	Tags           []string `json:"tags"`
	Format         *string  `json:"format"`
	UpdateInterval *string  `json:"updateInterval"`
	Path           *string  `json:"path"`
	IsEnabled      *bool    `json:"isEnabled"`
}

type reorderSRSListsRequest struct {
	Items []struct {
		UUID         string `json:"uuid"`
		ViewPosition int    `json:"viewPosition"`
	} `json:"items"`
}

type bulkDeleteRequest struct {
	UUIDs []string `json:"uuids"`
}

type bulkEnableRequest struct {
	UUIDs []string `json:"uuids"`
}

type bulkSetIntervalRequest struct {
	UUIDs          []string `json:"uuids"`
	UpdateInterval string   `json:"updateInterval"`
}

type checkListsRequest struct {
	UUIDs []string `json:"uuids"`
}

func getAllTags(ctx context.Context, db *pgxpool.Pool) ([]string, error) {
	rows, err := db.Query(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM srs_lists
		ORDER BY tag ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tags := make([]string, 0)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		trimmed := strings.TrimSpace(t)
		if trimmed != "" {
			tags = append(tags, trimmed)
		}
	}
	return tags, rows.Err()
}

func setTags(ctx context.Context, db *pgxpool.Pool, srsUUID string, tags []string) error {
	sanitized := shared.SanitizeTags(tags)
	result, err := db.Exec(ctx, `
		UPDATE srs_lists
		SET tags = $1::text[], updated_at = CURRENT_TIMESTAMP
		WHERE uuid::text = $2
	`, shared.PostgresTextArrayLiteral(sanitized), srsUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func checkSelectedLists(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig, uuids []string) error {
	clean, err := normalizeUUIDs(uuids)
	if err != nil {
		return err
	}
	if len(clean) == 0 {
		return nil
	}

	itemsByUUID := make(map[string]string, len(clean))
	rows, err := db.Query(ctx, `SELECT uuid, url FROM srs_lists WHERE uuid = ANY($1)`, clean)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, rawURL string
		if err := rows.Scan(&id, &rawURL); err != nil {
			return err
		}
		itemsByUUID[id] = rawURL
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for id, rawURL := range itemsByUUID {
		err := srscore.CheckOneURL(ctx, rawURL)
		isAvailable := err == nil
		var errText any
		if err != nil {
			errText = err.Error()
		}
		if _, writeErr := db.Exec(ctx, `
			UPDATE srs_lists
			SET is_available = $1,
				last_checked_at = CURRENT_TIMESTAMP,
				last_error = $2,
				updated_at = CURRENT_TIMESTAMP
			WHERE uuid = $3
		`, isAvailable, errText, id); writeErr != nil {
			if cfg != nil && cfg.Logger != nil {
				cfg.Logger.Warn("Failed to write selected srs check result", "uuid", id, "error", writeErr)
			}
		}
	}

	return nil
}

func normalizeUUIDs(values []string) ([]string, error) {
	return util.NormalizeAndValidateUUIDs(values)
}
