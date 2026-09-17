package nodeintegrations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	errIntegrationNotFound = errors.New("node integration not found")
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) GetAll(ctx context.Context) ([]NodeIntegrationAPI, error) {
	rows, err := r.db.Query(ctx, `
		SELECT uuid, name, description, config, created_at, updated_at
		FROM integrations
		ORDER BY created_at ASC
	`)
	if err != nil {
		// If table does not exist yet before migration, return empty list gracefully
		if strings.Contains(err.Error(), "relation \"integrations\" does not exist") {
			return []NodeIntegrationAPI{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]NodeIntegrationAPI, 0)
	for rows.Next() {
		var item NodeIntegrationAPI
		var configBytes []byte
		if err := rows.Scan(&item.UUID, &item.Name, &item.Description, &configBytes, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.Config = json.RawMessage(configBytes)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) GetByUUID(ctx context.Context, uuid string) (*NodeIntegrationAPI, error) {
	row := r.db.QueryRow(ctx, `
		SELECT uuid, name, description, config, created_at, updated_at
		FROM integrations
		WHERE uuid = $1
	`, uuid)

	var item NodeIntegrationAPI
	var configBytes []byte
	if err := row.Scan(&item.UUID, &item.Name, &item.Description, &configBytes, &item.CreatedAt, &item.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errIntegrationNotFound
		}
		return nil, err
	}
	item.Config = json.RawMessage(configBytes)
	return &item, nil
}

func (r *Repository) Create(ctx context.Context, req CreateNodeIntegrationRequest) (*NodeIntegrationAPI, error) {
	var item NodeIntegrationAPI
	var configBytes []byte
	configJSON, _ := json.Marshal(req.Config)
	if len(req.Config) == 0 {
		configJSON = []byte("{}")
	}

	err := r.db.QueryRow(ctx, `
		INSERT INTO integrations (name, description, config, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
		RETURNING uuid, name, description, config, created_at, updated_at
	`, strings.TrimSpace(req.Name), req.Description, configJSON).Scan(
		&item.UUID, &item.Name, &item.Description, &configBytes, &item.CreatedAt, &item.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	item.Config = json.RawMessage(configBytes)
	return &item, nil
}

func (r *Repository) Update(ctx context.Context, req UpdateNodeIntegrationRequest) (*NodeIntegrationAPI, error) {
	existing, err := r.GetByUUID(ctx, req.UUID)
	if err != nil {
		return nil, err
	}

	name := existing.Name
	if req.Name != nil && strings.TrimSpace(*req.Name) != "" {
		name = strings.TrimSpace(*req.Name)
	}

	description := existing.Description
	if req.Description != nil {
		description = req.Description
	}

	configJSON := []byte(existing.Config)
	if req.Config != nil {
		configJSON, _ = json.Marshal(*req.Config)
	}

	var item NodeIntegrationAPI
	var configBytes []byte
	err = r.db.QueryRow(ctx, `
		UPDATE integrations
		SET name = $1, description = $2, config = $3, updated_at = NOW()
		WHERE uuid = $4
		RETURNING uuid, name, description, config, created_at, updated_at
	`, name, description, configJSON, req.UUID).Scan(
		&item.UUID, &item.Name, &item.Description, &configBytes, &item.CreatedAt, &item.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	item.Config = json.RawMessage(configBytes)
	return &item, nil
}

func (r *Repository) Delete(ctx context.Context, uuid string) error {
	res, err := r.db.Exec(ctx, `DELETE FROM integrations WHERE uuid = $1`, uuid)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return errIntegrationNotFound
	}
	return nil
}
