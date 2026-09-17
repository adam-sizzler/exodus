package seed

import (
	"context"
	"fmt"
	"strings"

	"exodus/internal/config"
	"exodus/internal/db"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func ensureDefaultTemplates(ctx context.Context, tx pgx.Tx, _ *config.BackendConfig) error {
	fmt.Println("◐ Seeding subscription templates...")

	defaults := db.DefaultSubscriptionTemplates()
	validTypes := make([]string, 0, len(defaults))
	for _, tmpl := range defaults {
		validTypes = append(validTypes, tmpl.TemplateType)
	}

	var deletedCount int64
	if len(validTypes) > 0 {
		placeholders := make([]string, len(validTypes))
		args := make([]any, len(validTypes))
		for i, t := range validTypes {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = t
		}
		query := fmt.Sprintf("DELETE FROM subscription_templates WHERE template_type NOT IN (%s)", strings.Join(placeholders, ", "))
		tag, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("delete obsolete subscription templates: %w", err)
		}
		deletedCount = tag.RowsAffected()
	}

	fmt.Printf("✔ Deleted unknown templates: %d\n", deletedCount)

	for _, tmpl := range defaults {
		var count int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM subscription_templates WHERE template_type = $1`, tmpl.TemplateType).Scan(&count); err != nil {
			return fmt.Errorf("count template %s: %w", tmpl.TemplateType, err)
		}
		if count > 0 {
			fmt.Printf("ℹ Default %s config already exists\n", tmpl.TemplateType)
			continue
		}

		query := `
			INSERT INTO subscription_templates (
				uuid, view_position, name, template_type, template_yaml, template_json
			) VALUES ($1, $2, $3, $4, $5, $6)
		`
		if _, err := tx.Exec(ctx, query,
			uuid.NewString(),
			tmpl.ViewPosition,
			tmpl.Name,
			tmpl.TemplateType,
			tmpl.TemplateYAML,
			tmpl.TemplateJSON,
		); err != nil {
			return fmt.Errorf("insert template %s: %w", tmpl.TemplateType, err)
		}
		fmt.Printf("✔ Default %s config seeded\n", tmpl.TemplateType)
	}

	return nil
}
