package passkeys

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	gowebauthn "github.com/go-webauthn/webauthn/webauthn"
)

func loadPasskeySettings(ctx context.Context, db *pgxpool.Pool) (passkeySettings, error) {
	var settings passkeySettings
	var rawConfig *string
	row := db.QueryRow(ctx, `
		SELECT passkey_settings::text
		FROM exodus_settings
		WHERE id = 1
		LIMIT 1
	`)
	if scanErr := row.Scan(&rawConfig); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return settings, nil
		}
		return settings, scanErr
	}
	if rawConfig == nil || strings.TrimSpace(*rawConfig) == "" {
		return settings, nil
	}
	err := json.Unmarshal([]byte(*rawConfig), &settings)
	return settings, err
}

func loadWebAuthnAdmin(ctx context.Context, db *pgxpool.Pool, adminUUID string) (*webAuthnAdmin, error) {
	admin := &webAuthnAdmin{}
	var row pgx.Row
	if strings.TrimSpace(adminUUID) == "" {
		row = db.QueryRow(ctx, `
			SELECT uuid, username
			FROM admin
			ORDER BY created_at ASC
			LIMIT 1
		`)
	} else {
		row = db.QueryRow(ctx, `
			SELECT uuid, username
			FROM admin
			WHERE uuid = $1
			LIMIT 1
		`, adminUUID)
	}

	if err := row.Scan(&admin.uuid, &admin.username); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errAdminNotFound
		}
		return nil, err
	}

	rows, err := db.Query(ctx, `
		SELECT id, public_key, counter, COALESCE(transports, ''), COALESCE(device_type, ''), backed_up
		FROM passkeys
		WHERE admin_uuid = $1
		ORDER BY created_at ASC
	`, admin.uuid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id        string
			publicKey []byte
			counter   int64
			transRaw  string
			device    string
			backedUp  bool
		)
		if err := rows.Scan(&id, &publicKey, &counter, &transRaw, &device, &backedUp); err != nil {
			return nil, err
		}

		rawID, err := decodeCredentialID(id)
		if err != nil {
			return nil, fmt.Errorf("decode credential id: %w", err)
		}
		backupEligible := strings.EqualFold(device, "multiDevice") || backedUp
		admin.credentials = append(admin.credentials, gowebauthn.Credential{
			ID:        rawID,
			PublicKey: publicKey,
			Transport: parseTransports(transRaw),
			Flags: gowebauthn.CredentialFlags{
				BackupEligible: backupEligible,
				BackupState:    backedUp,
			},
			Authenticator: gowebauthn.Authenticator{
				SignCount: uint32Counter(counter),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if strings.TrimSpace(admin.uuid) == "" {
		return nil, errAdminNotFound
	}
	return admin, nil
}

func saveNewCredential(ctx context.Context, db *pgxpool.Pool, adminUUID string, credential *gowebauthn.Credential) error {
	credentialID := encodeCredentialID(credential.ID)
	transports := transportsToCSV(credential.Transport)
	deviceType := "singleDevice"
	if credential.Flags.BackupEligible {
		deviceType = "multiDevice"
	}
	provider := passkeyProviderName(credential.Transport)

	_, err := db.Exec(ctx, `
		INSERT INTO passkeys (
			id, admin_uuid, public_key, counter, device_type, backed_up, transports, passkey_provider
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, credentialID, adminUUID, credential.PublicKey, int64(credential.Authenticator.SignCount), deviceType, credential.Flags.BackupState, transports, provider)
	return err
}

func updateCredentialUsage(ctx context.Context, db *pgxpool.Pool, adminUUID string, credential *gowebauthn.Credential) error {
	credentialID := encodeCredentialID(credential.ID)
	deviceType := "singleDevice"
	if credential.Flags.BackupEligible {
		deviceType = "multiDevice"
	}

	result, err := db.Exec(ctx, `
		UPDATE passkeys
		SET counter = $1, device_type = $2, backed_up = $3, updated_at = CURRENT_TIMESTAMP
		WHERE id = $4 AND admin_uuid = $5
	`, int64(credential.Authenticator.SignCount), deviceType, credential.Flags.BackupState, credentialID, adminUUID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errAdminNotFound
	}
	return nil
}

func listPasskeysForAdmin(ctx context.Context, db *pgxpool.Pool, adminUUID string) ([]passkeyRecord, error) {
	rows, err := db.Query(ctx, `
		SELECT id,
		       COALESCE(NULLIF(passkey_provider, ''), id) AS name,
		       created_at,
		       updated_at
		FROM passkeys
		WHERE admin_uuid = $1
		ORDER BY created_at DESC
	`, adminUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]passkeyRecord, 0)
	for rows.Next() {
		var item passkeyRecord
		if scanErr := rows.Scan(&item.ID, &item.Name, &item.CreatedAt, &item.LastUsedAt); scanErr != nil {
			return nil, scanErr
		}
		records = append(records, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}
