package cmd

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"exodus/internal/security"

	"github.com/jackc/pgx/v5"
)

type rescueSecretPayload struct {
	NodeCertPem  string `json:"nodeCertPem"`
	NodeKeyPem   string `json:"nodeKeyPem"`
	CaCertPem    string `json:"caCertPem"`
	JWTPublicKey string `json:"jwtPublicKey"`
}

func resetCerts(resources *rescueResources, reader *bufio.Reader) error {
	answer, err := promptConfirm(
		reader,
		"Are you sure you want to delete the certs? You will need to add new certs to all nodes again.",
	)
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	printStatus("◐", "🔄 Deleting certs...")

	ctx := context.Background()

	var keygenUUID string

	err = resources.db.QueryRow(ctx, `
		SELECT uuid
		FROM keygen
		ORDER BY created_at ASC
		LIMIT 1
	`).Scan(&keygenUUID)

	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("certs not found")
	}
	if err != nil {
		return fmt.Errorf("find certs: %w", err)
	}

	if _, err := resources.db.Exec(ctx, "DELETE FROM keygen WHERE uuid = $1", keygenUUID); err != nil {
		return fmt.Errorf("delete certs: %w", err)
	}

	printStatus("✔", "✅ Certs deleted successfully.")
	fmt.Println(`⚠ Restart Exodus to apply changes by running "docker compose down && docker compose up -d".`)

	return nil
}

func getSecretKeyForNode(resources *rescueResources) error {
	printStatus("◐", "🔑 Getting SECRET_KEY for node...")

	ctx := context.Background()

	var (
		pubKey string
		caCert string
		caKey  string
	)

	err := resources.db.QueryRow(ctx, `
		SELECT pub_key, ca_cert, ca_key
		FROM keygen
		ORDER BY created_at ASC
		LIMIT 1
	`).Scan(&pubKey, &caCert, &caKey)

	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("keygen not found; reset certs first or restart Exodus")
	}
	if err != nil {
		return fmt.Errorf("fetch keygen data: %w", err)
	}

	if strings.TrimSpace(caCert) == "" || strings.TrimSpace(caKey) == "" {
		return fmt.Errorf("certs not found; reset certs first or restart Exodus")
	}

	nodeCert, err := security.GenerateNodeCert(caCert, caKey)
	if err != nil {
		return fmt.Errorf("generate node certificate: %w", err)
	}

	raw, err := json.Marshal(rescueSecretPayload{
		NodeCertPem:  nodeCert.NodeCertPEM,
		NodeKeyPem:   nodeCert.NodeKeyPEM,
		CaCertPem:    caCert,
		JWTPublicKey: pubKey,
	})
	if err != nil {
		return fmt.Errorf("encode secret payload: %w", err)
	}

	printStatus("✔", "✅ SECRET_KEY for node generated successfully.")
	fmt.Printf("\nSECRET_KEY=\"%s\"\n", base64.StdEncoding.EncodeToString(raw))

	return nil
}
