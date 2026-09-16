package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/jobqueue"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type CLIFlags struct {
	Rescue bool
}

type rescueResources struct {
	db    *pgxpool.Pool
	redis *redis.Client
	cfg   *config.BackendConfig
}

type cliAction struct {
	Value string
	Label string
	Hint  string
}

func (a cliAction) String() string {
	return a.Label
}

func ParseCLIFlags() CLIFlags {
	var flags CLIFlags

	exeName := strings.ToLower(filepath.Base(os.Args[0]))
	if exeName == "cli" || exeName == "rescue" {
		flags.Rescue = true
	}

	for _, arg := range os.Args[1:] {
		switch strings.ToLower(strings.TrimSpace(arg)) {
		case "cli", "rescue", "--rescue", "-rescue":
			flags.Rescue = true
		}
	}

	return flags
}

func RunPreConfigCLI(flags CLIFlags) bool {
	if !flags.Rescue {
		return false
	}

	if err := runRescueCLI(); err != nil {
		fmt.Printf("❌ Rescue CLI failed: %v\n", err)
		os.Exit(1)
	}

	return true
}

func RunConfiguredCLI(_ CLIFlags, _ *config.BackendConfig) bool {
	return false
}

func runRescueCLI() error {
	reader := bufio.NewReader(os.Stdin)

	printCLIBox("Exodus Rescue CLI v0.4")

	printStatus("◐", "🌱 Checking database connection...")
	resources, err := openRescueResources()
	if err != nil {
		printStatus("✖", "❌ Failed to connect to database.")
		return err
	}
	defer resources.close()
	printStatus("✔", "✅ Database connected!")

	printStatus("◐", "🌱 Checking Redis connection...")
	if err := checkRescueRedis(resources); err != nil {
		printStatus("✖", "❌ Failed to connect to Redis.")
		return err
	}
	printStatus("✔", "✅ Redis connected!")

	action, err := promptAction()
	if err != nil {
		return err
	}

	switch action {
	case "reset-superadmin":
		return resetSuperadmin(resources, reader)
	case "enable-password-auth":
		return enablePasswordAuth(resources, reader)
	case "generate-encryption-keys":
		return generateEncryptionKeys(resources, reader)
	case "reset-certs":
		return resetCerts(resources, reader)
	case "get-secret-key-for-node":
		return getSecretKeyForNode(resources)
	case "truncate-hwid-user-devices":
		return truncateHwidUserDevices(resources, reader)
	case "truncate-srh-table":
		return truncateSRHTable(resources, reader)
	case "truncate-users-usage-table":
		return truncateUsersUsageTable(resources, reader)
	case "delete-users-usage-by-date-range":
		return deleteUsersUsageByDateRange(resources, reader)
	case "exit":
		printStatus("ℹ", "👋 Exiting...")
		return nil
	default:
		return fmt.Errorf("unknown action: %s", action)
	}
}

func openRescueResources() (*rescueResources, error) {
	var cfg config.BackendConfig
	var pgxPool *pgxpool.Pool

	err := runSilently(func() error {
		loadedCfg, err := config.LoadConfig()
		if err != nil {
			return fmt.Errorf("load configuration: %w", err)
		}

		cfg = loadedCfg

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		pool, err := db.OpenPgxPool(ctx, cfg.Database.URL, 4, 1)
		if err != nil {
			return fmt.Errorf("connect database: %w", err)
		}

		pgxPool = pool
		return nil
	})
	if err != nil {
		if pgxPool != nil {
			pgxPool.Close()
		}

		return nil, err
	}

	var redisClient *redis.Client

	err = runSilently(func() error {
		client, err := jobqueue.NewRedisClient(&cfg)
		if err != nil {
			return err
		}

		if client == nil {
			return fmt.Errorf("redis is not configured")
		}

		redisClient = client

		return nil
	})
	if err != nil {
		pgxPool.Close()
		return nil, err
	}

	return &rescueResources{
		db:    pgxPool,
		redis: redisClient,
		cfg:   &cfg,
	}, nil
}

func checkRescueRedis(resources *rescueResources) error {
	if resources == nil || resources.redis == nil {
		return fmt.Errorf("redis is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := resources.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}

	return nil
}

func (r *rescueResources) close() {
	if r == nil {
		return
	}

	if r.redis != nil {
		_ = r.redis.Close()
	}

	if r.db != nil {
		r.db.Close()
	}
}

func rescueActions() []cliAction {
	return []cliAction{
		{
			Value: "reset-superadmin",
			Label: "Reset superadmin",
			Hint:  "Fully reset superadmin",
		},
		{
			Value: "enable-password-auth",
			Label: "Enable password authentication",
			Hint:  "Enable password authentication",
		},
		{
			Value: "generate-encryption-keys",
			Label: "Generate keypairs",
			Hint:  "Generate keypairs for response rules encryption",
		},
		{
			Value: "reset-certs",
			Label: "Reset certs",
			Hint:  "Fully reset certs",
		},
		{
			Value: "get-secret-key-for-node",
			Label: "Get SECRET_KEY for an Exodus Node",
			Hint:  "Get SECRET_KEY in cases, where you can not get from Panel",
		},
		{
			Value: "truncate-hwid-user-devices",
			Label: "Clean up HWID Devices",
			Hint:  "Remove all HWID Devices from the database",
		},
		{
			Value: "truncate-srh-table",
			Label: "Clean up SRH Table",
			Hint:  "Remove all SRH data from the database",
		},
		{
			Value: "truncate-users-usage-table",
			Label: "Clean up Users Usage Table",
			Hint:  "Remove all users traffic statistics data from the database",
		},
		{
			Value: "delete-users-usage-by-date-range",
			Label: "Delete Users Usage by date range",
			Hint:  "Remove traffic statistics for a period (day-month-year); choose single or batched",
		},
		{
			Value: "exit",
			Label: "Exit",
		},
	}
}

func runSilently(fn func() error) error {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return runWithLogSilenced(fn)
	}
	defer devNull.Close()

	stdoutFD := int(os.Stdout.Fd())
	stderrFD := int(os.Stderr.Fd())

	savedStdout, err := syscall.Dup(stdoutFD)
	if err != nil {
		return runWithLogSilenced(fn)
	}
	defer syscall.Close(savedStdout)

	savedStderr, err := syscall.Dup(stderrFD)
	if err != nil {
		return runWithLogSilenced(fn)
	}
	defer syscall.Close(savedStderr)

	oldLogWriter := log.Writer()
	log.SetOutput(io.Discard)

	_ = syscall.Dup2(int(devNull.Fd()), stdoutFD)
	_ = syscall.Dup2(int(devNull.Fd()), stderrFD)

	fnErr := fn()

	_ = syscall.Dup2(savedStdout, stdoutFD)
	_ = syscall.Dup2(savedStderr, stderrFD)

	log.SetOutput(oldLogWriter)

	return fnErr
}

func runWithLogSilenced(fn func() error) error {
	oldLogWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldLogWriter)

	return fn()
}
