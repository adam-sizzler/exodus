package server

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"exodus-node/api"
	"exodus-node/config"
)

const (
	coreProcessName         = "singbox"
	coreHealthcheckAttempts = 30
	coreMinTimeout          = 100 * time.Millisecond
	coreMaxTimeout          = 2 * time.Second
	coreBackoffFactor       = 1.5
)

// coreHealthcheckIntervalForAttempt computes the retry delay matching upstream p-retry
// (minTimeout: 100ms, factor: 1.5, maxTimeout: 2000ms, 30 retries).
func coreHealthcheckIntervalForAttempt(attempt int) time.Duration {
	if attempt <= 1 {
		return coreMinTimeout
	}
	delayFloat := float64(coreMinTimeout) * math.Pow(coreBackoffFactor, float64(attempt-1))
	delay := time.Duration(delayFloat)
	if delay > coreMaxTimeout {
		return coreMaxTimeout
	}
	return delay
}



type s6ProcessInfo struct {
	Name      string
	StateName string
	PID       int
}

type s6Client struct {
	servicePath string
	svstatBin   string
	svcBin      string
	logger      interface {
		Trace(string, ...any)
		Debug(string, ...any)
		Warn(string, ...any)
		Error(string, ...any)
	}
}

type coreLifecycleResult struct {
	ProcessBefore string
	ProcessAfter  string
	Started       bool
	Ready         bool
	Error         string
}

func (r coreLifecycleResult) failed() bool {
	return strings.TrimSpace(r.Error) != ""
}

func restartCoreProcessLifecycle(ctx context.Context, cfg *config.NodeConfig, apiService *api.Service) coreLifecycleResult {
	result := coreLifecycleResult{}
	if cfg == nil || cfg.Logger == nil {
		result.Error = "node config/logger is nil"
		return result
	}
	log := cfg.LoggerFor("SingboxService")
	if apiService != nil {
		apiService.MarkCoreOffline()
	}

	log.Debug("Starting managed core lifecycle (s6-overlay)", "process", coreProcessName, "config", config.FixedSingboxConfigPath)

	s6 := newS6Client(cfg)

	before, err := s6.GetProcessInfo(ctx, coreProcessName)
	if err != nil {
		result.Error = fmt.Sprintf("get s6 process info: %v", err)
		log.Error("Failed to start Sing-box: " + err.Error())
		return result
	}
	result.ProcessBefore = before.StateName
	log.Debug("Core service state before start", "state", before.StateName)

	if shouldStopBeforeStart(before.StateName) {
		log.Debug("Stopping existing core process before managed start", "state", before.StateName)
		if err := s6.StopProcess(ctx, coreProcessName, true); err != nil {
			log.Warn("Core process stop warning", "error", err)
		} else {
			log.Debug("Core process stopped")
		}
	}

	log.Debug("Starting core process through s6-svc", "process", coreProcessName)
	if err := s6.StartProcess(ctx, coreProcessName, true); err != nil {
		result.Error = fmt.Sprintf("start %s: %v", coreProcessName, err)
		log.Error("Failed to start Sing-box: " + err.Error())
		if apiService != nil {
			apiService.SetCoreError(result.Error)
		}
		return result
	}
	result.Started = true
	log.Debug("Core process start command accepted")

	after, err := s6.GetProcessInfo(ctx, coreProcessName)
	startedPID := 0
	if err == nil {
		result.ProcessAfter = after.StateName
		startedPID = after.PID
	} else {
		result.ProcessAfter = "UNKNOWN"
	}
	log.Debug("Core service state after start", "state", result.ProcessAfter, "pid", startedPID)

	// startedPID pins the healthcheck to the exact process this start attempt
	// launched. s6-svc -wu only confirms the OS process was forked/exec'd -
	// sing-box can still fatal-exit moments later (e.g. "address already in
	// use" on the v2ray_api listen port) well after s6 already reported it
	// up. Without pinning to a PID, a subsequent CheckCoreReady() success is
	// meaningless: it only proves *something* answers on that host:port, not
	// that it's the process we just started - it could be a stale/orphaned
	// core, or an unrelated process squatting the same port (the exact
	// failure mode reported: the node "connects" but to a foreign core).
	// Matches upstream Exodus node's XrayProcessDownError guard in
	// xray.service.ts#getXrayInternalStatus, which aborts the moment the
	// s6-tracked PID no longer matches the one recorded right after start.
	elapsed, err := waitForCoreAPIReady(ctx, cfg, apiService, s6, coreProcessName, startedPID)
	if err != nil {
		diagCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		diagMsg, _ := RunSingboxCheck(diagCtx, config.FixedSingboxConfigPath)
		cancel()

		// Refresh state after failure
		if afterInfo, s6Err := s6.GetProcessInfo(ctx, coreProcessName); s6Err == nil {
			result.ProcessAfter = afterInfo.StateName
		}

		logReason := ExtractSingboxLogReason(DefaultSingboxLogPath, 10)

		if strings.TrimSpace(diagMsg) != "" {
			result.Error = strings.TrimSpace(diagMsg)
		} else if strings.TrimSpace(logReason) != "" {
			result.Error = fmt.Sprintf("core process is %s · %s", result.ProcessAfter, strings.TrimSpace(logReason))
		} else {
			result.Error = fmt.Sprintf("core API healthcheck failed: %v", err)
		}

		if apiService != nil {
			apiService.MarkCoreOffline()
			apiService.SetCoreError(result.Error)
		}
		log.Error("Failed to start Sing-box: " + result.Error)
		return result
	}

	result.Ready = true
	if apiService != nil {
		apiService.MarkCoreOnline()
	}
	log.Log(fmt.Sprintf("✔ Sing-box Core v%s is up and running.", detectManagedCoreVersion()))
	log.Log(fmt.Sprintf("Attempt to start Sing-box took: %s", formatDuration(elapsed)))
	return result
}

func newS6Client(cfg *config.NodeConfig) *s6Client {
	servicePath := os.Getenv("S6_SERVICE_PATH")
	if servicePath == "" {
		servicePath = "/run/service/singbox"
	}
	var logger interface {
		Trace(string, ...any)
		Debug(string, ...any)
		Warn(string, ...any)
		Error(string, ...any)
	}
	if cfg != nil {
		logger = cfg.LoggerFor("SingboxService")
	}
	return &s6Client{
		servicePath: servicePath,
		svstatBin:   findExecutable("s6-svstat", "/command/s6-svstat", "/usr/bin/s6-svstat"),
		svcBin:      findExecutable("s6-svc", "/command/s6-svc", "/usr/bin/s6-svc"),
		logger:      logger,
	}
}

var pidRegexp = regexp.MustCompile(`\(pid (\d+)\)`)

func findExecutable(names ...string) string {
	for _, name := range names {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
		if _, err := os.Stat(name); err == nil {
			return name
		}
	}
	return ""
}

func (c *s6Client) GetProcessInfo(ctx context.Context, name string) (s6ProcessInfo, error) {
	svstatBin := c.svstatBin
	if svstatBin == "" {
		svstatBin = findExecutable("s6-svstat", "/command/s6-svstat", "/usr/bin/s6-svstat")
	}
	if svstatBin == "" {
		return s6ProcessInfo{Name: name, StateName: "STOPPED", PID: 0}, nil
	}

	cmd := exec.CommandContext(ctx, svstatBin, c.servicePath)
	out, err := cmd.CombinedOutput()
	outputStr := strings.TrimSpace(string(out))
	if err != nil {
		return s6ProcessInfo{Name: name, StateName: "STOPPED", PID: 0}, nil
	}

	state := "STOPPED"
	pid := 0
	if strings.HasPrefix(outputStr, "up") || strings.Contains(outputStr, "(pid") {
		state = "RUNNING"
		matches := pidRegexp.FindStringSubmatch(outputStr)
		if len(matches) == 2 {
			pid, _ = strconv.Atoi(matches[1])
		}
	}

	return s6ProcessInfo{
		Name:      name,
		StateName: state,
		PID:       pid,
	}, nil
}

func (c *s6Client) StartProcess(ctx context.Context, name string, wait bool) error {
	svcBin := c.svcBin
	if svcBin == "" {
		svcBin = findExecutable("s6-svc", "/command/s6-svc", "/usr/bin/s6-svc")
	}
	if svcBin == "" {
		if c.logger != nil {
			c.logger.Debug("s6-svc binary not found; s6 mock start active")
		}
		return nil
	}

	args := []string{"-wu", "-T", "10000", "-u", c.servicePath}
	cmd := exec.CommandContext(ctx, svcBin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := strings.TrimSpace(string(out))
		return fmt.Errorf("s6-svc start failed (%w): %s", err, outputStr)
	}
	return nil
}

func (c *s6Client) StopProcess(ctx context.Context, name string, wait bool) error {
	svcBin := c.svcBin
	if svcBin == "" {
		svcBin = findExecutable("s6-svc", "/command/s6-svc", "/usr/bin/s6-svc")
	}
	if svcBin == "" {
		if c.logger != nil {
			c.logger.Debug("s6-svc binary not found; s6 mock stop active")
		}
		return nil
	}

	args := []string{"-wD", "-T", "10000", "-d", c.servicePath}
	cmd := exec.CommandContext(ctx, svcBin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := strings.TrimSpace(string(out))
		return fmt.Errorf("s6-svc stop failed (%w): %s", err, outputStr)
	}
	return nil
}

func shouldStopBeforeStart(stateName string) bool {
	switch strings.ToUpper(strings.TrimSpace(stateName)) {
	case "RUNNING", "STARTING", "UP":
		return true
	default:
		return false
	}
}

// coreIdentityFailure inspects the s6-supervised core process and reports
// whether it can no longer vouch for startedPID: either the supervisor
// reports it stopped/dead outright, or it's "up" under a *different* PID
// than the one this start attempt launched (crashed and got respawned by
// s6 since). startedPID <= 0 means the PID right after start couldn't be
// determined (e.g. no s6-svstat available) - in that case only the state
// is checked, matching upstream Exodus's `startedPid !== null` guard.
func coreIdentityFailure(ctx context.Context, s6 *s6Client, coreProcessName string, startedPID int) (string, bool) {
	if s6 == nil || coreProcessName == "" {
		return "", false
	}
	pInfo, err := s6.GetProcessInfo(ctx, coreProcessName)
	if err != nil {
		return "", false
	}
	state := strings.ToUpper(strings.TrimSpace(pInfo.StateName))
	if state == "STOPPED" || state == "DOWN" || state == "DEAD" || state == "FATAL" {
		return fmt.Sprintf("core process is %s", pInfo.StateName), true
	}
	if startedPID > 0 && pInfo.PID > 0 && pInfo.PID != startedPID {
		return fmt.Sprintf("core process restarted under a new pid (was %d, now %d) - a healthcheck response can no longer be trusted as coming from this start attempt", startedPID, pInfo.PID), true
	}
	return "", false
}

func waitForCoreAPIReady(ctx context.Context, cfg *config.NodeConfig, apiService *api.Service, s6 *s6Client, coreProcessName string, startedPID int) (time.Duration, error) {
	log := cfg.LoggerFor("SingboxService")
	if apiService == nil {
		return 0, fmt.Errorf("core API service is nil")
	}

	startTime := time.Now()

	var lastErr error
	for attempt := 1; attempt <= coreHealthcheckAttempts; attempt++ {
		// Quick check if core already died/stopped, or was respawned under a
		// different pid, according to the supervisor
		if reason, broken := coreIdentityFailure(ctx, s6, coreProcessName, startedPID); broken {
			log.Debug("Core process identity broken, fast failing healthcheck", "reason", reason, "attempt", attempt)
			return time.Since(startTime), fmt.Errorf("%s", reason)
		}

		checkTimeout := 500 * time.Millisecond
		if attempt > 8 {
			checkTimeout = 2 * time.Second
		}
		checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
		err := apiService.CheckCoreReady(checkCtx)
		cancel()

		elapsed := time.Since(startTime)

		if err == nil {
			// A successful API response alone doesn't prove it came from
			// *our* process - re-verify identity right now before trusting it,
			// since the process could have crashed and something else could
			// be answering on the same host:port in the gap since the last check.
			if reason, broken := coreIdentityFailure(ctx, s6, coreProcessName, startedPID); broken {
				log.Debug("Core API answered but process identity is broken, not trusting it", "reason", reason, "attempt", attempt)
				return elapsed, fmt.Errorf("%s", reason)
			}
			log.Debug("Core API healthcheck passed", "attempt", attempt, "elapsed", elapsed)
			return elapsed, nil
		}
		lastErr = err

		interval := coreHealthcheckIntervalForAttempt(attempt)

		// Warn with formatted retry message matching upstream
		log.Warn(fmt.Sprintf("▸ Sing-box Core status check, %d/%d · elapsed %s · retrying in %s",
			attempt, coreHealthcheckAttempts, formatDuration(elapsed), formatDuration(interval)))

		// After failed attempt, verify if process stopped or was respawned
		// under a different pid before waiting
		if reason, broken := coreIdentityFailure(ctx, s6, coreProcessName, startedPID); broken {
			log.Debug("Core process identity broken after failed attempt, fast failing healthcheck", "reason", reason)
			return elapsed, fmt.Errorf("%s", reason)
		}

		if attempt == coreHealthcheckAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return elapsed, ctx.Err()
		case <-time.After(interval):
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("unknown core API healthcheck error")
	}
	return time.Since(startTime), lastErr
}

// formatDuration formats a duration as "Xs Yms" or just "Yms" for short durations.
func formatDuration(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	sec := ms / 1000
	remainMs := ms % 1000
	if remainMs == 0 {
		return fmt.Sprintf("%ds", sec)
	}
	return fmt.Sprintf("%ds %dms", sec, remainMs)
}
