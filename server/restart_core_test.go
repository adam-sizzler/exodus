package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFakeS6Svstat drops an executable named "s6-svstat" into a temp dir
// and prepends that dir to PATH, so s6Client.GetProcessInfo (which shells
// out via exec.LookPath) picks it up instead of a real s6-svstat binary.
func writeFakeS6Svstat(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "s6-svstat")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake s6-svstat: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestCoreIdentityFailureDetectsPidMismatch(t *testing.T) {
	writeFakeS6Svstat(t, "#!/bin/sh\necho 'up (pid 4242) 3 seconds, normally up'\n")
	s6 := &s6Client{servicePath: "/run/service/singbox"}

	// Same pid the start attempt recorded - the process is still the one we launched.
	if reason, broken := coreIdentityFailure(context.Background(), s6, coreProcessName, 4242); broken {
		t.Fatalf("expected no identity failure when pid matches, got reason=%q", reason)
	}

	// Different pid - s6 has since respawned it; a healthcheck success here
	// would not actually prove *this* start attempt's process is alive.
	reason, broken := coreIdentityFailure(context.Background(), s6, coreProcessName, 9999)
	if !broken {
		t.Fatalf("expected identity failure on pid mismatch")
	}
	if !strings.Contains(reason, "new pid") {
		t.Fatalf("unexpected reason: %q", reason)
	}
}

func TestCoreIdentityFailureDetectsStoppedProcess(t *testing.T) {
	writeFakeS6Svstat(t, "#!/bin/sh\necho 'down (exitcode 1) 2 seconds, normally up, want up'\nexit 1\n")
	s6 := &s6Client{servicePath: "/run/service/singbox"}

	reason, broken := coreIdentityFailure(context.Background(), s6, coreProcessName, 4242)
	if !broken {
		t.Fatalf("expected identity failure when supervisor reports the process down")
	}
	if !strings.Contains(reason, "STOPPED") {
		t.Fatalf("unexpected reason: %q", reason)
	}
}

func TestCoreIdentityFailureSkipsPidCheckWhenStartedPidUnknown(t *testing.T) {
	writeFakeS6Svstat(t, "#!/bin/sh\necho 'up (pid 4242) 3 seconds, normally up'\n")
	s6 := &s6Client{servicePath: "/run/service/singbox"}

	// startedPID <= 0 means we couldn't determine the pid right after start
	// (e.g. no s6-svstat at that moment) - only the up/down state should be
	// checked, matching upstream Exodus's `startedPid !== null` guard.
	if reason, broken := coreIdentityFailure(context.Background(), s6, coreProcessName, 0); broken {
		t.Fatalf("expected no identity failure when startedPID is unknown, got reason=%q", reason)
	}
}

func TestCoreHealthcheckIntervalForAttempt(t *testing.T) {
	tests := []struct {
		attempt  int
		expected string
	}{
		{1, "50ms"},
		{4, "50ms"},
		{5, "100ms"},
		{8, "100ms"},
		{9, "250ms"},
		{12, "250ms"},
		{13, "500ms"},
		{16, "500ms"},
		{17, "1s"},
		{20, "1s"},
		{21, "2s"},
		{25, "2s"},
	}

	for _, tc := range tests {
		got := coreHealthcheckIntervalForAttempt(tc.attempt).String()
		if got != tc.expected {
			t.Errorf("attempt %d: expected interval %s, got %s", tc.attempt, tc.expected, got)
		}
	}
}

