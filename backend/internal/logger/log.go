package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Level is kept for backward compatibility with older Exodus code.
type Level uint8

const (
	LevelPanic Level = iota
	LevelFatal
	LevelError
	LevelWarn
	LevelInfo
	LevelDebug
	LevelTrace
	LevelNone
)

const (
	RoleAPI       = "API"
	RoleScheduler = "Scheduler"
	RoleWorkers   = "Workers"

	ServiceBootstrap      = "Bootstrap"
	ServiceDatabase       = "Database"
	ServiceRedis          = "Redis"
	ServiceMetrics        = "Metrics"
	ServiceScheduler      = "Scheduler"
	ServiceQueues         = "Queues"
	ServiceHTTP           = "HTTP"
	ServiceGRPC           = "GRPC"
	ServiceAuth           = "Auth"
	ServiceHealthCheck    = "HealthCheck"
	ServiceJobs           = "Jobs"
	ServiceUsersQueue     = "UsersQueue"
	ServiceNodesQueue     = "NodesQueue"
	ServiceRuntimeMetrics = "RuntimeMetrics"
	ServiceConfig         = "Config"
	ServiceNotifications  = "Notifications"
)

const (
	FormatConsole = "console"
	FormatJSON    = "json"
)

const (
	ansiReset  = "\x1b[0m"
	ansiWhite  = "\x1b[37m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiRed    = "\x1b[31m"
	ansiCyan   = "\x1b[36m"
	ansiGray   = "\x1b[90m"
)

var levelMap = map[string]Level{
	"trace":   LevelTrace,
	"debug":   LevelDebug,
	"info":    LevelInfo,
	"warn":    LevelWarn,
	"warning": LevelWarn,
	"error":   LevelError,
	"fatal":   LevelFatal,
	"panic":   LevelPanic,
	"none":    LevelNone,
}

// Logger is the single backend logger. It preserves the old Exodus
// key/value logging API while using zerolog under the hood.
type Logger struct {
	base     zerolog.Logger
	writer   io.Writer
	level    zerolog.Level
	format   string
	timezone *time.Location
	role     string
	service  string
	mu       sync.Mutex
}

type BannerOptions struct {
	Title          string
	Version        string
	DocsURL        string
	CommunityURL   string
	HTTPPort       int
	PathPrefix     string
	DatabaseStatus string
	RedisStatus    string
	RescueCLI      string
}

// ParseLevel converts a string to the compatibility Level type.
func ParseLevel(level string) (Level, error) {
	if lvl, ok := levelMap[strings.ToLower(strings.TrimSpace(level))]; ok {
		return lvl, nil
	}
	return LevelNone, fmt.Errorf("unknown log level: %s", level)
}

func NewLogger(level, timezone string, writer io.Writer) (*Logger, error) {
	return newLogger(level, normalizeLogFormat(os.Getenv("LOG_FORMAT")), timezone, writer, RoleAPI, ServiceBootstrap)
}

func NewLoggerWithValidation(level, timezone string, writer io.Writer) (*Logger, error) {
	validatedLevel := normalizeLogLevel(level)
	validatedFormat := normalizeLogFormat(os.Getenv("LOG_FORMAT"))
	return newLogger(validatedLevel, validatedFormat, timezone, writer, RoleAPI, ServiceBootstrap)
}

func NewLoggerFromEnv(level, logFormat, timezone string, writer io.Writer) (*Logger, error) {
	return newLogger(normalizeLogLevel(level), normalizeLogFormat(logFormat), timezone, writer, RoleAPI, ServiceBootstrap)
}

func newLogger(level, logFormat, timezone string, writer io.Writer, role, service string) (*Logger, error) {
	if writer == nil {
		writer = os.Stderr
	}
	loc := time.UTC
	if timezone != "" {
		loaded, err := time.LoadLocation(timezone)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone: %w", err)
		}
		loc = loaded
	}

	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000Z07:00"
	zerolog.LevelFieldName = "level"
	zerolog.TimestampFieldName = "time"
	zerolog.MessageFieldName = "message"

	zlvl := parseZerologLevel(level)
	out := writer
	if logFormat == FormatConsole {
		out = &consoleJSONWriter{out: writer, timezone: loc, color: shouldColorizeLogs()}
	}

	base := zerolog.New(out).Level(zlvl).With().Timestamp().Logger()
	return &Logger{
		base:     base,
		writer:   writer,
		level:    zlvl,
		format:   logFormat,
		timezone: loc,
		role:     cleanContext(role, RoleAPI),
		service:  cleanContext(service, ServiceBootstrap),
	}, nil
}

func (l *Logger) WithRole(role string) *Logger {
	return l.withContext(role, "")
}

func (l *Logger) WithService(service string) *Logger {
	return l.withContext("", service)
}

func (l *Logger) RoleService(role, service string) *Logger {
	return l.withContext(role, service)
}

func (l *Logger) withContext(role, service string) *Logger {
	if l == nil {
		return nil
	}
	cleanR := cleanContext(role, l.role)
	cleanS := cleanContext(service, l.service)
	if cleanR == l.role && cleanS == l.service {
		return l
	}
	l.mu.Lock()
	clone := Logger{
		base:     l.base,
		writer:   l.writer,
		level:    l.level,
		format:   l.format,
		timezone: l.timezone,
		role:     cleanR,
		service:  cleanS,
	}
	l.mu.Unlock()
	return &clone
}

func (l *Logger) Trace(msg string, args ...any) { l.Log(LevelTrace, msg, args...) }
func (l *Logger) Debug(msg string, args ...any) { l.Log(LevelDebug, msg, args...) }
func (l *Logger) Info(msg string, args ...any)  { l.Log(LevelInfo, msg, args...) }
func (l *Logger) Warn(msg string, args ...any)  { l.Log(LevelWarn, msg, args...) }
func (l *Logger) Error(msg string, args ...any) { l.Log(LevelError, msg, args...) }
func (l *Logger) Fatal(msg string, args ...any) { l.Log(LevelFatal, msg, args...) }
func (l *Logger) Panic(msg string, args ...any) { l.Log(LevelPanic, msg, args...) }

func (l *Logger) Log(level Level, msg string, args ...any) {
	if l == nil || level == LevelNone {
		return
	}
	zlevel := compatibilityLevelToZerolog(level)
	if zlevel < l.level {
		return
	}

	role, service, fields := l.resolveContext(msg, args)
	event := l.base.WithLevel(zlevel)
	if event == nil {
		return
	}
	event = event.Str("role", role).Str("context", service)
	addFields(event, fields)
	event.Msg(msg)

	if level == LevelFatal {
		os.Exit(1)
	}
	if level == LevelPanic {
		panic(msg)
	}
}

func (l *Logger) IsDebugEnabled() bool {
	if l == nil {
		return false
	}
	return l.level <= zerolog.DebugLevel
}

func (l *Logger) IsTraceEnabled() bool {
	if l == nil {
		return false
	}
	return l.level <= zerolog.TraceLevel
}

func (l *Logger) PrintEnvironmentErrors(errors map[string]string) {
	if l == nil || len(errors) == 0 {
		return
	}
	l.RoleService(RoleAPI, ServiceConfig).Error("Environment validation failed")
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintln(l.writer, "🔧 Environment Configuration Errors")
	fmt.Fprintln(l.writer)
	for key, err := range errors {
		fmt.Fprintf(l.writer, "❌ %s\n%s\n\n", key, err)
	}
	fmt.Fprintln(l.writer, "Please fix configuration and restart application.")
}

func (l *Logger) PrintStartupBanner(opts BannerOptions) {
	if l == nil || strings.EqualFold(l.format, FormatJSON) {
		return
	}
	if strings.TrimSpace(opts.Title) == "" {
		opts.Title = "Exodus Backend"
	}
	if strings.TrimSpace(opts.Version) != "" {
		opts.Title = strings.TrimSpace(opts.Title) + " " + strings.TrimSpace(opts.Version)
	}
	if strings.TrimSpace(opts.DocsURL) == "" {
		opts.DocsURL = "https://docs.ex"
	}
	if strings.TrimSpace(opts.CommunityURL) == "" {
		opts.CommunityURL = "https://t.me/exodus"
	}
	if strings.TrimSpace(opts.PathPrefix) == "" {
		opts.PathPrefix = "/"
	}
	if strings.TrimSpace(opts.DatabaseStatus) == "" {
		opts.DatabaseStatus = "Unknown"
	}
	if strings.TrimSpace(opts.RedisStatus) == "" {
		opts.RedisStatus = "Disabled"
	}
	if strings.TrimSpace(opts.RescueCLI) == "" {
		opts.RescueCLI = "docker exec -it exodus cli"
	}

	lines := []string{
		"╭" + strings.Repeat("─", 78) + "╮",
		bannerLine(opts.Title),
		"├" + strings.Repeat("─", 78) + "┤",
		bannerLine("Docs ······ " + opts.DocsURL),
		bannerLine("Community ······ " + opts.CommunityURL),
		"│" + strings.Repeat("-", 78) + "│",
		bannerLine(fmt.Sprintf("HTTP Port ······ %d", opts.HTTPPort)),
		bannerLine("Path Prefix ······ " + opts.PathPrefix),
		"│" + strings.Repeat("-", 78) + "│",
		bannerLine("Database ······ " + opts.DatabaseStatus),
		bannerLine("Redis ······ " + opts.RedisStatus),
		"│" + strings.Repeat("-", 78) + "│",
		bannerLine("Rescue CLI ······ " + opts.RescueCLI),
		"╰" + strings.Repeat("─", 78) + "╯",
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range lines {
		fmt.Fprintln(l.writer, ansiGreen+line+ansiReset)
	}
}

func (l *Logger) resolveContext(msg string, args []any) (string, string, []any) {
	role := l.role
	service := l.service
	fields := make([]any, 0, len(args))

	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) {
			fields = append(fields, args[i])
			continue
		}
		key := fmt.Sprint(args[i])
		value := args[i+1]
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "role":
			role = cleanContext(fmt.Sprint(value), role)
		case "context", "service":
			service = cleanContext(fmt.Sprint(value), service)
		case "component":
			service = inferServiceFromComponent(fmt.Sprint(value), service)
			fields = append(fields, key, value)
		default:
			fields = append(fields, key, value)
		}
	}

	if service == "" || service == ServiceBootstrap {
		service = inferServiceFromMessage(msg, service)
	}
	return cleanContext(role, RoleAPI), cleanContext(service, ServiceBootstrap), fields
}

func addFields(event *zerolog.Event, args []any) {
	for i := 0; i < len(args); i += 2 {
		if i+1 >= len(args) {
			event.Interface(fmt.Sprintf("arg_%d", i), args[i])
			continue
		}
		key := fmt.Sprint(args[i])
		if strings.TrimSpace(key) == "" {
			continue
		}
		value := args[i+1]
		if err, ok := value.(error); ok {
			if strings.EqualFold(key, "error") || strings.EqualFold(key, "err") {
				event.Err(err)
			} else {
				event.Str(key, err.Error())
			}
			continue
		}
		event.Interface(key, value)
	}
}

func normalizeLogLevel(level string) string {
	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		level = "info"
	}
	if _, ok := levelMap[level]; ok {
		return level
	}
	return "info"
}

func normalizeLogFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case FormatJSON:
		return FormatJSON
	case "", FormatConsole:
		return FormatConsole
	default:
		return FormatConsole
	}
}

func parseZerologLevel(level string) zerolog.Level {
	switch normalizeLogLevel(level) {
	case "trace":
		return zerolog.TraceLevel
	case "debug":
		return zerolog.DebugLevel
	case "warn", "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	case "panic":
		return zerolog.PanicLevel
	case "none":
		return zerolog.Disabled
	default:
		return zerolog.InfoLevel
	}
}

func compatibilityLevelToZerolog(level Level) zerolog.Level {
	switch level {
	case LevelTrace:
		return zerolog.TraceLevel
	case LevelDebug:
		return zerolog.DebugLevel
	case LevelInfo:
		return zerolog.InfoLevel
	case LevelWarn:
		return zerolog.WarnLevel
	case LevelError:
		return zerolog.ErrorLevel
	case LevelFatal:
		return zerolog.FatalLevel
	case LevelPanic:
		return zerolog.PanicLevel
	case LevelNone:
		return zerolog.Disabled
	default:
		return zerolog.InfoLevel
	}
}

func cleanContext(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "<nil>") {
		return fallback
	}
	value = strings.Trim(value, "[]")
	if value == "" {
		return fallback
	}
	return value
}

func inferServiceFromComponent(component, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(component)) {
	case "web", "api", "http":
		return ServiceHTTP
	case "metrics", "prometheus":
		return ServiceMetrics
	case "scheduler":
		return ServiceScheduler
	case "workers", "worker", "jobs":
		return ServiceJobs
	default:
		return fallback
	}
}

func inferServiceFromMessage(msg, fallback string) string {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "database"), strings.Contains(lower, "migration"), strings.Contains(lower, "sql"), strings.Contains(lower, "postgres"):
		return ServiceDatabase
	case strings.Contains(lower, "redis"):
		return ServiceRedis
	case strings.Contains(lower, "metric"), strings.Contains(lower, "prometheus"):
		return ServiceMetrics
	case strings.Contains(lower, "scheduler"):
		return ServiceScheduler
	case strings.Contains(lower, "queue"):
		return ServiceQueues
	case strings.Contains(lower, "http"), strings.Contains(lower, "web server"):
		return ServiceHTTP
	case strings.Contains(lower, "grpc"):
		return ServiceGRPC
	case strings.Contains(lower, "auth"), strings.Contains(lower, "token"), strings.Contains(lower, "login"):
		return ServiceAuth
	case strings.Contains(lower, "health"):
		return ServiceHealthCheck
	case strings.Contains(lower, "job"):
		return ServiceJobs
	case strings.Contains(lower, "runtime"):
		return ServiceRuntimeMetrics
	case strings.Contains(lower, "config"), strings.Contains(lower, "environment"):
		return ServiceConfig
	case strings.Contains(lower, "notification"):
		return ServiceNotifications
	default:
		return fallback
	}
}

var consoleLogBufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 512))
	},
}

type consoleJSONWriter struct {
	out      io.Writer
	timezone *time.Location
	color    bool
	mu       sync.Mutex
}

func (w *consoleJSONWriter) Write(p []byte) (int, error) {
	line := bytes.TrimSpace(p)
	if len(line) == 0 {
		return len(p), nil
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(line, &rawFields); err != nil {
		_, writeErr := w.out.Write(p)
		return len(p), writeErr
	}

	timeVal := unquoteRawString(rawFields["time"])
	levelVal := unquoteRawString(rawFields["level"])
	msgVal := unquoteRawString(rawFields["message"])
	roleVal := cleanContext(unquoteRawString(rawFields["role"]), "")
	serviceVal := unquoteRawString(rawFields["context"])
	if serviceVal == "" {
		serviceVal = unquoteRawString(rawFields["service"])
	}
	serviceVal = cleanContext(serviceVal, "")

	delete(rawFields, "time")
	delete(rawFields, "level")
	delete(rawFields, "message")
	delete(rawFields, "role")
	delete(rawFields, "context")
	delete(rawFields, "service")

	cleanMsg := strings.Trim(msgVal, "\n")
	if isBoxMessage(cleanMsg) {
		if w.color {
			cleanMsg = colorizeMultiline(cleanMsg, ansiGreen)
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		_, err := fmt.Fprintln(w.out, cleanMsg)
		return len(p), err
	}

	buf := consoleLogBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer consoleLogBufPool.Put(buf)

	timestamp := formatConsoleTimestampStr(timeVal, w.timezone)
	buf.WriteString(colorize(timestamp, ansiWhite, w.color))
	buf.WriteByte(' ')

	levelUpper := strings.ToUpper(levelVal)
	if levelUpper == "" || levelUpper == "<NIL>" {
		levelUpper = "INFO"
	}
	buf.WriteString(colorize(levelUpper, levelColor(levelUpper), w.color))

	if roleVal != "" {
		buf.WriteByte(' ')
		buf.WriteString(colorize("["+roleVal+"]", ansiYellow, w.color))
	}
	if serviceVal != "" {
		buf.WriteByte(' ')
		buf.WriteString(colorize("["+serviceVal+"]", ansiYellow, w.color))
	}
	if cleanMsg != "" && cleanMsg != "<nil>" {
		buf.WriteByte(' ')
		buf.WriteString(cleanMsg)
	}

	if len(rawFields) > 0 {
		keys := make([]string, 0, len(rawFields))
		for k := range rawFields {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		extraBuf := consoleLogBufPool.Get().(*bytes.Buffer)
		extraBuf.Reset()
		for _, k := range keys {
			extraBuf.WriteByte(' ')
			extraBuf.WriteString(k)
			extraBuf.WriteByte('=')
			extraBuf.WriteString(formatRawJSONValue(rawFields[k]))
		}
		extraStr := extraBuf.String()
		consoleLogBufPool.Put(extraBuf)

		buf.WriteString(colorize(extraStr, ansiGray, w.color))
	}

	buf.WriteByte('\n')

	w.mu.Lock()
	_, err := w.out.Write(buf.Bytes())
	w.mu.Unlock()
	return len(p), err
}

func formatConsoleTimestampStr(raw string, loc *time.Location) string {
	if loc == nil {
		loc = time.UTC
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
			return parsed.In(loc).Format("2006-01-02 15:04:05.000")
		}
		return trimmed
	}
	return time.Now().In(loc).Format("2006-01-02 15:04:05.000")
}

func unquoteRawString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

func formatRawJSONValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	s := string(raw)
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		unquoted, err := strconv.Unquote(s)
		if err == nil {
			if strings.ContainsAny(unquoted, " \t\n\r") {
				return strconv.Quote(unquoted)
			}
			return unquoted
		}
	}
	return s
}

func isBoxMessage(message string) bool {
	trimmed := strings.TrimLeft(message, "\r\n ")
	return strings.HasPrefix(trimmed, "╭") || strings.HasPrefix(trimmed, "╔") || strings.HasPrefix(trimmed, "┌") || strings.HasPrefix(trimmed, "│")
}

func colorizeMultiline(value, color string) string {
	lines := strings.Split(value, "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		lines[i] = color + line + ansiReset
	}
	return strings.Join(lines, "\n")
}

func levelColor(level string) string {
	switch strings.ToUpper(level) {
	case "INFO":
		return ansiGreen
	case "WARN":
		return ansiYellow
	case "ERROR", "FATAL", "PANIC":
		return ansiRed
	case "DEBUG":
		return ansiCyan
	case "TRACE":
		return ansiGray
	default:
		return ansiWhite
	}
}

func shouldColorizeLogs() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if strings.EqualFold(os.Getenv("LOG_COLORS"), "false") {
		return false
	}
	return true
}

func colorize(value, color string, enabled bool) string {
	if !enabled || value == "" {
		return value
	}
	return color + value + ansiReset
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return ""
}

func bannerLine(content string) string {
	content = strings.TrimSpace(content)
	if len([]rune(content)) > 78 {
		content = trimRunes(content, 78)
	}
	padding := 78 - len([]rune(content))
	left := padding / 2
	right := padding - left
	return "│" + strings.Repeat(" ", left) + content + strings.Repeat(" ", right) + "│"
}

func trimRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	if limit <= 1 {
		return string(runes[:limit])
	}
	return string(runes[:limit-1]) + "…"
}
