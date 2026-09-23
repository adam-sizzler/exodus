package middleware

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/logger"
)

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	bytes      int
}

func (lrw *loggingResponseWriter) WriteHeader(statusCode int) {
	lrw.statusCode = statusCode
	lrw.ResponseWriter.WriteHeader(statusCode)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	if lrw.statusCode == 0 {
		lrw.statusCode = http.StatusOK
	}
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytes += n
	return n, err
}

func (lrw *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := lrw.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
}

func (lrw *loggingResponseWriter) Flush() {
	if flusher, ok := lrw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (lrw *loggingResponseWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := lrw.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}
	return http.ErrNotSupported
}

func WithRequestLogging(cfg *config.BackendConfig, component string, next http.Handler) http.Handler {
	if cfg == nil || cfg.Logger == nil {
		return next
	}

	apiLogger := cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP)
	schedulerLogger := cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceHTTP)
	isMetrics := strings.EqualFold(component, "metrics")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &loggingResponseWriter{ResponseWriter: w}
		next.ServeHTTP(lrw, r)

		statusCode := lrw.statusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}

		duration := time.Since(start)
		durationMs := duration.Milliseconds()
		if durationMs == 0 && duration > 0 {
			durationMs = 1
		}

		if shouldSkipAccessLog(cfg, r.URL.Path) {
			return
		}

		serviceLogger := apiLogger
		comp := component
		if isMetrics {
			serviceLogger = schedulerLogger
		} else if strings.Contains(r.URL.Path, "/api/") {
			comp = "api"
		}

		var msgBuf [128]byte
		msg := formatRequestLogMessage(msgBuf[:0], r.Method, r.URL.Path, statusCode, durationMs)
		if cfg.Log.IsHTTPLoggingEnabled {
			serviceLogger.Info(msg,
				"component", comp,
				"method", r.Method,
				"path", r.URL.Path,
				"status", statusCode,
				"bytes", lrw.bytes,
				"duration_ms", durationMs,
			)
		} else {
			serviceLogger.Debug(msg,
				"component", comp,
				"method", r.Method,
				"path", r.URL.Path,
				"status", statusCode,
				"bytes", lrw.bytes,
				"duration_ms", durationMs,
			)
		}
		if serviceLogger.IsTraceEnabled() {
			serviceLogger.Trace("HTTP request details",
				"component", comp,
				"method", r.Method,
				"path", r.URL.Path,
				"query", r.URL.RawQuery,
				"status", statusCode,
				"bytes", lrw.bytes,
				"duration_ms", durationMs,
				"duration_us", duration.Microseconds(),
				"client_ip", GetClientIP(r, cfg),
				"remote_addr", r.RemoteAddr,
				"user_agent", r.UserAgent(),
				"x_exodus_real_ip", r.Header.Get(ExodusRealIPHeader),
				"x_forwarded_for", r.Header.Get("X-Forwarded-For"),
				"x_forwarded_proto", r.Header.Get("X-Forwarded-Proto"),
			)
		}
	})
}

func formatRequestLogMessage(buf []byte, method, path string, status int, durationMs int64) string {
	buf = append(buf, method...)
	buf = append(buf, ' ')
	buf = append(buf, path...)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, int64(status), 10)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, durationMs, 10)
	buf = append(buf, "ms"...)
	return string(buf)
}

var skippedStaticExts = []string{".css", ".js", ".map", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp", ".woff", ".woff2", ".ttf", ".eot"}

func shouldSkipAccessLog(cfg *config.BackendConfig, path string) bool {
	if cfg == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Log.NodeEnv), "development") {
		return false
	}
	path = strings.ToLower(strings.TrimSpace(path))
	if path == "" {
		return false
	}
	if path == "/favicon.ico" || path == "/site.webmanifest" || path == "/robots.txt" {
		return true
	}
	if strings.HasPrefix(path, "/assets/") || strings.HasPrefix(path, "/locales/") {
		return true
	}
	for _, ext := range skippedStaticExts {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}
