package nodessh

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/shared"
	"exodus/internal/logger"
	"exodus/internal/security"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/ssh"
)

// ─── Session limiter (#5) ───────────────────────────────────────────────────

const (
	maxConcurrentSessions = 20
	// Match upstream behavior: ping every 25 s, kill after 30 min idle.
	wsPingInterval = 25 * time.Second
	wsIdleTimeout  = 30 * time.Minute
	// Backpressure limits — match upstream WS_HIGH_WATER_BYTES / MAX_BUFFERED_BYTES.
	// When outBuffered exceeds wsOutBufMax the session is terminated so the SSH
	// channel is not starved and the goroutine does not leak indefinitely.
	wsOutBufMax = 65536 // 64 KB
)

var activeSessions atomic.Int32

// ─── Client message structs ─────────────────────────────────────────────────

// SshClientMsg is the JSON envelope received from the browser xterm.js agent.
type SshClientMsg struct {
	T         string   `json:"t"`
	ID        int      `json:"id,omitempty"`
	Host      string   `json:"host,omitempty"`
	Port      int      `json:"port,omitempty"`
	Username  string   `json:"username,omitempty"`
	Cols      int      `json:"cols,omitempty"`
	Rows      int      `json:"rows,omitempty"`
	Keys      []string `json:"keys,omitempty"`
	Signature string   `json:"signature,omitempty"`
	Accept    bool     `json:"accept,omitempty"`
	Message   string   `json:"message,omitempty"`
}

// SshServerMsg is the JSON envelope sent to the browser xterm.js agent.
type SshServerMsg struct {
	T           string  `json:"t"`
	ID          int     `json:"id,omitempty"`
	PublicKey   string  `json:"publicKey,omitempty"`
	Data        string  `json:"data,omitempty"`
	Hash        *string `json:"hash,omitempty"`
	Algo        string  `json:"algo,omitempty"`
	Fingerprint string  `json:"fingerprint,omitempty"`
	Key         string  `json:"key,omitempty"`
	Code        *int    `json:"code,omitempty"`
	Signal      *string `json:"signal,omitempty"`
	Message     string  `json:"message,omitempty"`
}

// ─── Session hub ─────────────────────────────────────────────────────────────

type sessionHub struct {
	ws           *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	reqMu        sync.Mutex
	nextID       int32
	lastActivity atomic.Int64 // unix seconds, touched on every inbound WS message
	outBuffered  atomic.Int64 // bytes currently queued for WS send (backpressure)
	identCh      map[int]chan []string
	signCh       map[int]chan string
	hostCh       map[int]chan bool
	errCh        map[int]chan string
	resizeCh     chan [2]int
	openCh       chan SshClientMsg
	stdinPipe    io.WriteCloser
	pipeMu       sync.Mutex
	log          *logger.Logger
}

func newSessionHub(parentCtx context.Context, ws *websocket.Conn, log *logger.Logger) *sessionHub {
	ctx, cancel := context.WithCancel(parentCtx)
	h := &sessionHub{
		ws:      ws,
		ctx:     ctx,
		cancel:  cancel,
		identCh: make(map[int]chan []string),
		signCh:  make(map[int]chan string),
		hostCh:  make(map[int]chan bool),
		errCh:   make(map[int]chan string),
		// resizeCh is intentionally size-1: the consumer reads the latest resize
		// and the producer uses a non-blocking send that overwrites on full,
		// so only the most-recent dimensions are ever applied (no stale intermediates).
		resizeCh: make(chan [2]int, 1),
		openCh:   make(chan SshClientMsg, 1),
		log:      log,
	}
	h.lastActivity.Store(time.Now().Unix())
	return h
}

func (h *sessionHub) getNextID() int {
	return int(atomic.AddInt32(&h.nextID, 1))
}

// touchActivity records the current time as the last activity timestamp.
// Called on every inbound WS message to drive the idle-timeout check.
func (h *sessionHub) touchActivity() {
	h.lastActivity.Store(time.Now().Unix())
}

func (h *sessionHub) sendJSON(v any) error {
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	return wsjson.Write(ctx, h.ws, v)
}

func (h *sessionHub) sendBinary(b []byte) error {
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()
	return h.ws.Write(ctx, websocket.MessageBinary, b)
}

func (h *sessionHub) sendError(msg string) {
	_ = h.sendJSON(SshServerMsg{T: "error", Message: msg})
}

func (h *sessionHub) setStdinPipe(w io.WriteCloser) {
	h.pipeMu.Lock()
	defer h.pipeMu.Unlock()
	h.stdinPipe = w
}

// writeStdin delivers data to the SSH stdin pipe.
// The lock is released before the blocking Write so the WS reader loop
// is never stalled by a slow SSH server.
func (h *sessionHub) writeStdin(data []byte) {
	h.pipeMu.Lock()
	pipe := h.stdinPipe
	h.pipeMu.Unlock()
	if pipe != nil {
		_, _ = pipe.Write(data)
	}
}

// ─── WebSocket handler ───────────────────────────────────────────────────────

// NodeSSHWSHandler handles the WebSocket stream bridging xterm.js to Node SSH pty.
func NodeSSHWSHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var log *logger.Logger
		if cfg != nil && cfg.Logger != nil {
			log = cfg.Logger.RoleService("API", "NodeSSH")
		}

		if log != nil {
			log.Debug("Incoming WebSocket connection request", "uri", r.URL.RequestURI(), "remoteAddr", r.RemoteAddr)
		}

		// ── Parse subprotocols: "ex, <ticket>, <token>" ──────────────────────
		// #11: Only accept ticket via Sec-WebSocket-Protocol — no ?ticket= query param.
		protoHeader := r.Header.Get("Sec-WebSocket-Protocol")
		parts := strings.Split(protoHeader, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}

		// We require at least [proto, ticket, token] — three elements.
		if len(parts) < 3 || parts[1] == "" || parts[2] == "" {
			if log != nil {
				log.Warn("WebSocket rejected: Sec-WebSocket-Protocol must contain proto,ticket,token")
			}
			http.Error(w, "missing ticket or token in Sec-WebSocket-Protocol", http.StatusBadRequest)
			return
		}
		// Protocol must be exactly "ex" — we no longer support legacy "rw".
		if parts[0] != "ex" {
			if log != nil {
				log.Warn("WebSocket rejected: unsupported subprotocol", "proto", parts[0])
			}
			http.Error(w, "unsupported subprotocol: expected ex", http.StatusBadRequest)
			return
		}
		ticket := parts[1]
		rawToken := parts[2]

		// #1: Verify JWT admin token from sec-websocket-protocol[2].
		if cfg == nil || strings.TrimSpace(cfg.JWT.AuthSecret) == "" {
			if log != nil {
				log.Error("JWT secret not configured")
			}
			http.Error(w, "server misconfiguration", http.StatusInternalServerError)
			return
		}
		tokenPayload, err := security.ParseJWT(cfg.JWT.AuthSecret, rawToken)
		if err != nil {
			if log != nil {
				log.Warn("WebSocket rejected: invalid admin JWT", "error", err)
			}
			http.Error(w, "unauthorized: invalid token", http.StatusUnauthorized)
			return
		}
		if strings.ToUpper(tokenPayload.Role) != "ADMIN" {
			if log != nil {
				log.Warn("WebSocket rejected: token role is not ADMIN", "role", tokenPayload.Role)
			}
			http.Error(w, "unauthorized: insufficient role", http.StatusForbidden)
			return
		}

		// #5: Enforce MAX_CONCURRENT_SESSIONS = 20.
		// Check BEFORE incrementing so the counter is only held for real sessions.
		if cur := activeSessions.Load(); cur >= maxConcurrentSessions {
			if log != nil {
				log.Warn("WebSocket rejected: too many concurrent SSH sessions", "active", cur)
			}
			shared.WriteJSONError(w, http.StatusTooManyRequests, "too many concurrent SSH sessions")
			return
		}
		activeSessions.Add(1)
		defer activeSessions.Add(-1)

		// ── Resolve client IP for ticket IP-binding check (#3) ────────────────
		clientIP := middleware.GetClientIP(r, cfg)
		if clientIP == "" {
			host, _, splitErr := net.SplitHostPort(r.RemoteAddr)
			if splitErr == nil && host != "" {
				clientIP = host
			} else {
				clientIP = r.RemoteAddr
			}
		}

		// Consume ticket (also checks IP and expiry).
		ticketInfo, ok := consumeTicket(ticket, clientIP)
		if !ok {
			if log != nil {
				// Do NOT log the ticket value — it is a bearer credential.
				log.Warn("WebSocket rejected: invalid, expired, or IP-mismatched ticket", "ip", clientIP)
			}
			http.Error(w, "invalid or expired ticket", http.StatusUnauthorized)
			return
		}

		// #1: Cross-check JWT uuid matches ticket's AdminUUID.
		if tokenPayload.UUID != ticketInfo.AdminUUID {
			if log != nil {
				log.Warn("WebSocket rejected: JWT admin UUID does not match ticket AdminUUID",
					"tokenUUID", tokenPayload.UUID, "ticketAdminUUID", ticketInfo.AdminUUID)
			}
			http.Error(w, "unauthorized: token/ticket mismatch", http.StatusUnauthorized)
			return
		}

		if log != nil {
			log.Debug("Ticket and JWT validated", "adminUuid", ticketInfo.AdminUUID, "nodeUuid", ticketInfo.NodeUUID)
		}

		// #10: InsecureSkipVerify removed. Set OriginPatterns to ["*"] so that
		// reverse-proxy deployments (where the Origin header differs from the Host
		// seen by the backend) are not rejected. The JWT+ticket two-factor auth
		// already provides strong request authentication; origin is defence-in-depth.
		opts := &websocket.AcceptOptions{
			Subprotocols:   []string{"ex"},
			OriginPatterns: []string{"*"},
		}

		conn, err := websocket.Accept(w, r, opts)
		if err != nil {
			if log != nil {
				log.Error("WebSocket accept error", "error", err)
			}
			return
		}
		// Matches upstream's explicit payload cap so large pastes into the
		// terminal aren't silently truncated by a smaller library default.
		conn.SetReadLimit(1 << 20)
		defer conn.Close(websocket.StatusNormalClosure, "session closed")

		hub := newSessionHub(r.Context(), conn, log)
		defer hub.cancel()

		// ── Keepalive + idle-timeout goroutine ───────────────────────────────
		// Matches upstream: ping every 25 s, close after 30 min idle.
		// coder/websocket requires Ping to run concurrently with an active Read
		// call — the reader loop below satisfies that requirement.
		go func() {
			ticker := time.NewTicker(wsPingInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					idle := time.Since(time.Unix(hub.lastActivity.Load(), 0))
					if idle >= wsIdleTimeout {
						if log != nil {
							log.Warn("SSH WebSocket idle timeout — closing",
								"idle", idle.Round(time.Second))
						}
						_ = conn.Close(websocket.StatusGoingAway, "idle timeout")
						hub.cancel()
						return
					}
					pingCtx, pingCancel := context.WithTimeout(hub.ctx, 5*time.Second)
					pingErr := conn.Ping(pingCtx)
					pingCancel()
					if pingErr != nil {
						if log != nil {
							log.Debug("SSH WebSocket ping failed — closing", "error", pingErr)
						}
						hub.cancel()
						return
					}
				case <-hub.ctx.Done():
					return
				}
			}
		}()

		// Run SSH session in separate goroutine
		go runSshSession(hub, db, ticketInfo)

		// Main reader loop
		for {
			msgType, data, err := conn.Read(hub.ctx)
			if err != nil {
				if log != nil {
					log.Debug("WebSocket client disconnected", "error", err)
				}
				break
			}
			// Touch on every inbound frame — drives idle timeout.
			hub.touchActivity()

			if msgType == websocket.MessageBinary {
				hub.writeStdin(data)
				continue
			}

			if msgType == websocket.MessageText {
				var msg SshClientMsg
				if err := json.Unmarshal(data, &msg); err != nil {
					if log != nil {
						log.Debug("Malformed control message — closing session", "raw", string(data), "error", err)
					}
					// #13/#44: Close on invalid control message (matches upstream zod safeParse behaviour)
					hub.sendError("invalid control message")
					hub.cancel()
					break
				}

				hub.reqMu.Lock()
				switch msg.T {
				case "open":
					select {
					case hub.openCh <- msg:
					default:
					}
				case "identities":
					if ch, exists := hub.identCh[msg.ID]; exists {
						ch <- msg.Keys
					}
				case "sign":
					if ch, exists := hub.signCh[msg.ID]; exists {
						ch <- msg.Signature
					}
				case "hostkey":
					if ch, exists := hub.hostCh[msg.ID]; exists {
						ch <- msg.Accept
					}
				case "error":
					if ch, exists := hub.errCh[msg.ID]; exists {
						ch <- msg.Message
					}
				case "resize":
					// Drain old pending resize so only the latest dimensions are queued.
					select {
					case <-hub.resizeCh:
					default:
					}
					hub.resizeCh <- [2]int{msg.Rows, msg.Cols}
				}
				hub.reqMu.Unlock()
			}
		}
	}
}

// ─── SSH session runner ──────────────────────────────────────────────────────

func runSshSession(hub *sessionHub, db *pgxpool.Pool, ticketInfo TicketInfo) {
	var openMsg SshClientMsg
	openTimer := time.NewTimer(15 * time.Second)
	defer openTimer.Stop()
	select {
	case openMsg = <-hub.openCh:
	case <-openTimer.C:
		if hub.log != nil {
			hub.log.Warn("Open message timeout")
		}
		hub.sendError("Timeout waiting for open message")
		hub.cancel()
		return
	case <-hub.ctx.Done():
		return
	}

	// #13: Validate open-message field bounds (mirroring upstream zod schema)
	targetHost := strings.TrimSpace(openMsg.Host)
	targetUsername := strings.TrimSpace(openMsg.Username)
	targetPort := openMsg.Port

	if len(targetHost) == 0 || len(targetHost) > 253 {
		hub.sendError("invalid host: must be 1–253 characters")
		hub.cancel()
		return
	}
	if targetPort < 1 || targetPort > 65535 {
		if targetPort == 0 {
			targetPort = 22 // default SSH port
		} else {
			hub.sendError("invalid port: must be 1–65535")
			hub.cancel()
			return
		}
	}
	if len(targetUsername) == 0 || len(targetUsername) > 64 {
		hub.sendError("invalid username: must be 1–64 characters")
		hub.cancel()
		return
	}
	cols := openMsg.Cols
	if cols < 1 || cols > 1000 {
		cols = 80
	}
	rows := openMsg.Rows
	if rows < 1 || rows > 1000 {
		rows = 24
	}
	if len(openMsg.Keys) > 16 {
		hub.sendError("too many identity keys: max 16")
		hub.cancel()
		return
	}
	for _, k := range openMsg.Keys {
		if len(k) > 4096 {
			hub.sendError("identity key too large: max 4096 bytes")
			hub.cancel()
			return
		}
	}
	if len(openMsg.Signature) > 2048 {
		hub.sendError("signature too large: max 2048 bytes")
		hub.cancel()
		return
	}

	if hub.log != nil {
		hub.log.Debug("SSH Open requested", "host", targetHost, "port", targetPort, "username", targetUsername, "cols", cols, "rows", rows)
	}

	// #2: Fetch node address + configured IPs and enforce them as the only
	// allowed SSH targets (SSRF protection). Only checking `address` here
	// would reject legitimate connections to a node's secondary IPs, which
	// the terminal's own connection-setup screen offers via autocomplete.
	var nodeName, nodeAddress string
	var nodeIPsRaw []byte
	err := db.QueryRow(hub.ctx, `SELECT name, address, ips FROM nodes WHERE uuid = $1`, ticketInfo.NodeUUID).
		Scan(&nodeName, &nodeAddress, &nodeIPsRaw)
	if err != nil {
		if hub.log != nil {
			hub.log.Error("Node not found in database", "error", err, "nodeUuid", ticketInfo.NodeUUID)
		}
		hub.sendError("Node not found")
		hub.cancel()
		return
	}

	allowedHosts := []string{nodeAddress}
	if len(nodeIPsRaw) > 0 {
		var ipEntries []struct {
			IP string `json:"ip"`
		}
		if jsonErr := json.Unmarshal(nodeIPsRaw, &ipEntries); jsonErr == nil {
			for _, entry := range ipEntries {
				if ip := strings.TrimSpace(entry.IP); ip != "" {
					allowedHosts = append(allowedHosts, ip)
				}
			}
		}
	}

	// #2: Resolve target and each allowed host to IPs and compare to prevent SSRF
	targetAllowed := false
	for _, allowedHost := range allowedHosts {
		if hostsMatch(targetHost, allowedHost) {
			targetAllowed = true
			break
		}
	}
	if !targetAllowed {
		if hub.log != nil {
			hub.log.Warn("SSH target rejected: host does not match node address or ips",
				"targetHost", targetHost, "nodeUuid", ticketInfo.NodeUUID)
		}
		hub.sendError("SSH target not allowed: host must belong to this node")
		hub.cancel()
		return
	}

	// ── Setup SSH ClientConfig with Browser Signer ───────────────────────────
	authMethod := ssh.PublicKeysCallback(func() ([]ssh.Signer, error) {
		reqID := hub.getNextID()
		idCh := make(chan []string, 1)
		errCh := make(chan string, 1)

		hub.reqMu.Lock()
		hub.identCh[reqID] = idCh
		hub.errCh[reqID] = errCh
		hub.reqMu.Unlock()

		defer func() {
			hub.reqMu.Lock()
			delete(hub.identCh, reqID)
			delete(hub.errCh, reqID)
			hub.reqMu.Unlock()
		}()

		if err := hub.sendJSON(SshServerMsg{T: "agent-identities", ID: reqID}); err != nil {
			return nil, err
		}

		idTimer := time.NewTimer(30 * time.Second)
		defer idTimer.Stop()
		select {
		case keys := <-idCh:
			var signers []ssh.Signer
			for _, kStr := range keys {
				pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(kStr))
				if err != nil {
					continue
				}
				signers = append(signers, &browserSigner{
					hub:    hub,
					pubKey: pubKey,
				})
			}
			return signers, nil
		case errMsg := <-errCh:
			return nil, errors.New(errMsg)
		case <-idTimer.C:
			return nil, errors.New("agent identities timeout")
		case <-hub.ctx.Done():
			return nil, errors.New("session closed")
		}
	})

	sshConfig := &ssh.ClientConfig{
		User: targetUsername,
		Auth: []ssh.AuthMethod{authMethod},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			reqID := hub.getNextID()
			hostCh := make(chan bool, 1)

			hub.reqMu.Lock()
			hub.hostCh[reqID] = hostCh
			hub.reqMu.Unlock()

			defer func() {
				hub.reqMu.Lock()
				delete(hub.hostCh, reqID)
				hub.reqMu.Unlock()
			}()

			fp := ssh.FingerprintSHA256(key)
			keyB64 := base64.StdEncoding.EncodeToString(key.Marshal())

			if err := hub.sendJSON(SshServerMsg{
				T:           "hostkey",
				ID:          reqID,
				Algo:        key.Type(),
				Fingerprint: fp,
				Key:         keyB64,
			}); err != nil {
				return err
			}

			hostTimer := time.NewTimer(30 * time.Second)
			defer hostTimer.Stop()
			select {
			case accept := <-hostCh:
				if !accept {
					return errors.New("host key rejected by user")
				}
				return nil
			case <-hostTimer.C:
				return errors.New("host key prompt timeout")
			case <-hub.ctx.Done():
				return errors.New("session closed")
			}
		},
		Timeout: 30 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", targetHost, targetPort)
	if hub.log != nil {
		hub.log.Debug("Dialing SSH target", "address", addr, "user", targetUsername)
	}

	client, err := ssh.Dial("tcp", addr, sshConfig)
	if err != nil {
		if hub.log != nil {
			hub.log.Error("SSH dial failed", "address", addr, "error", err)
		}
		hub.sendError(fmt.Sprintf("SSH connection failed: %v", err))
		hub.cancel()
		return
	}

	session, err := client.NewSession()
	if err != nil {
		if hub.log != nil {
			hub.log.Error("SSH session creation failed", "error", err)
		}
		_ = client.Close()
		hub.sendError(fmt.Sprintf("SSH session creation failed: %v", err))
		hub.cancel()
		return
	}

	// Active cleanup: when hub.ctx is cancelled (client disconnects, WS closes,
	// timeout, or fatal error), immediately close session and client. This unblocks
	// session.Wait(), stdoutPipe.Read(), and stderrPipe.Read(), allowing all session
	// goroutines and internal crypto/ssh transport loops to terminate cleanly without leaks.
	closeOnce := sync.Once{}
	cleanupSSH := func() {
		closeOnce.Do(func() {
			hub.setStdinPipe(nil)
			if session != nil {
				_ = session.Close()
			}
			if client != nil {
				_ = client.Close()
			}
		})
	}
	defer cleanupSSH()

	go func() {
		<-hub.ctx.Done()
		cleanupSSH()
	}()

	modes := ssh.TerminalModes{
		ssh.ECHO:          1,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}

	if err := session.RequestPty("xterm-256color", rows, cols, modes); err != nil {
		if hub.log != nil {
			hub.log.Error("SSH Request PTY failed", "error", err)
		}
		hub.sendError(fmt.Sprintf("Request PTY failed: %v", err))
		hub.cancel()
		return
	}

	stdinPipe, err := session.StdinPipe()
	if err != nil {
		hub.sendError(fmt.Sprintf("Stdin pipe error: %v", err))
		hub.cancel()
		return
	}
	hub.setStdinPipe(stdinPipe)
	defer func() {
		hub.setStdinPipe(nil)
		stdinPipe.Close()
	}()

	stdoutPipe, err := session.StdoutPipe()
	if err != nil {
		hub.sendError(fmt.Sprintf("Stdout pipe error: %v", err))
		hub.cancel()
		return
	}

	stderrPipe, err := session.StderrPipe()
	if err != nil {
		hub.sendError(fmt.Sprintf("Stderr pipe error: %v", err))
		hub.cancel()
		return
	}

	if err := session.Shell(); err != nil {
		hub.sendError(fmt.Sprintf("Failed to start shell: %v", err))
		hub.cancel()
		return
	}

	if hub.log != nil {
		// Audit trail: log at Info so this survives in production where
		// Debug is typically disabled — otherwise there is no record of
		// who opened an SSH session to which node.
		hub.log.Info("SSH session opened",
			"adminUuid", ticketInfo.AdminUUID,
			"node", nodeName,
			"nodeUuid", ticketInfo.NodeUUID,
			"target", fmt.Sprintf("%s@%s:%d", targetUsername, targetHost, targetPort),
		)
	}
	_ = hub.sendJSON(SshServerMsg{T: "ready"})

	// Handle window resize requests
	go func() {
		for {
			select {
			case sz := <-hub.resizeCh:
				_ = session.WindowChange(sz[0], sz[1])
			case <-hub.ctx.Done():
				return
			}
		}
	}()

	// Pipe SSH stdout to WebSocket binary messages.
	// Backpressure: track outgoing bytes; terminate if client falls too far behind
	// (matches upstream MAX_BUFFERED_BYTES = 65536 terminate-on-overflow behaviour).
	go func() {
		buf := make([]byte, 4096)
		for {
			n, readErr := stdoutPipe.Read(buf)
			if n > 0 {
				if hub.outBuffered.Load() > wsOutBufMax {
					if hub.log != nil {
						hub.log.Warn("SSH stdout buffer overflow — client too slow, terminating session")
					}
					hub.cancel()
					break
				}
				hub.outBuffered.Add(int64(n))
				sendErr := hub.sendBinary(buf[:n])
				hub.outBuffered.Add(-int64(n))
				if sendErr != nil {
					hub.cancel()
					break
				}
			}
			if readErr != nil {
				break
			}
		}
	}()

	// Pipe SSH stderr to WebSocket binary messages (same backpressure logic).
	go func() {
		buf := make([]byte, 4096)
		for {
			n, readErr := stderrPipe.Read(buf)
			if n > 0 {
				if hub.outBuffered.Load() > wsOutBufMax {
					hub.cancel()
					break
				}
				hub.outBuffered.Add(int64(n))
				sendErr := hub.sendBinary(buf[:n])
				hub.outBuffered.Add(-int64(n))
				if sendErr != nil {
					hub.cancel()
					break
				}
			}
			if readErr != nil {
				break
			}
		}
	}()

	// #9: Capture real exit code/signal from session.Wait()
	waitErr := session.Wait()
	exitCode := 0
	var exitSignal *string
	if waitErr != nil {
		var exitErr *ssh.ExitError
		if errors.As(waitErr, &exitErr) {
			exitCode = exitErr.ExitStatus()
			if sig := exitErr.Signal(); sig != "" {
				s := sig
				exitSignal = &s
			}
		}
	}

	if hub.log != nil {
		hub.log.Debug("SSH session ended", "exitCode", exitCode, "signal", exitSignal)
	}
	_ = hub.sendJSON(SshServerMsg{T: "exit", Code: &exitCode, Signal: exitSignal})
	hub.cancel()
}

// ─── SSRF host comparison helper (#2) ───────────────────────────────────────

// hostsMatch reports whether targetHost identifies the same node address as
// allowedHost, using literal comparison only — never DNS resolution.
//
// The upstream equivalent check is a plain `allowedHosts.includes(host)`
// with no lookups at all. Resolving either side via DNS lets the check pass
// today against a domain the caller controls and points at the node, then the
// actual ssh.Dial — which re-resolves independently, moments later — can be
// redirected elsewhere by simply repointing that domain's DNS in between
// (DNS rebinding / TOCTOU). So: domain names for node.address are not
// resolved here — nodes are expected to be identified by their configured
// IP literal(s), matching upstream's model exactly.
func hostsMatch(targetHost, allowedHost string) bool {
	if strings.EqualFold(strings.TrimSpace(targetHost), strings.TrimSpace(allowedHost)) {
		return true
	}
	targetIP := middleware.CanonicalIP(targetHost)
	allowedIP := middleware.CanonicalIP(allowedHost)
	if targetIP == "" || allowedIP == "" {
		return false
	}
	return targetIP == allowedIP
}

// ─── Browser SSH agent signer ────────────────────────────────────────────────

type browserSigner struct {
	hub    *sessionHub
	pubKey ssh.PublicKey
}

func (s *browserSigner) PublicKey() ssh.PublicKey {
	return s.pubKey
}

func (s *browserSigner) Sign(rand io.Reader, data []byte) (*ssh.Signature, error) {
	reqID := s.hub.getNextID()
	sigCh := make(chan string, 1)
	errCh := make(chan string, 1)

	s.hub.reqMu.Lock()
	s.hub.signCh[reqID] = sigCh
	s.hub.errCh[reqID] = errCh
	s.hub.reqMu.Unlock()

	defer func() {
		s.hub.reqMu.Lock()
		delete(s.hub.signCh, reqID)
		delete(s.hub.errCh, reqID)
		s.hub.reqMu.Unlock()
	}()

	pubKeyB64 := base64.StdEncoding.EncodeToString(s.pubKey.Marshal())
	dataB64 := base64.StdEncoding.EncodeToString(data)
	hashAlgo := "sha512"

	if err := s.hub.sendJSON(SshServerMsg{
		T:         "agent-sign",
		ID:        reqID,
		PublicKey: pubKeyB64,
		Data:      dataB64,
		Hash:      &hashAlgo,
	}); err != nil {
		return nil, err
	}

	signTimer := time.NewTimer(30 * time.Second)
	defer signTimer.Stop()
	select {
	case sigB64 := <-sigCh:
		sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
		if err != nil {
			return nil, err
		}
		return &ssh.Signature{
			Format: s.pubKey.Type(),
			Blob:   sigBytes,
		}, nil
	case errMsg := <-errCh:
		return nil, errors.New(errMsg)
	case <-signTimer.C:
		return nil, errors.New("agent sign timeout")
	case <-s.hub.ctx.Done():
		return nil, errors.New("session closed")
	}
}

// SignWithAlgorithm implements ssh.AlgorithmSigner
func (s *browserSigner) SignWithAlgorithm(rand io.Reader, data []byte, algorithm string) (*ssh.Signature, error) {
	reqID := s.hub.getNextID()
	sigCh := make(chan string, 1)
	errCh := make(chan string, 1)

	s.hub.reqMu.Lock()
	s.hub.signCh[reqID] = sigCh
	s.hub.errCh[reqID] = errCh
	s.hub.reqMu.Unlock()

	defer func() {
		s.hub.reqMu.Lock()
		delete(s.hub.signCh, reqID)
		delete(s.hub.errCh, reqID)
		s.hub.reqMu.Unlock()
	}()

	pubKeyB64 := base64.StdEncoding.EncodeToString(s.pubKey.Marshal())
	dataB64 := base64.StdEncoding.EncodeToString(data)
	hashAlgo := algorithm

	if err := s.hub.sendJSON(SshServerMsg{
		T:         "agent-sign",
		ID:        reqID,
		PublicKey: pubKeyB64,
		Data:      dataB64,
		Hash:      &hashAlgo,
	}); err != nil {
		return nil, err
	}

	signTimer := time.NewTimer(30 * time.Second)
	defer signTimer.Stop()
	select {
	case sigB64 := <-sigCh:
		sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
		if err != nil {
			return nil, err
		}
		return &ssh.Signature{
			Format: algorithm,
			Blob:   sigBytes,
		}, nil
	case errMsg := <-errCh:
		return nil, errors.New(errMsg)
	case <-signTimer.C:
		return nil, errors.New("agent sign timeout")
	case <-s.hub.ctx.Done():
		return nil, errors.New("session closed")
	}
}
