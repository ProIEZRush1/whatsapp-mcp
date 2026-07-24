package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	"rsc.io/qr"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages.
	// WAL mode + busy timeout so the bridge's constant writes never block the
	// Python MCP's reads (this was the main cause of the MCP "getting stuck").
	db, err := sql.Open("sqlite3", "file:store/messages.db?_foreign_keys=on&_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			quoted_id TEXT,
			quoted_sender TEXT,
			quoted_content TEXT,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);

		CREATE TABLE IF NOT EXISTS webhooks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chat_jid TEXT NOT NULL,
			kind TEXT NOT NULL DEFAULT 'http',
			target TEXT NOT NULL,
			include_own INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(chat_jid, kind, target)
		);

		CREATE TABLE IF NOT EXISTS cmux_outbox (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			surface TEXT NOT NULL,
			notice TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	// Migrate existing databases: add quoted-reply columns if missing.
	// ALTER TABLE errors (column already exists) are intentionally ignored.
	for _, stmt := range []string{
		"ALTER TABLE messages ADD COLUMN quoted_id TEXT",
		"ALTER TABLE messages ADD COLUMN quoted_sender TEXT",
		"ALTER TABLE messages ADD COLUMN quoted_content TEXT",
	} {
		db.Exec(stmt)
	}

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// ---- Webhooks: push new incoming messages to subscribed sessions ----

// Webhook is a subscription: deliver messages of ChatJID ('*' = all chats) to Target.
type Webhook struct {
	ChatJID    string `json:"chat_jid"`
	Kind       string `json:"kind"`   // "http" | "cmux"
	Target     string `json:"target"` // URL (http) or cmux surface id (cmux)
	IncludeOwn bool   `json:"include_own"`
}

// WebhookPayload is the JSON delivered on each matching message.
type WebhookPayload struct {
	Event         string `json:"event"`
	MessageID     string `json:"message_id"`
	ChatJID       string `json:"chat_jid"`
	ChatName      string `json:"chat_name"`
	Sender        string `json:"sender"`
	SenderName    string `json:"sender_name"`
	Content       string `json:"content"`
	Timestamp     string `json:"timestamp"`
	IsFromMe      bool   `json:"is_from_me"`
	IsGroup       bool   `json:"is_group"`
	MediaType     string `json:"media_type,omitempty"`
	Filename      string `json:"filename,omitempty"`
	QuotedID      string `json:"quoted_id,omitempty"`
	QuotedContent string `json:"quoted_content,omitempty"`
}

const cmuxBinPath = "/Applications/cmux.app/Contents/Resources/bin/cmux"

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// AddWebhook registers (or replaces) a subscription.
func (store *MessageStore) AddWebhook(chatJID, kind, target string, includeOwn bool) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO webhooks (chat_jid, kind, target, include_own) VALUES (?, ?, ?, ?)",
		chatJID, kind, target, b2i(includeOwn),
	)
	return err
}

// DeleteWebhook removes a subscription; returns the number of rows removed.
func (store *MessageStore) DeleteWebhook(chatJID, kind, target string) (int64, error) {
	res, err := store.db.Exec(
		"DELETE FROM webhooks WHERE chat_jid = ? AND kind = ? AND target = ?",
		chatJID, kind, target,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func scanWebhooks(rows *sql.Rows) ([]Webhook, error) {
	defer rows.Close()
	hooks := []Webhook{}
	for rows.Next() {
		var w Webhook
		var inc int
		if err := rows.Scan(&w.ChatJID, &w.Kind, &w.Target, &inc); err != nil {
			return nil, err
		}
		w.IncludeOwn = inc != 0
		hooks = append(hooks, w)
	}
	return hooks, rows.Err()
}

// ListWebhooks returns all subscriptions.
func (store *MessageStore) ListWebhooks() ([]Webhook, error) {
	rows, err := store.db.Query("SELECT chat_jid, kind, target, include_own FROM webhooks ORDER BY created_at")
	if err != nil {
		return nil, err
	}
	return scanWebhooks(rows)
}

// OutboxItem is one pending cmux delivery waiting to be sent by the in-session relay.
type OutboxItem struct {
	ID      int64  `json:"id"`
	Surface string `json:"surface"`
	Notice  string `json:"notice"`
}

// EnqueueCmux queues a cmux delivery. The bridge (a background daemon) cannot run
// `cmux send` itself — cmux only accepts socket writes from processes living inside
// its app session tree — so a relay running in a cmux surface drains this queue.
func (store *MessageStore) EnqueueCmux(surface, notice string) error {
	_, err := store.db.Exec("INSERT INTO cmux_outbox (surface, notice) VALUES (?, ?)", surface, notice)
	return err
}

// ListCmuxOutbox returns pending cmux deliveries (oldest first).
func (store *MessageStore) ListCmuxOutbox(limit int) ([]OutboxItem, error) {
	rows, err := store.db.Query("SELECT id, surface, notice FROM cmux_outbox ORDER BY id LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []OutboxItem
	for rows.Next() {
		var it OutboxItem
		if err := rows.Scan(&it.ID, &it.Surface, &it.Notice); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// AckCmuxOutbox deletes delivered rows by id.
func (store *MessageStore) AckCmuxOutbox(ids []int64) error {
	for _, id := range ids {
		if _, err := store.db.Exec("DELETE FROM cmux_outbox WHERE id = ?", id); err != nil {
			return err
		}
	}
	return nil
}

// getWebhooksForChat returns subscriptions matching a chat (exact JID or '*').
func (store *MessageStore) getWebhooksForChat(chatJID string) ([]Webhook, error) {
	rows, err := store.db.Query(
		"SELECT chat_jid, kind, target, include_own FROM webhooks WHERE chat_jid = ? OR chat_jid = '*'",
		chatJID,
	)
	if err != nil {
		return nil, err
	}
	return scanWebhooks(rows)
}

// ---------------------------------------------------------------------------
// Per-chat ordered delivery pipeline (only for SUBSCRIBED chats).
//
// Each subscribed chat has a FIFO worker so messages reach sessions in arrival
// order even while a slow audio is transcribed (a later text can't jump ahead
// of an earlier audio). Media is downloaded once and audio transcribed once,
// then the resulting notice is fanned out to every subscription — so two
// sessions never transcribe/download the same message. Unsubscribed chats are
// never downloaded or transcribed.
// ---------------------------------------------------------------------------

type chatJob struct {
	client *whatsmeow.Client
	store  *MessageStore
	p      WebhookPayload
	hooks  []Webhook
}

var (
	chatQueues   = make(map[string]chan chatJob)
	chatQueuesMu sync.Mutex
)

func enqueueChatJob(job chatJob, logger waLog.Logger) {
	chatQueuesMu.Lock()
	ch, ok := chatQueues[job.p.ChatJID]
	if !ok {
		ch = make(chan chatJob, 256)
		chatQueues[job.p.ChatJID] = ch
		go func(c chan chatJob) {
			for j := range c {
				processChatJob(j, logger)
			}
		}(ch)
	}
	chatQueuesMu.Unlock()
	select {
	case ch <- job:
	default:
		// Queue full: deliver without ordering rather than block the event loop.
		logger.Warnf("chat queue full for %s; delivering out of order", job.p.ChatJID)
		go processChatJob(job, logger)
	}
}

// processChatJob downloads media (once) + transcribes audio (once), then enqueues
// the resulting notice to every subscription. Runs inside the chat's FIFO worker,
// so a slow audio transcription holds back later messages IN THAT CHAT only.
func processChatJob(job chatJob, logger waLog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			logger.Warnf("chat job panicked: %v", r)
		}
	}()
	p := job.p
	if p.MediaType != "" {
		ok, _, _, absPath, err := downloadMedia(job.client, job.store, p.MessageID, p.ChatJID)
		caption := strings.TrimSpace(p.Content)
		if !ok || err != nil {
			logger.Warnf("media download failed for %s: %v", p.MessageID, err)
			if p.Content == "" {
				p.Content = "[" + p.MediaType + " – no se pudo descargar]"
			}
		} else if p.MediaType == "audio" {
			text := transcribeAudio(absPath, logger)
			if text != "" {
				p.Content = "🎤 " + text
			} else {
				p.Content = "🎤 [audio – no se pudo transcribir] → " + absPath
			}
			if caption != "" {
				p.Content += " | " + caption
			}
		} else {
			// any other file: hand the session the local path directly
			p.Content = "📎 " + p.MediaType + " → " + absPath
			if caption != "" {
				p.Content += " | " + caption
			}
		}
	}
	notice := strings.ReplaceAll(strings.ReplaceAll(formatWhatsAppNotice(p), "\n", " "), "\r", " ")
	for _, h := range job.hooks {
		switch h.Kind {
		case "cmux":
			if err := job.store.EnqueueCmux(h.Target, notice); err != nil {
				logger.Warnf("cmux outbox enqueue failed for %s: %v", h.Target, err)
			}
		default: // http
			go deliverWebhook(job.store, h, p, logger)
		}
	}
}

// storeMaxBytes caps the on-disk media store (downloaded files) at ~20 GB;
// oldest files are deleted first. Databases and the pairing QR are never touched.
const storeMaxBytes int64 = 20 * 1024 * 1024 * 1024

func enforceStoreCap() {
	type fentry struct {
		path string
		size int64
		mod  time.Time
	}
	var files []fentry
	var total int64
	_ = filepath.Walk("store", func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		name := info.Name()
		if strings.HasSuffix(name, ".db") || strings.Contains(name, ".db-") || name == "pair_qr.png" {
			return nil
		}
		files = append(files, fentry{path, info.Size(), info.ModTime()})
		total += info.Size()
		return nil
	})
	if total <= storeMaxBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.Before(files[j].mod) }) // oldest first
	var freed int64
	for _, f := range files {
		if total-freed <= storeMaxBytes {
			break
		}
		if os.Remove(f.path) == nil {
			freed += f.size
		}
	}
	if freed > 0 {
		fmt.Printf("[store-cap] freed %d MB to stay under 20GB\n", freed/1024/1024)
	}
}

func startStoreCleaner() {
	enforceStoreCap()
	go func() {
		t := time.NewTicker(20 * time.Minute)
		defer t.Stop()
		for range t.C {
			enforceStoreCap()
		}
	}()
}

// transcribeAudio runs the local whisper model (once) on an audio file.
func transcribeAudio(path string, logger waLog.Logger) string {
	script := "transcribe.py"
	if wd, err := os.Getwd(); err == nil {
		if p := filepath.Join(wd, "transcribe.py"); fileExists(p) {
			script = p
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", script, path)
	out, err := cmd.Output()
	if err != nil {
		logger.Warnf("transcription failed for %s: %v", path, err)
		return ""
	}
	return strings.TrimSpace(string(out))
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// --- Presence-based priority coordination -------------------------------------
// When two bot nodes share the same chats, one node can be given priority on
// certain chats. This node then defers on those chats WHEN the peer is actively
// processing them (per the presence service), and takes over only if the peer
// doesn't reply within a timeout. Config is presence.json (per-machine, absent
// -> feature off).

type presenceCfg struct {
	NodeID          string   `json:"node_id"`
	PresenceURL     string   `json:"presence_url"`
	Token           string   `json:"token"`
	PriorityPeer    string   `json:"priority_peer"`   // single peer (legacy); prefer priority_peers
	PriorityPeers   []string `json:"priority_peers"`  // nodes we defer to (any active -> defer)
	PriorityChats   []string `json:"priority_chats"`  // chats the peers lead
	PeerNumber      string   `json:"peer_number"`     // single peer number (legacy); prefer peer_numbers
	PeerNumbers     []string `json:"peer_numbers"`    // peers' WhatsApp numbers (to detect their replies)
	TakeoverSeconds int      `json:"takeover_seconds"`
}

var presence *presenceCfg
var priorityChatSet = map[string]bool{}
var priorityPeers []string          // node ids we defer to
var peerNumberSet = map[string]bool{} // digits-only peer numbers, for reply detection
var peerLastMsg = struct {
	sync.Mutex
	m map[string]time.Time
}{m: map[string]time.Time{}}

func onlyDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func loadPresenceConfig() {
	path := "presence.json"
	if wd, err := os.Getwd(); err == nil {
		if p := filepath.Join(wd, "presence.json"); fileExists(p) {
			path = p
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var c presenceCfg
	if err := json.Unmarshal(data, &c); err != nil {
		fmt.Println("presence.json parse error:", err)
		return
	}
	if c.PresenceURL == "" || c.Token == "" || c.NodeID == "" {
		return
	}
	if c.TakeoverSeconds <= 0 {
		c.TakeoverSeconds = 90
	}
	for _, ch := range c.PriorityChats {
		priorityChatSet[ch] = true
	}
	// Merge legacy single fields into the plural lists.
	priorityPeers = append([]string{}, c.PriorityPeers...)
	if c.PriorityPeer != "" {
		priorityPeers = append(priorityPeers, c.PriorityPeer)
	}
	for _, n := range append(append([]string{}, c.PeerNumbers...), c.PeerNumber) {
		if d := onlyDigits(n); d != "" {
			peerNumberSet[d] = true
		}
	}
	presence = &c
	fmt.Printf("presence coordination on: node=%s peers=%v priorityChats=%d peerNumbers=%d\n",
		c.NodeID, priorityPeers, len(c.PriorityChats), len(peerNumberSet))
}

// recordPeerMessage notes when ANY priority peer posts in a chat, so takeover can
// tell whether a peer already answered.
func recordPeerMessage(chatJID, sender string, ts time.Time) {
	if presence == nil || len(peerNumberSet) == 0 {
		return
	}
	if !peerNumberSet[onlyDigits(sender)] {
		return
	}
	peerLastMsg.Lock()
	peerLastMsg.m[chatJID] = ts
	peerLastMsg.Unlock()
}

func peerRepliedAfter(chatJID string, t time.Time) bool {
	peerLastMsg.Lock()
	defer peerLastMsg.Unlock()
	last, ok := peerLastMsg.m[chatJID]
	return ok && last.After(t)
}

// peerActiveForChat asks the presence service whether ANY priority peer is
// currently processing this chat. Fails OPEN (false) so a presence outage means
// we answer rather than go silent.
func peerActiveForChat(chatJID string) bool {
	if presence == nil || len(priorityPeers) == 0 {
		return false
	}
	client := &http.Client{Timeout: 6 * time.Second}
	for _, peer := range priorityPeers {
		if peer == "" {
			continue
		}
		u := fmt.Sprintf("%s/active?node=%s&chat=%s",
			strings.TrimRight(presence.PresenceURL, "/"),
			url.QueryEscape(peer), url.QueryEscape(chatJID))
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		req.Header.Set("X-Presence-Token", presence.Token)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var out struct {
			Active bool `json:"active"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if err == nil && out.Active {
			return true // some peer is handling this chat
		}
	}
	return false
}

// contextStale marks chats where we skipped ≥1 message (the peer handled it) since
// our last delivery. When we next deliver here, we first send the recent history
// as a CONTEXT-ONLY block so the taking-over session has the thread.
var contextStale = struct {
	sync.Mutex
	m map[string]bool
}{m: map[string]bool{}}

func markContextStale(chatJID string) {
	contextStale.Lock()
	contextStale.m[chatJID] = true
	contextStale.Unlock()
}

// takeContextStale returns true and clears the flag if the chat had skipped messages.
func takeContextStale(chatJID string) bool {
	contextStale.Lock()
	defer contextStale.Unlock()
	if contextStale.m[chatJID] {
		delete(contextStale.m, chatJID)
		return true
	}
	return false
}

// deliverContextBacklog enqueues the chat's recent history to subscribed sessions
// as a DO-NOT-PROCESS context block, so a session taking over understands the
// thread the peer was handling.
func deliverContextBacklog(store *MessageStore, chatJID, chatName string, logger waLog.Logger) {
	hooks, err := store.getWebhooksForChat(chatJID)
	if err != nil || len(hooks) == 0 {
		return
	}
	msgs, err := store.GetMessages(chatJID, 25) // newest-first
	if err != nil || len(msgs) == 0 {
		return
	}
	var b strings.Builder
	b.WriteString("🧩 CONTEXTO — NO RESPONDER / NO PROCESAR. Es solo el historial reciente de \"")
	b.WriteString(chatName)
	b.WriteString("\" que atendió el otro nodo mientras este chat estaba en pausa. Léelo para tener contexto del siguiente mensaje; NO contestes a esto:")
	for i := len(msgs) - 1; i >= 0; i-- { // oldest -> newest
		m := msgs[i]
		who := m.Sender
		if m.IsFromMe {
			who = "Yo"
		} else if peerNumberSet[onlyDigits(m.Sender)] {
			who = "colega" // a priority peer (Jose/Elias) handled this
		}
		content := m.Content
		if content == "" && m.MediaType != "" {
			content = "[" + m.MediaType + "]"
		}
		if content == "" {
			continue
		}
		b.WriteString(" | " + who + ": " + content)
	}
	notice := strings.ReplaceAll(strings.ReplaceAll(b.String(), "\n", " "), "\r", " ")
	for _, h := range hooks {
		if h.Kind == "cmux" {
			if err := store.EnqueueCmux(h.Target, notice); err != nil {
				logger.Warnf("context backlog enqueue failed for %s: %v", h.Target, err)
			}
		}
	}
	logger.Infof("[priority] delivered context backlog for %s (%d msgs)", chatJID, len(msgs))
}

// gatedDispatch applies priority coordination before fanning a message out to
// sessions. On a priority chat where the peer is actively processing, it holds
// the message ~takeover seconds; if the peer posts a reply meanwhile it drops
// the message (marking the chat context-stale), otherwise it takes over. Whenever
// it delivers after having skipped messages, it first back-fills the history as
// a do-not-process context block.
func gatedDispatch(client *whatsmeow.Client, store *MessageStore, p WebhookPayload, logger waLog.Logger) {
	if presence == nil || p.IsFromMe || !priorityChatSet[p.ChatJID] {
		dispatchWebhooks(client, store, p, logger)
		return
	}
	if !peerActiveForChat(p.ChatJID) {
		// peer offline -> we answer; back-fill context if we skipped messages here.
		if takeContextStale(p.ChatJID) {
			deliverContextBacklog(store, p.ChatJID, p.ChatName, logger)
		}
		dispatchWebhooks(client, store, p, logger)
		return
	}
	arrival := time.Now()
	d := time.Duration(presence.TakeoverSeconds) * time.Second
	logger.Infof("[priority] %s: a peer %v active — holding %s for takeover", p.ChatJID, priorityPeers, d)
	go func() {
		time.Sleep(d)
		if peerRepliedAfter(p.ChatJID, arrival) {
			markContextStale(p.ChatJID) // peer handled it; remember to back-fill later
			logger.Infof("[priority] %s handled by peer — skipping (marked context-stale)", p.ChatJID)
			return
		}
		if takeContextStale(p.ChatJID) {
			deliverContextBacklog(store, p.ChatJID, p.ChatName, logger)
		}
		logger.Infof("[priority] peer silent on %s — taking over", p.ChatJID)
		dispatchWebhooks(client, store, p, logger)
	}()
}

// dispatchWebhooks: only subscribed chats get processed. Everything for a
// subscribed chat goes through the per-chat FIFO so order is preserved.
func dispatchWebhooks(client *whatsmeow.Client, store *MessageStore, p WebhookPayload, logger waLog.Logger) {
	hooks, err := store.getWebhooksForChat(p.ChatJID)
	if err != nil {
		logger.Warnf("Webhook lookup failed: %v", err)
		return
	}
	var applicable []Webhook
	for _, h := range hooks {
		if p.IsFromMe && !h.IncludeOwn {
			continue
		}
		applicable = append(applicable, h)
	}
	if len(applicable) == 0 {
		return // not subscribed → do not download or transcribe
	}
	enqueueChatJob(chatJob{client: client, store: store, p: p, hooks: applicable}, logger)
}

func deliverWebhook(store *MessageStore, h Webhook, p WebhookPayload, logger waLog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			logger.Warnf("Webhook delivery panicked: %v", r)
		}
	}()
	switch h.Kind {
	case "cmux":
		// The bridge runs as a background daemon (reparented to launchd), so it is
		// NOT a live descendant of the cmux app; cmux rejects socket writes from
		// outside its session tree with "broken pipe". Instead we enqueue the
		// delivery and let a relay running INSIDE a cmux surface (cmux-relay.sh)
		// drain the queue and perform the actual `cmux send`.
		notice := strings.ReplaceAll(strings.ReplaceAll(formatWhatsAppNotice(p), "\n", " "), "\r", " ")
		if err := store.EnqueueCmux(h.Target, notice); err != nil {
			logger.Warnf("cmux outbox enqueue failed for surface %s: %v", h.Target, err)
		}
	default: // "http"
		body, _ := json.Marshal(p)
		req, err := http.NewRequest(http.MethodPost, h.Target, bytes.NewReader(body))
		if err != nil {
			logger.Warnf("http webhook build failed for %s: %v", h.Target, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			logger.Warnf("http webhook -> %s failed: %v", h.Target, err)
			return
		}
		resp.Body.Close()
	}
}

func formatWhatsAppNotice(p WebhookPayload) string {
	who := p.ChatName
	if who == "" {
		who = p.ChatJID
	}
	// Always surface who actually sent it: prefer the contact/push name, fall
	// back to the phone number. In groups it's appended after the group name;
	// in DMs it confirms the sender even if the chat name is stale.
	slabel := p.SenderName
	if slabel == "" {
		slabel = p.Sender
	}
	if !p.IsFromMe && slabel != "" {
		if p.IsGroup {
			who = fmt.Sprintf("%s / %s", who, slabel)
		} else if slabel != who {
			who = fmt.Sprintf("%s (%s)", who, slabel)
		}
	}
	body := p.Content
	if body == "" && p.MediaType != "" {
		body = "[" + p.MediaType + "]"
	}
	// Include the message id so the agent can answer NATIVELY (WhatsApp quoted
	// reply): send_message(recipient=<jid>, message=..., reply_to=<msg id>).
	idPart := ""
	if p.Event != "reaction" && p.MessageID != "" {
		idPart = fmt.Sprintf(" (msg %s)", p.MessageID)
	}
	notice := fmt.Sprintf("\U0001F4E9 WhatsApp — %s [%s]%s: %s", who, p.ChatJID, idPart, body)
	if p.QuotedID != "" {
		notice += fmt.Sprintf(" (↩ reply-to %s", p.QuotedID)
		if q := p.QuotedContent; q != "" {
			r := []rune(q)
			if len(r) > 60 {
				q = string(r[:60]) + "…"
			}
			notice += ": " + q
		}
		notice += ")"
	}
	return notice
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64,
	quotedID, quotedSender, quotedContent string) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, quoted_id, quoted_sender, quoted_content)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, quotedID, quotedSender, quotedContent,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// getMessageContent returns the stored text of a message (for reaction context).
func (store *MessageStore) getMessageContent(chatJID, id string) string {
	if id == "" {
		return ""
	}
	var content string
	err := store.db.QueryRow(
		"SELECT content FROM messages WHERE id = ? AND chat_jid = ? LIMIT 1", id, chatJID,
	).Scan(&content)
	if err != nil {
		return ""
	}
	return content
}

// updateMessageContent rewrites a stored message's text in place — used when an
// edit arrives so history and quoted-reply lookups reflect the edited content.
func (store *MessageStore) updateMessageContent(chatJID, id, content string) {
	if id == "" {
		return
	}
	_, _ = store.db.Exec(
		"UPDATE messages SET content = ? WHERE id = ? AND chat_jid = ?", content, id, chatJID)
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// Media captions (image/video/document) — so a captioned media message isn't
	// stored with empty content (otherwise the caption/request would be lost).
	if img := msg.GetImageMessage(); img != nil {
		if c := strings.TrimSpace(img.GetCaption()); c != "" {
			return c
		}
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		if c := strings.TrimSpace(vid.GetCaption()); c != "" {
			return c
		}
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		if c := strings.TrimSpace(doc.GetCaption()); c != "" {
			return c
		}
	}

	// For now, we're ignoring other non-text messages
	return ""
}

// extractContextInfo pulls the quoted-reply reference (if any) out of a message.
// Returns the quoted message's StanzaID, its sender (participant JID), and a
// best-effort text representation of the quoted message.
func extractContextInfo(msg *waProto.Message) (quotedID string, quotedSender string, quotedContent string) {
	if msg == nil {
		return "", "", ""
	}

	var ci *waProto.ContextInfo
	switch {
	case msg.GetExtendedTextMessage() != nil:
		ci = msg.GetExtendedTextMessage().GetContextInfo()
	case msg.GetImageMessage() != nil:
		ci = msg.GetImageMessage().GetContextInfo()
	case msg.GetVideoMessage() != nil:
		ci = msg.GetVideoMessage().GetContextInfo()
	case msg.GetDocumentMessage() != nil:
		ci = msg.GetDocumentMessage().GetContextInfo()
	case msg.GetAudioMessage() != nil:
		ci = msg.GetAudioMessage().GetContextInfo()
	case msg.GetStickerMessage() != nil:
		ci = msg.GetStickerMessage().GetContextInfo()
	}

	if ci == nil || ci.GetStanzaID() == "" {
		return "", "", ""
	}

	quotedID = ci.GetStanzaID()
	quotedSender = ci.GetParticipant()

	// Best-effort text of the quoted message: plain text first, then captions/filenames.
	qm := ci.GetQuotedMessage()
	quotedContent = extractTextContent(qm)
	if quotedContent == "" && qm != nil {
		if img := qm.GetImageMessage(); img != nil {
			quotedContent = strings.TrimSpace("[image] " + img.GetCaption())
		} else if vid := qm.GetVideoMessage(); vid != nil {
			quotedContent = strings.TrimSpace("[video] " + vid.GetCaption())
		} else if doc := qm.GetDocumentMessage(); doc != nil {
			quotedContent = strings.TrimSpace("[document] " + doc.GetFileName())
		} else if qm.GetAudioMessage() != nil {
			quotedContent = "[audio]"
		}
	}

	return quotedID, quotedSender, quotedContent
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient        string   `json:"recipient"`
	Message          string   `json:"message"`
	MediaPath        string   `json:"media_path,omitempty"`
	ReplyTo          string   `json:"reply_to,omitempty"`
	ReplyParticipant string   `json:"reply_participant,omitempty"`
	ReplyText        string   `json:"reply_text,omitempty"`
	Mentions         []string `json:"mentions,omitempty"` // phone numbers or JIDs to @-tag
}

// mentionNumberRe matches an @-mention written as a bare international number
// (WhatsApp's on-the-wire mention form, e.g. "@5215551091852"). 7–15 digits
// avoids false positives on short "@123" fragments or prices.
var mentionNumberRe = regexp.MustCompile(`@(\d{7,15})`)

// buildMentionedJIDs turns @-number tokens found in the text plus any explicitly
// requested mentions into the list of JIDs WhatsApp needs in ContextInfo for the
// "@number" to render as the member's name and actually ping them. Numbers map to
// "<number>@s.whatsapp.net"; a value that already contains "@" is used verbatim
// (so LID JIDs like "<lid>@lid" pass through). Deduplicated, order-stable.
func buildMentionedJIDs(text string, explicit []string) []string {
	seen := map[string]bool{}
	var out []string
	addJID := func(jid string) {
		if jid == "" || seen[jid] {
			return
		}
		seen[jid] = true
		out = append(out, jid)
	}
	digitsOnly := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			if r >= '0' && r <= '9' {
				b.WriteRune(r)
			}
		}
		return b.String()
	}
	for _, m := range mentionNumberRe.FindAllStringSubmatch(text, -1) {
		addJID(m[1] + "@s.whatsapp.net")
	}
	for _, e := range explicit {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if strings.Contains(e, "@") {
			addJID(e) // already a JID
		} else if n := digitsOnly(strings.TrimPrefix(e, "+")); n != "" {
			addJID(n + "@s.whatsapp.net")
		}
	}
	return out
}

// ReactionRequest is the body for /api/react
type ReactionRequest struct {
	Recipient string `json:"recipient"`
	MessageID string `json:"message_id"`
	Sender    string `json:"sender,omitempty"`
	Emoji     string `json:"emoji"`
}

// MarkReadRequest is the body for /api/markread
type MarkReadRequest struct {
	Recipient string `json:"recipient"`
	MessageID string `json:"message_id"`
	Sender    string `json:"sender,omitempty"`
}

// PresenceRequest is the body for /api/presence
type PresenceRequest struct {
	Recipient string `json:"recipient"`
	State     string `json:"state"`
}

// GroupParticipantsRequest is the body for /api/group-participants
type GroupParticipantsRequest struct {
	GroupJID     string   `json:"group_jid"`
	Participants []string `json:"participants"` // phone numbers or JIDs
	Action       string   `json:"action"`       // add | remove | promote | demote
}

// parseRecipientJID turns a phone number or JID string into a types.JID.
func parseRecipientJID(recipient string) (types.JID, error) {
	if strings.Contains(recipient, "@") {
		return types.ParseJID(recipient)
	}
	return types.JID{User: recipient, Server: "s.whatsapp.net"}, nil
}

// normalizeQuotedParticipant turns a stored sender (often a bare @lid number
// like "277128604086355") into a full, resolvable JID so WhatsApp attributes a
// quoted reply to the real sender instead of defaulting to "You". Bare numbers
// are treated as @lid and resolved to the phone-number JID (matching how chats
// and messages are stored).
func normalizeQuotedParticipant(client *whatsmeow.Client, participant string) string {
	if participant == "" {
		return ""
	}
	s := participant
	if !strings.Contains(s, "@") {
		s = s + "@lid"
	}
	pj, err := types.ParseJID(s)
	if err != nil {
		return participant
	}
	if pj.Server == types.HiddenUserServer {
		if pn, e := client.Store.LIDs.GetPNForLID(context.Background(), pj); e == nil && !pn.IsEmpty() && pn.Server == types.DefaultUserServer {
			pj = pn
		}
	}
	return pj.ToNonAD().String()
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, recipient string, message string, mediaPath string, replyTo string, replyParticipant string, replyText string, mentions []string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types
		case "pdf":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/pdf"
		case "doc":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/msword"
		case "docx":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
		case "xls":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.ms-excel"
		case "xlsx":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		case "ppt":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.ms-powerpoint"
		case "pptx":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
		case "csv":
			mediaType = whatsmeow.MediaDocument
			mimeType = "text/csv"
		case "txt":
			mediaType = whatsmeow.MediaDocument
			mimeType = "text/plain"
		case "zip":
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/zip"

		// Any other file type
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			docFileName := mediaPath[strings.LastIndex(mediaPath, "/")+1:]
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(docFileName),
				FileName:      proto.String(docFileName),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		mentionedJIDs := buildMentionedJIDs(message, mentions)
		// A plain Conversation can't carry ContextInfo, so any reply OR mention
		// forces the richer ExtendedTextMessage.
		if replyTo != "" || len(mentionedJIDs) > 0 {
			ctxInfo := &waProto.ContextInfo{}
			if replyTo != "" {
				ctxInfo.StanzaID = proto.String(replyTo)
				ctxInfo.QuotedMessage = &waProto.Message{Conversation: proto.String(replyText)}
				if replyParticipant != "" {
					ctxInfo.Participant = proto.String(normalizeQuotedParticipant(client, replyParticipant))
				}
			}
			if len(mentionedJIDs) > 0 {
				ctxInfo.MentionedJID = mentionedJIDs
			}
			msg.ExtendedTextMessage = &waProto.ExtendedTextMessage{
				Text:        proto.String(message),
				ContextInfo: ctxInfo,
			}
		} else {
			msg.Conversation = proto.String(message)
		}
	}

	// Send message
	_, err = client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
// canonicalChatJID maps a LID direct-message chat to the contact's phone-number
// JID (preferring the alternative address carried on the message, falling back to
// the LID->PN mapping in the store) so a contact never fragments into separate
// @lid and @s.whatsapp.net chats. Groups and already-phone-number chats pass
// through unchanged.
func canonicalChatJID(client *whatsmeow.Client, chat, senderAlt, recipientAlt types.JID, isFromMe, isGroup bool, logger waLog.Logger) types.JID {
	if isGroup || chat.Server != types.HiddenUserServer {
		return chat
	}
	alt := senderAlt
	if isFromMe {
		alt = recipientAlt
	}
	if !alt.IsEmpty() && alt.Server == types.DefaultUserServer {
		return alt
	}
	if pn, err := client.Store.LIDs.GetPNForLID(context.Background(), chat); err == nil && !pn.IsEmpty() && pn.Server == types.DefaultUserServer {
		return pn
	}
	logger.Warnf("Could not resolve phone number for LID chat %s; storing under LID", chat.String())
	return chat
}

// senderDisplayName resolves a friendly name for whoever sent the message:
// the name saved in the account's contacts if any, otherwise the sender's own
// push name (broadcast with every message). Returns "" when nothing is known,
// so callers fall back to the phone number. Group senders often arrive as @lid,
// so we try both the raw sender JID and its phone-number alt.
func senderDisplayName(client *whatsmeow.Client, msg *events.Message) string {
	if msg.Info.IsFromMe {
		return "Yo"
	}
	pick := func(c types.ContactInfo) string {
		if c.FullName != "" {
			return c.FullName
		}
		if c.FirstName != "" {
			return c.FirstName
		}
		if c.BusinessName != "" {
			return c.BusinessName
		}
		return c.PushName
	}
	candidates := []types.JID{msg.Info.Sender.ToNonAD()}
	if !msg.Info.SenderAlt.IsEmpty() {
		candidates = append(candidates, msg.Info.SenderAlt.ToNonAD())
	}
	for _, jid := range candidates {
		if c, err := client.Store.Contacts.GetContact(context.Background(), jid); err == nil {
			if n := pick(c); n != "" {
				return n
			}
		}
	}
	return msg.Info.PushName
}

func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database. Canonicalize LID direct chats to the contact's
	// phone-number JID so a contact never fragments into separate @lid and
	// @s.whatsapp.net chats.
	// Normalize away any device/agent suffix (e.g. 1234:73@s.whatsapp.net ->
	// 1234@s.whatsapp.net) so a contact never fragments across device sessions.
	chatObj := canonicalChatJID(client, msg.Info.Chat, msg.Info.SenderAlt, msg.Info.RecipientAlt, msg.Info.IsFromMe, msg.Info.IsGroup, logger).ToNonAD()
	chatJID := chatObj.String()
	sender := msg.Info.Sender.User
	senderName := senderDisplayName(client, msg)

	// Note when the priority peer posts here, so takeover can tell if they answered.
	recordPeerMessage(chatJID, sender, msg.Info.Timestamp)

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, chatObj, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Extract quoted-reply context (if this message replies to another)
	quotedID, quotedSender, quotedContent := extractContextInfo(msg.Message)

	// Reactions arrive as a message carrying a ReactionMessage (no text/media), so
	// they'd be skipped below. Forward them to subscribed sessions with the
	// reacted-to message for context.
	if reaction := msg.Message.GetReactionMessage(); reaction != nil {
		emoji := reaction.GetText()
		desc := "reaccionó " + emoji
		if emoji == "" {
			desc = "quitó su reacción"
		}
		if key := reaction.GetKey(); key != nil {
			if tc := messageStore.getMessageContent(chatJID, key.GetID()); tc != "" {
				r := []rune(tc)
				if len(r) > 60 {
					tc = string(r[:60]) + "…"
				}
				desc += " a: «" + tc + "»"
			}
		}
		if !msg.Info.IsFromMe {
			fmt.Printf("[%s] ← %s: %s\n", msg.Info.Timestamp.Format("2006-01-02 15:04:05"), sender, desc)
		}
		dispatchWebhooks(client, messageStore, WebhookPayload{
			Event:      "reaction",
			MessageID:  msg.Info.ID,
			ChatJID:    chatJID,
			ChatName:   name,
			Sender:     sender,
			SenderName: senderName,
			Content:    desc,
			Timestamp: msg.Info.Timestamp.Format(time.RFC3339),
			IsFromMe:  msg.Info.IsFromMe,
			IsGroup:   msg.Info.IsGroup,
		}, logger)
		return
	}

	// Message edits arrive as a ProtocolMessage (type MESSAGE_EDIT) whose new text
	// lives in GetEditedMessage() and whose target is GetKey(). whatsmeow does NOT
	// unwrap this (UnwrapRaw only handles Message.EditedMessage, a different field),
	// so `content` above is empty and the edit would be skipped below. Forward it to
	// subscribed sessions with the new text, and update the stored original so
	// history/quotes reflect the edit.
	if pm := msg.Message.GetProtocolMessage(); pm != nil && pm.GetType() == waProto.ProtocolMessage_MESSAGE_EDIT {
		targetID := pm.GetKey().GetID()
		newText := extractTextContent(pm.GetEditedMessage())
		if newText == "" {
			return // an edit we can't render as text (e.g. media-only) — nothing to send
		}
		desc := "✏️ editó: " + newText
		if old := messageStore.getMessageContent(chatJID, targetID); old != "" && old != newText {
			r := []rune(old)
			if len(r) > 60 {
				old = string(r[:60]) + "…"
			}
			desc = "✏️ editó «" + old + "» → " + newText
		}
		messageStore.updateMessageContent(chatJID, targetID, newText)
		if !msg.Info.IsFromMe {
			fmt.Printf("[%s] ← %s: %s\n", msg.Info.Timestamp.Format("2006-01-02 15:04:05"), sender, desc)
		}
		dispatchWebhooks(client, messageStore, WebhookPayload{
			Event:      "edit",
			MessageID:  targetID,
			ChatJID:    chatJID,
			ChatName:   name,
			Sender:     sender,
			SenderName: senderName,
			Content:    desc,
			Timestamp:  msg.Info.Timestamp.Format(time.RFC3339),
			IsFromMe:   msg.Info.IsFromMe,
			IsGroup:    msg.Info.IsGroup,
		}, logger)
		return
	}

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
		quotedID,
		quotedSender,
		quotedContent,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}

		// Push this message to any subscribed sessions (async, non-blocking).
		// gatedDispatch applies priority coordination (defer to a peer node that
		// is actively processing this chat); it's a pass-through when off.
		gatedDispatch(client, messageStore, WebhookPayload{
			Event:         "message",
			MessageID:     msg.Info.ID,
			ChatJID:       chatJID,
			ChatName:      name,
			Sender:        sender,
			SenderName:    senderName,
			Content:       content,
			Timestamp:     msg.Info.Timestamp.Format(time.RFC3339),
			IsFromMe:      msg.Info.IsFromMe,
			IsGroup:       msg.Info.IsGroup,
			MediaType:     mediaType,
			Filename:      filename,
			QuotedID:      quotedID,
			QuotedContent: quotedContent,
		}, logger)
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// Resolve the actual stored chat_jid for this message. WhatsApp stores
	// messages under either the bare JID (e.g. 1234@s.whatsapp.net) or a
	// device-suffixed JID (e.g. 1234:47@s.whatsapp.net) depending on the
	// sending session. Callers may pass either variant, so if the exact
	// (id, chat_jid) pair isn't present, fall back to matching by message ID
	// alone so downloads work for any chat regardless of JID variant.
	var resolvedJID string
	if scanErr := messageStore.db.QueryRow(
		"SELECT chat_jid FROM messages WHERE id = ? AND chat_jid = ?",
		messageID, chatJID,
	).Scan(&resolvedJID); scanErr != nil {
		if scanErr2 := messageStore.db.QueryRow(
			"SELECT chat_jid FROM messages WHERE id = ? LIMIT 1",
			messageID,
		).Scan(&resolvedJID); scanErr2 == nil && resolvedJID != "" {
			chatJID = resolvedJID
		}
	}

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// whatsmeow's DownloadMediaWithPath rebuilds the CDN URL from the direct
	// path (re-resolving a fresh host) and then appends &hash=&mms-type=. So the
	// direct path must KEEP its query (ccb, _nc_sid, mms3, …) — but must NOT keep
	// the host-specific auth params (oh/oe), which are only valid for the
	// original host and otherwise cause a 403 on the freshly-resolved host.
	// Example: https://mmg.whatsapp.net/v/t62.../..._n.enc?ccb=11-4&oh=..&oe=..&_nc_sid=..&mms3=true
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathAndQuery := parts[1]
	path := pathAndQuery
	query := ""
	if i := strings.IndexByte(pathAndQuery, '?'); i >= 0 {
		path = pathAndQuery[:i]
		var kept []string
		for _, p := range strings.Split(pathAndQuery[i+1:], "&") {
			if p == "" {
				continue
			}
			key := p
			if eq := strings.IndexByte(p, '='); eq >= 0 {
				key = p[:eq]
			}
			switch key {
			case "hash", "mms-type", "__wa-mms":
				// params whatsmeow appends itself; keep everything else
				// (ccb, oh, oe, _nc_sid, mms3 …) — oh/oe are the file's
				// signed auth/expiry and are required to avoid a 403.
				continue
			}
			kept = append(kept, p)
		}
		query = strings.Join(kept, "&")
	}

	directPath := "/" + path
	if query != "" {
		directPath += "?" + query
	}
	return directPath
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) {
	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath, req.ReplyTo, req.ReplyParticipant, req.ReplyText, req.Mentions)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for listing a group's members (so a session can resolve a member's
	// NAME to the number it needs to @-mention them). GET /api/group-members?jid=<group JID>
	http.HandleFunc("/api/group-members", func(w http.ResponseWriter, r *http.Request) {
		jidStr := r.URL.Query().Get("jid")
		if jidStr == "" {
			http.Error(w, "jid query param is required", http.StatusBadRequest)
			return
		}
		groupJID, err := types.ParseJID(jidStr)
		if err != nil {
			http.Error(w, "invalid jid", http.StatusBadRequest)
			return
		}
		info, err := client.GetGroupInfo(context.Background(), groupJID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "error": err.Error()})
			return
		}
		type member struct {
			JID     string `json:"jid"`
			Phone   string `json:"phone"`   // the number to write as "@<phone>" to mention
			Name    string `json:"name"`
			IsAdmin bool   `json:"is_admin"`
		}
		members := make([]member, 0, len(info.Participants))
		for _, p := range info.Participants {
			// Prefer the real phone number; fall back to the JID user part.
			phone := p.PhoneNumber.User
			if phone == "" {
				phone = p.JID.User
			}
			name := p.DisplayName
			if name == "" {
				if c, err := client.Store.Contacts.GetContact(context.Background(), p.JID); err == nil {
					switch {
					case c.FullName != "":
						name = c.FullName
					case c.PushName != "":
						name = c.PushName
					case c.FirstName != "":
						name = c.FirstName
					case c.BusinessName != "":
						name = c.BusinessName
					}
				}
			}
			members = append(members, member{
				JID:     p.JID.String(),
				Phone:   phone,
				Name:    name,
				IsAdmin: p.IsAdmin || p.IsSuperAdmin,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"group":   info.Name,
			"members": members,
		})
	})

	// Handler for adding/removing/promoting/demoting group members.
	// POST /api/group-participants
	//   { "group_jid":"…@g.us", "participants":["<number or jid>",…], "action":"add|remove|promote|demote" }
	// Requires that this account is an admin of the group (WhatsApp enforces it
	// server-side). Returns a per-participant result so the caller sees who
	// succeeded vs. who failed (e.g. not on WhatsApp, privacy-blocked).
	http.HandleFunc("/api/group-participants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req GroupParticipantsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if req.GroupJID == "" || len(req.Participants) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "group_jid and participants are required"})
			return
		}

		var action whatsmeow.ParticipantChange
		switch req.Action {
		case "add":
			action = whatsmeow.ParticipantChangeAdd
		case "remove":
			action = whatsmeow.ParticipantChangeRemove
		case "promote":
			action = whatsmeow.ParticipantChangePromote
		case "demote":
			action = whatsmeow.ParticipantChangeDemote
		default:
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "action must be add, remove, promote or demote"})
			return
		}

		groupJID, err := types.ParseJID(req.GroupJID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "invalid group_jid"})
			return
		}

		// A value with "@" is a JID (verbatim, so "<lid>@lid" passes through); a
		// bare value is normalized to digits -> "<number>@s.whatsapp.net". For ADD,
		// prefer the phone form (resolution-free); for REMOVE in LID groups pass the
		// member's jid from /api/group-members.
		participantJIDs := make([]types.JID, 0, len(req.Participants))
		for _, p := range req.Participants {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if strings.Contains(p, "@") {
				pj, perr := types.ParseJID(p)
				if perr != nil {
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(map[string]any{"success": false, "error": fmt.Sprintf("invalid participant %q: %v", p, perr)})
					return
				}
				participantJIDs = append(participantJIDs, pj)
			} else if n := onlyDigits(p); n != "" {
				participantJIDs = append(participantJIDs, types.JID{User: n, Server: "s.whatsapp.net"})
			}
		}
		if len(participantJIDs) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "no valid participants"})
			return
		}

		updated, err := client.UpdateGroupParticipants(context.Background(), groupJID, participantJIDs, action)
		if err != nil {
			// Whole-call failure: usually not-admin (403), not-logged-in, or network.
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"success": false, "error": err.Error()})
			return
		}

		// Per-participant outcome: Error==0 is success; non-zero is that member's
		// server code (408 not on WhatsApp, 409 already in, 403 privacy, 401 blocked).
		// A 403 with an AddRequest means a direct add was refused and an invite must
		// be sent instead — surface its code so the caller knows.
		type ppResult struct {
			JID        string `json:"jid"`
			Phone      string `json:"phone"`
			Success    bool   `json:"success"`
			Code       int    `json:"code"`
			InviteCode string `json:"invite_code,omitempty"`
		}
		results := make([]ppResult, 0, len(updated))
		for _, p := range updated {
			phone := p.PhoneNumber.User
			if phone == "" {
				phone = p.JID.User
			}
			res := ppResult{
				JID:     p.JID.String(),
				Phone:   phone,
				Success: p.Error == 0,
				Code:    p.Error,
			}
			if p.AddRequest != nil {
				res.InviteCode = p.AddRequest.Code
			}
			results = append(results, res)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"action":  req.Action,
			"group":   groupJID.String(),
			"results": results,
		})
	})

	// Handler for downloading media
	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Handler for reacting to a message with an emoji
	http.HandleFunc("/api/react", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req ReactionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if req.Recipient == "" || req.MessageID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: "recipient and message_id are required"})
			return
		}
		chatJID, err := parseRecipientJID(req.Recipient)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: fmt.Sprintf("Invalid recipient: %v", err)})
			return
		}
		senderJID := chatJID
		if req.Sender != "" {
			if sj, e := types.ParseJID(req.Sender); e == nil {
				senderJID = sj
			}
		}
		reaction := client.BuildReaction(chatJID, senderJID, types.MessageID(req.MessageID), req.Emoji)
		if _, err = client.SendMessage(context.Background(), chatJID, reaction); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: err.Error()})
			return
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: true, Message: "Reaction sent"})
	})

	// Handler for marking a message as read
	http.HandleFunc("/api/markread", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req MarkReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if req.Recipient == "" || req.MessageID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: "recipient and message_id are required"})
			return
		}
		chatJID, err := parseRecipientJID(req.Recipient)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: fmt.Sprintf("Invalid recipient: %v", err)})
			return
		}
		senderJID := chatJID
		if req.Sender != "" {
			if sj, e := types.ParseJID(req.Sender); e == nil {
				senderJID = sj
			}
		}
		if err = client.MarkRead(context.Background(), []types.MessageID{types.MessageID(req.MessageID)}, time.Now(), chatJID, senderJID); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: err.Error()})
			return
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: true, Message: "Marked as read"})
	})

	// Handler for presence (typing / recording indicator)
	http.HandleFunc("/api/presence", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req PresenceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if req.Recipient == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: "recipient is required"})
			return
		}
		chatJID, err := parseRecipientJID(req.Recipient)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: fmt.Sprintf("Invalid recipient: %v", err)})
			return
		}
		state := types.ChatPresenceComposing
		media := types.ChatPresenceMediaText
		switch req.State {
		case "", "composing":
			state = types.ChatPresenceComposing
		case "recording":
			state = types.ChatPresenceComposing
			media = types.ChatPresenceMediaAudio
		case "paused":
			state = types.ChatPresencePaused
		default:
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: "state must be composing, recording or paused"})
			return
		}
		// Must be marked available to send chat presence.
		_ = client.SendPresence(context.Background(), types.PresenceAvailable)
		if err = client.SendChatPresence(context.Background(), chatJID, state, media); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(SendMessageResponse{Success: false, Message: err.Error()})
			return
		}
		json.NewEncoder(w).Encode(SendMessageResponse{Success: true, Message: "Presence sent"})
	})

	// Handler for webhook subscriptions (push new incoming messages to sessions)
	http.HandleFunc("/api/webhooks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			hooks, err := messageStore.ListWebhooks()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "webhooks": hooks})
		case http.MethodPost:
			var req struct {
				ChatJID    string `json:"chat_jid"`
				Kind       string `json:"kind"`
				Target     string `json:"target"`
				IncludeOwn bool   `json:"include_own"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "invalid request format"})
				return
			}
			if req.Kind == "" {
				req.Kind = "http"
			}
			if req.ChatJID == "" || req.Target == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "chat_jid and target are required"})
				return
			}
			if req.Kind != "http" && req.Kind != "cmux" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "kind must be 'http' or 'cmux'"})
				return
			}
			if err := messageStore.AddWebhook(req.ChatJID, req.Kind, req.Target, req.IncludeOwn); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "message": "subscribed"})
		case http.MethodDelete:
			var req struct {
				ChatJID string `json:"chat_jid"`
				Kind    string `json:"kind"`
				Target  string `json:"target"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "invalid request format"})
				return
			}
			if req.Kind == "" {
				req.Kind = "http"
			}
			n, err := messageStore.DeleteWebhook(req.ChatJID, req.Kind, req.Target)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "deleted": n})
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// cmux delivery queue, drained by the in-session relay (cmux-relay.sh).
	//   GET  /api/cmux-outbox        -> { success, items:[{id,surface,notice}] }
	//   POST /api/cmux-outbox/ack    { "ids":[1,2] } -> deletes delivered rows
	http.HandleFunc("/api/cmux-outbox", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		items, err := messageStore.ListCmuxOutbox(200)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		if items == nil {
			items = []OutboxItem{}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "items": items})
	})

	http.HandleFunc("/api/cmux-outbox/ack", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			IDs []int64 `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "invalid JSON"})
			return
		}
		if err := messageStore.AckCmuxOutbox(req.IDs); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "acked": len(req.IDs)})
	})

	// Start the server
	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := http.ListenAndServe(serverAddr, nil); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

func main() {
	loadPresenceConfig()
	// Single-instance guard: if another bridge already holds the REST port,
	// exit immediately WITHOUT connecting to WhatsApp. Two bridges sharing the
	// same linked-device session trigger a "stream replaced" war that
	// repeatedly drops the connection (sends fail with "Not connected").
	if ln, err := net.Listen("tcp", ":8080"); err != nil {
		fmt.Println("Another wabridge instance is already running on :8080 — exiting.")
		os.Exit(0)
	} else {
		ln.Close()
	}

	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_foreign_keys=on", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}

		// Print QR code for pairing with phone
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				// Also write a scannable PNG of the current QR so it can be shared/displayed.
				if qrCode, qrErr := qr.Encode(evt.Code, qr.M); qrErr == nil {
					if wErr := os.WriteFile("store/pair_qr.png", qrCode.PNG(), 0644); wErr != nil {
						fmt.Printf("Failed to write QR PNG: %v\n", wErr)
					} else {
						fmt.Println("QR_PNG_WRITTEN:store/pair_qr.png")
					}
				}
			} else if evt.Event == "success" {
				connected <- true
				break
			}
		}

		// Wait for connection
		select {
		case <-connected:
			fmt.Println("\nSuccessfully connected and authenticated!")
		case <-time.After(3 * time.Minute):
			logger.Errorf("Timeout waiting for QR code scan")
			return
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Keep the downloaded-media store under the 20GB cap (oldest deleted first).
	startStoreCleaner()

	// Start REST API server
	startRESTServer(client, messageStore, 8080)

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Canonicalize LID direct chats to the contact's phone-number JID so
		// history doesn't fragment into separate @lid and @s.whatsapp.net chats.
		if jid.Server == types.HiddenUserServer {
			if pn, perr := client.Store.LIDs.GetPNForLID(context.Background(), jid); perr == nil && !pn.IsEmpty() && pn.Server == types.DefaultUserServer {
				jid = pn
				chatJID = pn.String()
			}
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content (incl. media captions)
				var content string
				if msg.Message.Message != nil {
					content = extractTextContent(msg.Message.Message)
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				// Extract quoted-reply context
				var quotedID, quotedSender, quotedContent string

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
					quotedID, quotedSender, quotedContent = extractContextInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
					quotedID,
					quotedSender,
					quotedContent,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// Request history sync from the server
func requestHistorySync(client *whatsmeow.Client) {
	if client == nil {
		fmt.Println("Client is not initialized. Cannot request history sync.")
		return
	}

	if !client.IsConnected() {
		fmt.Println("Client is not connected. Please ensure you are connected to WhatsApp first.")
		return
	}

	if client.Store.ID == nil {
		fmt.Println("Client is not logged in. Please scan the QR code first.")
		return
	}

	// Build and send a history sync request
	historyMsg := client.BuildHistorySyncRequest(nil, 100)
	if historyMsg == nil {
		fmt.Println("Failed to build history sync request.")
		return
	}

	_, err := client.SendMessage(context.Background(), types.JID{
		Server: "s.whatsapp.net",
		User:   "status",
	}, historyMsg)

	if err != nil {
		fmt.Printf("Failed to request history sync: %v\n", err)
	} else {
		fmt.Println("History sync requested. Waiting for server response...")
	}
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}
