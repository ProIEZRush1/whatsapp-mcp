#!/usr/bin/env python3
"""cmux WhatsApp self-heal daemon.

Fixes the two failures that happen every time cmux restarts:
  1. The WhatsApp->cmux relay dies  -> ensure the wa-relay workspace is running.
  2. Surface IDs get reassigned, so cmux subscriptions (webhooks table) point at
     dead surfaces and nothing is delivered -> re-point dead subscriptions to the
     SAME workspace's current surface (workspace UUIDs are stable across restarts).

Conservative by design:
  - Only ever touches kind='cmux' webhooks whose target surface is DEAD.
  - Re-points to a surface of the workspace that dead surface belonged to
    (learned while it was alive), preferring same-title, then focused/selected.
  - Prunes a dead subscription only when its workspace no longer exists.
  - Never invents new (chat, workspace) links. Live subscriptions are untouched.

Run:  python3 cmux-wa-heal.py [--dry-run]
"""
import json
import os
import subprocess
import sqlite3
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "store", "messages.db")
STATE_FILE = os.path.join(HERE, "cmux-wa-heal-state.json")
LOG_FILE = os.path.join(HERE, "cmux-wa-heal.log")
START_RELAY = os.path.join(HERE, "start-relay.sh")
CMUX = "/Applications/cmux.app/Contents/Resources/bin/cmux"
DRY = "--dry-run" in sys.argv


def log(msg):
    line = "[%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line)
    try:
        # keep the log bounded (~1 MB) without external rotation
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 1_000_000:
            with open(LOG_FILE) as f:
                tail = f.readlines()[-2000:]
            with open(LOG_FILE, "w") as f:
                f.writelines(tail)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"surface_ws": {}}


def save_state(state):
    if DRY:
        return
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=0)
    os.replace(tmp, STATE_FILE)


def ensure_socket_env():
    """cmux CLI locates the daemon via $CMUX_SOCKET_PATH. launchd doesn't inherit
    it, so discover the current socket and export it for child cmux processes."""
    import glob
    p = os.environ.get("CMUX_SOCKET_PATH")
    if p and os.path.exists(p):
        return p
    cands = (glob.glob(os.path.expanduser("~/.local/state/cmux/cmux-*.sock"))
             + glob.glob(os.path.expanduser("~/.local/state/cmux/cmux.sock"))
             + glob.glob(os.path.expanduser("~/Library/Application Support/cmux/*.sock")))
    cands = [c for c in cands if os.path.exists(c)]
    if not cands:
        return None
    pref = [c for c in cands if ("cmux-%d.sock" % os.getuid()) in c]
    chosen = (pref or sorted(cands, key=os.path.getmtime, reverse=True))[0]
    os.environ["CMUX_SOCKET_PATH"] = chosen
    return chosen


def cmux_tree():
    """Return the cmux tree JSON, or None if cmux is unreachable."""
    sock = ensure_socket_env()
    try:
        out = subprocess.run([CMUX, "--json", "tree", "--all", "--id-format", "both"],
                             capture_output=True, text=True, timeout=15)
        if out.returncode != 0:
            log("cmux tree rc=%s sock=%s err=%r" % (out.returncode, sock, (out.stderr or "")[:200]))
            return None
        return json.loads(out.stdout)
    except Exception as e:
        log("cmux tree exception sock=%s: %r" % (sock, e))
        return None


def build_model(tree):
    """live_surfaces:set, surf_ws:{sid:wsid}, ws:{wsid:{title,surfaces:[...]}}"""
    live, surf_ws, ws = set(), {}, {}
    for window in tree.get("windows", []):
        for w in window.get("workspaces", []):
            wsid = w.get("id")
            if not wsid:
                continue
            entry = ws.setdefault(wsid, {"title": w.get("title"), "surfaces": []})
            for pane in w.get("panes", []):
                for s in pane.get("surfaces", []):
                    sid = s.get("id")
                    if not sid:
                        continue
                    live.add(sid)
                    surf_ws[sid] = wsid
                    entry["surfaces"].append({
                        "id": sid,
                        "title": s.get("title"),
                        "focused": bool(s.get("focused")),
                        "selected": bool(s.get("selected")),
                        "type": s.get("type"),
                        "tty": s.get("tty"),
                    })
    return live, surf_ws, ws


def pick_surface(ws_entry, want_title=None):
    """Choose the best current surface for a workspace: same title > focused >
    selected > first terminal."""
    surfaces = [s for s in ws_entry["surfaces"] if s.get("type") == "terminal"] or ws_entry["surfaces"]
    if not surfaces:
        return None
    if want_title:
        for s in surfaces:
            if s.get("title") == want_title:
                return s["id"]
    for key in ("focused", "selected"):
        for s in surfaces:
            if s.get(key):
                return s["id"]
    return surfaces[0]["id"]


def read_cmux_webhooks():
    con = sqlite3.connect(DB, timeout=5)
    try:
        con.execute("PRAGMA busy_timeout=4000;")
        return con.execute("SELECT chat_jid, target FROM webhooks WHERE kind='cmux'").fetchall()
    finally:
        con.close()


def apply_sql(statements):
    if DRY or not statements:
        return
    con = sqlite3.connect(DB, timeout=5)
    try:
        con.execute("PRAGMA busy_timeout=4000;")
        for sql, params in statements:
            con.execute(sql, params)
        con.commit()
    finally:
        con.close()


def ensure_relay(ws):
    """Make sure the wa-relay workspace exists; if not, (re)create it."""
    for wsid, entry in ws.items():
        if entry.get("title") == "wa-relay":
            return "ok"
    log("relay: wa-relay workspace MISSING -> %s start-relay.sh" % ("WOULD run" if DRY else "running"))
    if DRY:
        return "would-start"
    try:
        subprocess.run(["/bin/bash", START_RELAY], capture_output=True, text=True, timeout=30)
        return "started"
    except Exception as e:
        log("relay: failed to start: %s" % e)
        return "error"


def reconcile():
    if not os.path.exists(DB):
        log("DB not found: %s" % DB)
        return
    tree = cmux_tree()
    if tree is None:
        log("cmux unreachable; skipping this cycle")
        return
    live, surf_ws, ws = build_model(tree)
    state = load_state()
    smap = state.setdefault("surface_ws", {})
    streak = state.setdefault("dead_streak", {})
    last_live = int(state.get("last_live_count", 0))

    # Transient guard: right after a cmux restart the tree is incomplete. If the
    # live-surface count collapsed vs. last healthy cycle, skip acting this cycle
    # (still learn nothing destructive) to avoid pruning/repointing on partial data.
    REPOINT_AFTER = 2   # consecutive dead cycles before re-pointing
    PRUNE_AFTER = 4     # consecutive dead cycles before deleting an orphan
    partial = last_live and len(live) < max(10, last_live * 0.5)
    if partial:
        log("tree looks partial (live=%d, last=%d) -> observe only, no changes" % (len(live), last_live))

    # LEARN: record workspace + title for every live surface (stable wsid).
    for sid, wsid in surf_ws.items():
        title = None
        for s in ws[wsid]["surfaces"]:
            if s["id"] == sid:
                title = s["title"]
                break
        smap[sid] = {"ws": wsid, "ws_title": ws[wsid]["title"], "surf_title": title}

    webhooks = read_cmux_webhooks()
    stmts = []
    repointed = pruned = 0
    seen_targets = set()
    for chat, target in webhooks:
        seen_targets.add(target)
        if target in live:
            streak.pop(target, None)  # healthy -> reset dead counter
            continue
        streak[target] = int(streak.get(target, 0)) + 1
        if partial:
            continue  # don't act on incomplete tree
        n = streak[target]
        info = smap.get(target)
        wsid = info.get("ws") if info else None
        if wsid and wsid in ws:
            newsurf = pick_surface(ws[wsid], want_title=(info or {}).get("surf_title"))
            if newsurf and newsurf != target:
                if n < REPOINT_AFTER:
                    log("watch %s: dead %s (ws '%s') streak=%d/%d" % (chat, target[:8], ws[wsid]["title"], n, REPOINT_AFTER))
                    continue
                log("HEAL %s: dead %s (ws '%s') -> %s" % (chat, target[:8], ws[wsid]["title"], newsurf[:8]))
                stmts.append(("INSERT OR IGNORE INTO webhooks(chat_jid,kind,target,include_own) VALUES(?, 'cmux', ?, 0)", (chat, newsurf)))
                stmts.append(("DELETE FROM webhooks WHERE chat_jid=? AND kind='cmux' AND target=?", (chat, target)))
                repointed += 1
                streak.pop(target, None)
            # else: workspace's only surface is the (dead) target itself -> leave
        else:
            # workspace gone or never learned -> dead row can never deliver.
            if n < PRUNE_AFTER:
                log("watch %s: dead %s (unknown ws) streak=%d/%d" % (chat, target[:8], n, PRUNE_AFTER))
                continue
            log("PRUNE %s: dead %s (workspace gone/unknown)" % (chat, target[:8]))
            stmts.append(("DELETE FROM webhooks WHERE chat_jid=? AND kind='cmux' AND target=?", (chat, target)))
            pruned += 1
            streak.pop(target, None)

    # forget streaks for targets no longer subscribed
    for t in list(streak.keys()):
        if t not in seen_targets:
            streak.pop(t, None)

    apply_sql(stmts)
    if not partial:
        state["last_live_count"] = len(live)
    save_state(state)
    relay = "skipped" if partial else ensure_relay(ws)
    log("cycle done%s: repointed=%d pruned=%d relay=%s live_surfaces=%d subs=%d"
        % (" (DRY)" if DRY else "", repointed, pruned, relay, len(live), len(webhooks)))


if __name__ == "__main__":
    reconcile()
