#!/usr/bin/env python3
"""WhatsApp -> cmux delivery relay.

The whatsapp bridge runs as a background daemon and therefore CANNOT run
`cmux send` itself: cmux only accepts socket writes from a process that is a
live descendant of the cmux app (i.e. running inside a cmux terminal surface).
So the bridge queues cmux deliveries in its SQLite outbox and exposes them over
HTTP; THIS relay — which must run inside a cmux surface — drains the queue and
performs the actual `cmux send`.

IMPORTANT — deliver only when Claude is actually running in the target surface:
`cmux send` pastes text + presses Enter regardless of what occupies the surface.
If Claude isn't running (the surface is at a bare zsh prompt), the notice would
be typed into the shell and executed as a command — e.g. the `[12345@g.us]`
fragment triggers `zsh: no matches found`. So before delivering we detect whether
Claude Code's TUI is live in the surface by reading its ACTUAL on-screen content
(`cmux read-screen`). We do NOT trust cmux's reported tty for this: that field is
stale/unreliable (it can point at an old shell while Claude runs on another pty),
which previously made every session look idle and wrongly held all messages.
If Claude isn't detected, we HOLD the item (never ack it) so it stays queued and
is delivered the moment Claude is (re)opened in that surface.

SHABBAT — messages that arrive during Shabbat (candle-lighting Friday .. Havdalah
Saturday night, computed for Mexico City via Hebcal) are HELD, exactly like the
Claude-not-open case: they stay queued and flush automatically once Shabbat ends.
On the first held message per surface we send a one-time heads-up so the session
knows it's Shabbat and messages are being saved until Havdalah. This lives ONLY
in the relay — the bridge and the MCP server are untouched.

Run it in a dedicated cmux terminal tab and keep that tab open:
    python3 cmux-relay.py
"""
import datetime
import json
import re
import ssl
import subprocess
import time
import urllib.request

BRIDGE = "http://localhost:8080/api"
CMUX = "/Applications/cmux.app/Contents/Resources/bin/cmux"
POLL_SECONDS = 1.5
MAX_ATTEMPTS = 4       # drop after this many real send FAILURES (not holds)
MISSING_MAX = 40       # drop after this many cycles the surface is gone from the tree

# Hebcal Shabbat times for Mexico City (geonameid 3530597): candle-lighting 18 min
# before sunset (b=18), Havdalah at nightfall (M=on). Returns tz-aware ISO times.
SHABBAT_URL = ("https://www.hebcal.com/shabbat?cfg=json&geonameid=3530597"
               "&b=18&M=on")
SHABBAT_FETCH_THROTTLE = 300  # seconds between fetch attempts when we lack a window

_attempts = {}  # outbox id -> failed send attempts so far
_missing = {}   # outbox id -> cycles the target surface has been absent from cmux
_shabbat = {"start": None, "end": None, "last_try": None}  # cached Shabbat window
_shabbat_announced = set()  # surfaces already told "it's Shabbat" this window


def get_outbox():
    try:
        with urllib.request.urlopen(BRIDGE + "/cmux-outbox", timeout=5) as r:
            data = json.load(r)
        return data.get("items", []) if data.get("success") else []
    except Exception:
        return []


def ack(ids):
    if not ids:
        return
    body = json.dumps({"ids": ids}).encode()
    req = urllib.request.Request(
        BRIDGE + "/cmux-outbox/ack", data=body,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as e:
        print("[relay] ack failed:", e, flush=True)


_MODEL_TAG = re.compile(r"\[(Opus|Sonnet|Haiku|Claude)\b")


def claude_running(surface):
    """Detect whether Claude Code's TUI is live in this surface.

    Returns:
      True  -> Claude is running (safe to deliver)
      False -> a bare shell / other program is on screen (HOLD)
      None  -> the surface can't be read (gone or a transient cmux hiccup)

    Detection reads the surface's ACTUAL visible content, because cmux's reported
    tty is unreliable. Claude Code's footer carries distinctive markers: the mode
    line (`⏵⏵`), the `Context …/ Usage …` gauges, and a `[Opus|Sonnet|Haiku|
    Claude …]` model tag. A plain zsh prompt has none of these."""
    try:
        out = subprocess.run(
            [CMUX, "read-screen", "--surface", surface, "--lines", "40"],
            capture_output=True, text=True, timeout=8)
    except Exception:
        return None
    if out.returncode != 0:
        return None
    s = out.stdout
    if "⏵⏵" in s:  # ⏵⏵ mode line
        return True
    if "Context" in s and "Usage" in s:  # the usage gauges
        return True
    if _MODEL_TAG.search(s):  # [Opus 4.8] etc.
        return True
    return False


def _refresh_shabbat_window():
    """Fetch the current/upcoming Shabbat window (candle-lighting, Havdalah) from
    Hebcal for Mexico City. Returns (start, end) tz-aware datetimes, or None."""
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(SHABBAT_URL, timeout=15, context=ctx) as r:
            d = json.load(r)
    except Exception as e:
        print("[relay] hebcal fetch failed:", e, flush=True)
        return None
    start = end = None
    for it in d.get("items", []):
        cat, date = it.get("category"), it.get("date")
        if not date:
            continue
        try:
            when = datetime.datetime.fromisoformat(date)  # e.g. 2026-07-17T18:59:00-06:00
        except Exception:
            continue
        if cat == "candles":
            start = when
        elif cat == "havdalah":
            end = when
    if start and end:
        return (start, end)
    return None


def is_shabbat():
    """True if now is within the Shabbat window for Mexico City. Caches the window
    and only re-fetches once Havdalah has passed (throttled), so Hebcal is hit
    roughly once a week. Fails OPEN (False) on any error — a network blip must
    never silently hold messages forever."""
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        start, end = _shabbat["start"], _shabbat["end"]
        if end is None or now > end:  # no window yet, or the cached one has passed
            last = _shabbat["last_try"]
            if last is None or (now - last).total_seconds() > SHABBAT_FETCH_THROTTLE:
                _shabbat["last_try"] = now
                w = _refresh_shabbat_window()
                if w:
                    _shabbat["start"], _shabbat["end"] = w
                    start, end = w
        if start and end:
            return start <= now <= end
        return False
    except Exception as e:
        print("[relay] shabbat check error:", e, flush=True)
        return False


def shabbat_end_label():
    end = _shabbat.get("end")
    if not end:
        return "el anochecer del sábado"
    try:
        return end.astimezone().strftime("%a %H:%M")  # local (Mexico City) time
    except Exception:
        return "el anochecer del sábado"


def flatten(notice):
    # cmux `send` treats \n, \r and \t as key presses (Enter/Tab). WhatsApp
    # messages are often multi-line, so embedded newlines would submit the prompt
    # early and leave the rest stuck in the input. Collapse the notice to a single
    # line and neutralize backslash-escapes in the content so ONLY our final Enter
    # submits the whole message.
    text = notice.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    text = " ".join(part.strip() for part in text.split("\n") if part.strip())
    text = text.replace("\\", "\\\\")  # a literal '\' in the message must not become an escape
    return text


def send(surface, notice):
    # Two-step: (1) `send` pastes the flattened text into the prompt, (2) `send-key
    # enter` presses a DISCRETE Enter. A trailing "\n" inside `send` is delivered as
    # part of the paste and Claude's TUI treats it as a soft newline (never submits),
    # so the message would sit stuck in the input. A real Enter key press submits /
    # queues it, exactly like a human pressing Enter (works mid-turn too).
    try:
        p1 = subprocess.run(
            [CMUX, "send", "--surface", surface, "--", flatten(notice)],
            capture_output=True, text=True, timeout=15)
        if p1.returncode != 0:
            return False, "send: " + (p1.stdout + p1.stderr).strip()
        time.sleep(0.35)  # let the paste land before submitting
        p2 = subprocess.run(
            [CMUX, "send-key", "--surface", surface, "enter"],
            capture_output=True, text=True, timeout=15)
        return p2.returncode == 0, "send-key: " + (p2.stdout + p2.stderr).strip()
    except Exception as e:
        return False, str(e)


def main():
    print("cmux-relay: draining WhatsApp cmux deliveries from", BRIDGE, flush=True)
    was_shabbat = False
    while True:
        items = get_outbox()
        shab = is_shabbat()
        if not shab and was_shabbat:
            # Shabbat just ended — allow a fresh heads-up next Shabbat and let the
            # accumulated backlog flush through the normal path below.
            _shabbat_announced.clear()
            print("[relay] Shabbat ended — flushing held messages", flush=True)
        was_shabbat = shab
        done = []
        held = 0
        state_cache = {}  # surface -> True/False/None, computed once per cycle
        for it in items:
            iid, surface, notice = it["id"], it["surface"], it["notice"]
            key = str(surface)
            if key not in state_cache:
                state_cache[key] = claude_running(surface)
            state = state_cache[key]

            if state is None:
                # Surface unreadable: probably closed, possibly a transient cmux
                # hiccup. Hold a while, then drop so a dead subscription can't
                # grow the queue unbounded.
                _missing[iid] = _missing.get(iid, 0) + 1
                if _missing[iid] >= MISSING_MAX:
                    print(f"[relay] dropping #{iid} (surface {surface} gone)", flush=True)
                    done.append(iid)
                    _missing.pop(iid, None)
                    _attempts.pop(iid, None)
                else:
                    held += 1
                continue
            _missing.pop(iid, None)

            if state is False:
                # Claude isn't open here (bare shell / other program) -> HOLD.
                # Delivered as soon as Claude is (re)opened in this surface.
                held += 1
                continue

            if shab:
                # Shabbat: HOLD every message until Havdalah. On the first held
                # message for a surface (with Claude open), send a one-time notice
                # so the session knows it's Shabbat and messages are being saved.
                if key not in _shabbat_announced:
                    ann = ("\U0001F56F️ Shabat — Es Shabat ahora. Guardo los mensajes "
                           "de WhatsApp y te los envio todos juntos al terminar Shabat "
                           "(~%s). No respondas por WhatsApp hasta entonces."
                           % shabbat_end_label())
                    ok, _ = send(surface, ann)
                    if ok:
                        _shabbat_announced.add(key)
                        print(f"[relay] Shabbat notice -> {surface}", flush=True)
                held += 1
                continue

            ok, msg = send(surface, notice)
            if ok:
                done.append(iid)
                _attempts.pop(iid, None)
                print(f"[relay] delivered #{iid} -> {surface}", flush=True)
            else:
                _attempts[iid] = _attempts.get(iid, 0) + 1
                print(f"[relay] #{iid} -> {surface} failed "
                      f"(try {_attempts[iid]}/{MAX_ATTEMPTS}): {msg}", flush=True)
                if _attempts[iid] >= MAX_ATTEMPTS:
                    print(f"[relay] dropping #{iid} (send keeps failing)", flush=True)
                    done.append(iid)
                    _attempts.pop(iid, None)
        ack(done)
        if held:
            reason = ("Shabbat, hasta ~%s" % shabbat_end_label()) if shab \
                else "Claude not open in target surface(s)"
            print(f"[relay] holding {held} msg(s) — {reason}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
