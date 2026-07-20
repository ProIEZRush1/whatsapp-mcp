#!/usr/bin/env python3
"""Transcribe a WhatsApp audio file with the local, free, offline whisper model.

Usage: transcribe.py <audio_path>
Prints the transcription text to stdout (empty output + non-zero exit on failure).
The bridge calls this once per audio (dedup) before fanning a chat's notice out to
subscribed sessions, so sessions receive the text directly.
"""
import os
import sys
import warnings

warnings.filterwarnings("ignore")

# Whisper decodes audio by shelling out to `ffmpeg`. The bridge runs this script
# with a minimal launchd PATH (/usr/bin:/bin) that lacks Homebrew, so ffmpeg
# ('/opt/homebrew/bin' on Apple Silicon, '/usr/local/bin' on Intel) is invisible
# and transcription fails with "No such file or directory: 'ffmpeg'". Prepend the
# usual install dirs so ffmpeg is found no matter how we're launched.
_path = os.environ.get("PATH", "")
for _p in ("/opt/homebrew/bin", "/usr/local/bin"):
    if os.path.isdir(_p) and _p not in _path.split(os.pathsep):
        _path = _p + os.pathsep + _path
os.environ["PATH"] = _path

MODEL = "medium"  # cached local model (~/.cache/whisper/medium.pt); free + offline

def main() -> int:
    if len(sys.argv) < 2:
        return 2
    audio = sys.argv[1]
    try:
        import whisper
        model = whisper.load_model(MODEL, device="cpu")
        # language left to auto-detect if not Spanish; these chats are ES so hint it
        result = model.transcribe(audio, language="es", fp16=False)
        text = (result.get("text") or "").strip()
        sys.stdout.write(text)
        return 0
    except Exception as e:  # noqa: BLE001
        sys.stderr.write("transcribe error: %s" % e)
        return 1

if __name__ == "__main__":
    sys.exit(main())
