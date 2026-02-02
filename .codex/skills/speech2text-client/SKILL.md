---
name: speech2text-client
description: 'Use a Speech2Text (speech-to-text) service to transcribe audio/video: register/login (JWT via x-jwt-token), upload files (/tasks/add), poll status (/tasks/{id}), fetch text results (/results/txt), and troubleshoot common errors. Triggered by requests like "speech2text", "Speech2Text", "распознавание речи", "расшифруй/транскрибируй аудио/видео/голосовое", "получи транскрипцию из tg_uploads", "как получить токен/логин", "curl пример для API".'
---

# speech2text-client

## Quick start

- Swagger/OpenAPI: `${SPEECH2TEXT_BASE_URL}/openapi.json`
- UI (optional): (depends on your deployment)
- CLI helper in this repo: `python3 scripts/speech2text.py`

Check connectivity:

```bash
python3 scripts/speech2text.py doctor
```

## Auth (register/login → token)

- API uses JWT in header `x-jwt-token`.
- Store token outside the repo (default): `~/.config/speech2text/token` (mode `600`).

If you already have an account:

```bash
python3 scripts/speech2text.py login --username "YOUR_LOGIN"
```

If you don’t have an account yet (one-time):

```bash
python3 scripts/speech2text.py register --user-name "YOUR_LOGIN"
python3 scripts/speech2text.py login --username "YOUR_LOGIN"
```

Alternative: provide token via env var (no file write):

```bash
export SPEECH2TEXT_TOKEN="..."
```

## Transcribe (no diarization, ≤ 5 min)

Defaults are optimized for single-speaker voice notes:
- `diarization=false` (default)
- `max_speakers=1`
- timeout is clamped to `300s`

Transcribe a file:

```bash
python3 scripts/speech2text.py transcribe path/to/voice.ogg --out tmp/voice.txt
```

If the service rejects the format, convert to WAV and retry:

```bash
ffmpeg -y -i path/to/voice.ogg -ac 1 -ar 16000 tmp/voice.wav
python3 scripts/speech2text.py transcribe tmp/voice.wav --out tmp/voice.txt
```

## Minimal curl cheatsheet

```bash
# Upload (no diarization) — adjust base URL for your deployment
curl -sS -X POST \
  "$SPEECH2TEXT_BASE_URL/tasks/add?max_speakers=1&diarization=false&data_format=AUDIO" \
  -H "x-jwt-token: $SPEECH2TEXT_TOKEN" \
  -F "uploaded_file=@voice.ogg"

# Poll
curl -sS "$SPEECH2TEXT_BASE_URL/tasks/<uuid>" -H "x-jwt-token: $SPEECH2TEXT_TOKEN"

# Result
curl -sS "$SPEECH2TEXT_BASE_URL/results/txt?task_id=<uuid>" -H "x-jwt-token: $SPEECH2TEXT_TOKEN"
```

## Notes

- For a Telegram voice note saved under `tg_uploads/`, pass that `.ogg` file path into `transcribe`.
- For multi-speaker meetings, enable diarization explicitly: `--diarization --max-speakers N` (slower).
- If you see `CUDA failed with error out of memory`, the service is likely overloaded — wait a bit and retry.
