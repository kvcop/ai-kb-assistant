from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4


def _load_dotenv(path: Path) -> None:
    """Best-effort .env loader (no dependencies).

    Supports:
      - KEY=VALUE
      - export KEY=VALUE

    Does not override already-set env vars.
    """
    try:
        if not path.exists():
            return
        content = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[len('export ') :].strip()
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v.strip().replace(',', '.'))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = v.strip().lower()
    if s in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if s in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return default


def _now() -> float:
    return time.time()


def _jitter(seconds: float, *, frac: float = 0.15) -> float:
    if seconds <= 0:
        return 0.0
    spread = max(0.0, float(seconds) * float(frac))
    return random.uniform(0.0, spread)


_MM_OTP_RE = re.compile(r'(?is)^\s*/mm-otp\b')
_SENSITIVE_KV_RE = re.compile(r'(?m)(?i)\b([A-Z0-9_]*(?:TOKEN|PASSWORD|SECRET)[A-Z0-9_]*)\s*=\s*([^\s]+)')
_BEARER_RE = re.compile(r'(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{10,}')
_TOPIC_LOG_NOISE_PREFIXES: tuple[str, ...] = (
    '✅ принял',
    '🔄 принял',
    '💬 принял',
    '🎙️ принял',
    '📎 файлы сохранил',
    '🕓 перезапускаюсь',
    '🕓 уже перезапускаюсь',
    '🕓 жду завершения',
)


def _topic_log_mode() -> str:
    raw = (
        str((os.getenv('TG_MCP_TOPIC_LOG_MODE') or os.getenv('TG_TOPIC_LOG_MODE') or 'semantic') or '').strip().lower()
    )
    if raw in {'all', 'full', 'debug'}:
        return 'all'
    return 'semantic'


def _topic_log_should_write(item: dict[str, Any]) -> bool:
    if _topic_log_mode() == 'all':
        return True
    if bool(item.get('deferred', False)):
        return False

    op = str(item.get('op') or '').strip()
    if op in {'mcp_send_files_enqueue', 'mcp_send_files_error'}:
        return False

    if op not in {'send_message', 'edit_message_text'}:
        return True

    text = item.get('text')
    if not isinstance(text, str):
        return True
    norm = text.strip().casefold()
    if not norm:
        return True

    if '⏳ работаю' in norm:
        return False
    for pref in _TOPIC_LOG_NOISE_PREFIXES:
        if norm.startswith(pref):
            return False
    if norm.startswith('✅ готово') and len(norm) <= 80:
        return False
    if norm.startswith('🌐 сеть была недоступна'):
        return False

    return True


def _preview_text_redacted(text: object, *, max_chars: int = 2000) -> str:
    if not isinstance(text, str):
        return ''
    s = text.replace('\r', '').strip()
    if _MM_OTP_RE.match(s):
        return '/mm-otp <redacted>'
    try:
        s = _SENSITIVE_KV_RE.sub(lambda m: f'{m.group(1)}=<redacted>', s)
    except Exception:
        pass
    try:
        s = _BEARER_RE.sub('Bearer <redacted>', s)
    except Exception:
        pass
    s = s.replace('\n', ' ').strip()
    s = ' '.join(s.split())
    max_chars = max(0, int(max_chars))
    if max_chars > 0 and len(s) > max_chars:
        s = s[: max(0, max_chars - 1)] + '…'
    return s


def _topic_log_path(*, root: Path, chat_id: int, message_thread_id: int) -> Path | None:
    try:
        cid = int(chat_id)
    except Exception:
        cid = 0
    if cid == 0:
        return None
    try:
        tid = int(message_thread_id or 0)
    except Exception:
        tid = 0
    tid = max(0, int(tid))
    return root / str(int(cid)) / str(int(tid)) / 'events.jsonl'


def _topic_log_append(*, root: Path | None, chat_id: int, message_thread_id: int, item: dict[str, Any]) -> None:
    if root is None:
        return
    try:
        if not _topic_log_should_write(item):
            return
    except Exception:
        return
    path = _topic_log_path(root=root, chat_id=chat_id, message_thread_id=message_thread_id)
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    except Exception:
        return


def _relpath(repo_root: Path, p: Path) -> str:
    try:
        return str(p.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(p)


@dataclass
class TelegramSendJob:
    id: str
    created_ts: float
    chat_id: int
    message_thread_id: int
    caption: str
    files: list[dict[str, Any]]  # {path, filename}

    # Progress / retry
    attempt: int = 0
    max_retries: int = 100
    next_attempt_ts: float = 0.0
    status: str = 'pending'  # pending|retry|done|failed
    last_error: str = ''
    caption_message_id: int = 0
    sent_files: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            'id': self.id,
            'created_ts': float(self.created_ts),
            'chat_id': int(self.chat_id),
            'message_thread_id': int(self.message_thread_id or 0),
            'caption': self.caption,
            'files': list(self.files),
            'attempt': int(self.attempt),
            'max_retries': int(self.max_retries),
            'next_attempt_ts': float(self.next_attempt_ts),
            'status': self.status,
            'last_error': self.last_error,
            'caption_message_id': int(self.caption_message_id),
            'sent_files': int(self.sent_files),
        }

    @staticmethod
    def from_dict(obj: dict[str, Any]) -> TelegramSendJob | None:
        if not isinstance(obj, dict):
            return None
        job_id = str(obj.get('id') or '').strip()
        if not job_id:
            return None
        try:
            chat_id = int(obj.get('chat_id') or 0)
        except Exception:
            chat_id = 0
        if chat_id == 0:
            return None
        try:
            message_thread_id = int(obj.get('message_thread_id') or 0)
        except Exception:
            message_thread_id = 0
        message_thread_id = max(0, int(message_thread_id or 0))
        caption = obj.get('caption')
        caption_s = str(caption or '')
        files_raw = obj.get('files') or []
        files: list[dict[str, Any]] = []
        if isinstance(files_raw, list):
            for item in files_raw:
                if not isinstance(item, dict):
                    continue
                path_v = item.get('path')
                if not isinstance(path_v, str) or not path_v.strip():
                    continue
                filename_v = item.get('filename')
                filename_s = str(filename_v or '').strip() or Path(path_v).name
                files.append({'path': str(path_v).strip(), 'filename': filename_s})
        if not files:
            return None

        created_ts = float(obj.get('created_ts') or 0.0)
        attempt = int(obj.get('attempt') or 0)
        max_retries = int(obj.get('max_retries') or 100)
        next_attempt_ts = float(obj.get('next_attempt_ts') or 0.0)
        status = str(obj.get('status') or 'pending')
        last_error = str(obj.get('last_error') or '')
        caption_message_id = int(obj.get('caption_message_id') or 0)
        sent_files = int(obj.get('sent_files') or 0)

        job = TelegramSendJob(
            id=job_id,
            created_ts=created_ts,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            caption=caption_s,
            files=files,
        )
        job.attempt = max(0, attempt)
        job.max_retries = max(1, max_retries)
        job.next_attempt_ts = max(0.0, next_attempt_ts)
        job.status = status
        job.last_error = last_error
        job.caption_message_id = max(0, caption_message_id)
        job.sent_files = max(0, sent_files)
        return job


class QueueStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._jobs: list[TelegramSendJob] = []
        self._load()

    def _load(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if not self._path.exists():
            self._jobs = []
            return
        try:
            raw = self._path.read_text(encoding='utf-8', errors='replace')
        except OSError:
            self._jobs = []
            return
        try:
            obj = json.loads(raw or '{}')
        except Exception:
            self._jobs = []
            return
        jobs_raw = (obj.get('jobs') or []) if isinstance(obj, dict) else []
        jobs: list[TelegramSendJob] = []
        if isinstance(jobs_raw, list):
            for item in jobs_raw:
                if isinstance(item, dict):
                    j = TelegramSendJob.from_dict(item)
                    if j is not None:
                        jobs.append(j)
        self._jobs = jobs

    def _save(self) -> None:
        tmp = self._path.with_suffix(self._path.suffix + '.tmp')
        payload = {'version': 1, 'jobs': [j.to_dict() for j in self._jobs]}
        data = json.dumps(payload, ensure_ascii=False, indent=2)
        tmp.write_text(data, encoding='utf-8')
        os.replace(tmp, self._path)

    def add_job(self, job: TelegramSendJob) -> None:
        with self._lock:
            self._jobs.append(job)
            self._save()

    def snapshot(self) -> list[TelegramSendJob]:
        with self._lock:
            return [TelegramSendJob.from_dict(j.to_dict()) for j in self._jobs if j is not None]  # type: ignore[misc]

    def find_next_ready(self, *, now: float) -> TelegramSendJob | None:
        with self._lock:
            best: TelegramSendJob | None = None
            for j in self._jobs:
                if j.status not in {'pending', 'retry'}:
                    continue
                if j.next_attempt_ts and j.next_attempt_ts > now:
                    continue
                if best is None or j.created_ts < best.created_ts:
                    best = j
            return TelegramSendJob.from_dict(best.to_dict()) if best is not None else None

    def update_job(self, job: TelegramSendJob) -> None:
        with self._lock:
            for idx, existing in enumerate(self._jobs):
                if existing.id == job.id:
                    self._jobs[idx] = job
                    self._save()
                    return

    def remove_job(self, job_id: str) -> None:
        with self._lock:
            self._jobs = [j for j in self._jobs if j.id != job_id]
            self._save()

    def status(self) -> dict[str, Any]:
        now = _now()
        with self._lock:
            total = len(self._jobs)
            pending = 0
            retry = 0
            failed = 0
            done = 0
            next_ts: float | None = None
            for j in self._jobs:
                if j.status == 'done':
                    done += 1
                elif j.status == 'failed':
                    failed += 1
                elif j.status == 'retry':
                    retry += 1
                    if j.next_attempt_ts and j.next_attempt_ts > now:
                        next_ts = j.next_attempt_ts if next_ts is None else min(next_ts, j.next_attempt_ts)
                elif j.status == 'pending':
                    pending += 1
            return {
                'total': total,
                'pending': pending,
                'retry': retry,
                'done': done,
                'failed': failed,
                'next_attempt_in_sec': max(0.0, float(next_ts - now)) if next_ts is not None else 0.0,
            }


class TelegramSenderWorker:
    def __init__(
        self,
        *,
        store: QueueStore,
        token: str,
        topic_log_root: Path | None = None,
        topic_log_max_chars: int = 2000,
        backoff_base_seconds: float,
        backoff_max_seconds: float,
        max_bytes: int,
    ) -> None:
        self._store = store
        self._token = token
        self._topic_log_root = topic_log_root
        self._topic_log_max_chars = max(0, int(topic_log_max_chars))
        self._backoff_base = max(0.1, float(backoff_base_seconds))
        self._backoff_max = max(self._backoff_base, float(backoff_max_seconds))
        self._max_bytes = max(1, int(max_bytes))

        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(target=self._run, name='tg-send-worker', daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def wake(self) -> None:
        self._wake.set()

    def _log(self, msg: str) -> None:
        print(f'[telegram-send-mcp] {msg}', file=sys.stderr, flush=True)

    def _compute_backoff(self, attempt: int) -> float:
        # attempt starts at 1 for the first failure
        exp = min(60, max(0, attempt - 1))
        delay = self._backoff_base * (2**exp)
        delay = min(self._backoff_max, delay)
        delay += _jitter(delay)
        return float(delay)

    def _run(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        try:
            from tg_bot.telegram_api import TelegramAPI  # local import to keep server startup cheap
        except Exception as e:  # pragma: no cover
            self._log(f'Failed to import tg_bot.telegram_api: {e!r}')
            return

        api = TelegramAPI(token=self._token)

        while not self._stop.is_set():
            now = _now()
            job = self._store.find_next_ready(now=now)
            if job is None:
                self._wake.wait(timeout=1.0)
                self._wake.clear()
                continue

            try:
                self._process_job(api=api, job=job)
            except Exception as e:  # defensive
                self._log(f'Unexpected error while processing job id={job.id}: {e!r}')
                job.attempt += 1
                job.last_error = f'unexpected: {e!r}'
                if job.attempt >= job.max_retries:
                    job.status = 'failed'
                else:
                    job.status = 'retry'
                    job.next_attempt_ts = _now() + self._compute_backoff(job.attempt)
                self._store.update_job(job)

            self._wake.clear()

    def _process_job(self, *, api: Any, job: TelegramSendJob) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        chat_id = int(job.chat_id)
        message_thread_id = int(job.message_thread_id or 0)
        message_thread_id_opt = message_thread_id if message_thread_id > 0 else None

        # Preferred path for small batches: send as a single media group (album).
        if job.sent_files == 0 and len(job.files) <= 10:
            doc_paths: list[Path] = []
            for entry in job.files:
                p = Path(str(entry.get('path') or '')).expanduser()
                if not p.is_absolute():
                    p = (Path.cwd() / p).resolve()
                if not p.exists() or not p.is_file():
                    self._fail_or_retry(job, f'file_not_found: {p}')
                    return
                try:
                    size = int(p.stat().st_size)
                except Exception:
                    size = 0
                if size > 0 and size > self._max_bytes:
                    job.last_error = f'file_too_large: {p} ({size} > {self._max_bytes})'
                    job.status = 'failed'
                    self._store.update_job(job)
                    return
                doc_paths.append(p)

            caption = (job.caption or '').strip()
            # Telegram caption limits are lower than message text limits; if caption is huge, fallback.
            use_album = len(caption) <= 900
            if use_album:
                try:
                    resp = api.send_media_group_documents(
                        chat_id=chat_id,
                        document_paths=[str(p) for p in doc_paths],
                        caption=caption or None,
                        message_thread_id=message_thread_id_opt,
                        timeout=180,
                        max_bytes=self._max_bytes,
                    )
                    try:
                        msg_ids: list[int] = []
                        result = resp.get('result') if isinstance(resp, dict) else None
                        if isinstance(result, list):
                            for m in result:
                                if not isinstance(m, dict):
                                    continue
                                try:
                                    mid = int(m.get('message_id') or 0)
                                except Exception:
                                    mid = 0
                                if mid > 0:
                                    msg_ids.append(int(mid))
                        _topic_log_append(
                            root=self._topic_log_root,
                            chat_id=int(chat_id),
                            message_thread_id=int(message_thread_id),
                            item={
                                'ts': float(_now()),
                                'dir': 'out',
                                'op': 'send_media_group_documents',
                                'chat_id': int(chat_id),
                                'thread_id': int(message_thread_id),
                                'message_ids': msg_ids,
                                'caption': _preview_text_redacted(caption, max_chars=int(self._topic_log_max_chars)),
                                'files': [_relpath(repo_root, p) for p in doc_paths],
                                'deferred': False,
                                'meta': {'kind': 'mcp_send_files', 'job_id': str(job.id), 'mode': 'album'},
                            },
                        )
                    except Exception:
                        pass
                    job.sent_files = len(job.files)
                    job.status = 'done'
                    job.last_error = ''
                    self._store.update_job(job)
                    self._store.remove_job(job.id)
                    return
                except Exception as e:
                    self._fail_or_retry(job, f'send_media_group: {e!r}')
                    return

        # Caption message first (once), then documents reply to it.
        if job.caption and not job.caption_message_id:
            try:
                resp = api.send_message(
                    chat_id=chat_id, message_thread_id=message_thread_id_opt, text=job.caption, timeout=20
                )
                msg_id = int(((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id') or 0)
                job.caption_message_id = max(0, msg_id)
                job.last_error = ''
                self._store.update_job(job)
                try:
                    _topic_log_append(
                        root=self._topic_log_root,
                        chat_id=int(chat_id),
                        message_thread_id=int(message_thread_id),
                        item={
                            'ts': float(_now()),
                            'dir': 'out',
                            'op': 'send_message',
                            'chat_id': int(chat_id),
                            'thread_id': int(message_thread_id),
                            'message_id': int(job.caption_message_id),
                            'text': _preview_text_redacted(job.caption, max_chars=int(self._topic_log_max_chars)),
                            'deferred': False,
                            'meta': {'kind': 'mcp_send_files', 'job_id': str(job.id), 'role': 'caption'},
                        },
                    )
                except Exception:
                    pass
            except Exception as e:
                self._fail_or_retry(job, f'send_message: {e!r}')
                return

        reply_to = int(job.caption_message_id) if job.caption_message_id else None

        # Continue from where we stopped (idempotent across retries).
        while job.sent_files < len(job.files):
            entry = job.files[job.sent_files]
            p = Path(str(entry.get('path') or '')).expanduser()
            if not p.is_absolute():
                p = (Path.cwd() / p).resolve()
            filename = str(entry.get('filename') or p.name).strip() or p.name

            # Quick pre-check: if file is missing, retry later.
            if not p.exists() or not p.is_file():
                self._fail_or_retry(job, f'file_not_found: {p}')
                return

            # Quick pre-check: size (avoid 100 pointless retries on a permanently-too-big file).
            try:
                size = int(p.stat().st_size)
            except Exception:
                size = 0
            if size > 0 and size > self._max_bytes:
                job.last_error = f'file_too_large: {p} ({size} > {self._max_bytes})'
                job.status = 'failed'
                self._store.update_job(job)
                return

            try:
                resp = api.send_document(
                    chat_id=chat_id,
                    message_thread_id=message_thread_id_opt,
                    document_path=p,
                    filename=filename,
                    caption=None,
                    reply_to_message_id=reply_to,
                    timeout=60,
                    max_bytes=self._max_bytes,
                )
                try:
                    msg_id = 0
                    try:
                        msg_id = int(
                            ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id') or 0
                        )
                    except Exception:
                        msg_id = 0
                    _topic_log_append(
                        root=self._topic_log_root,
                        chat_id=int(chat_id),
                        message_thread_id=int(message_thread_id),
                        item={
                            'ts': float(_now()),
                            'dir': 'out',
                            'op': 'send_document',
                            'chat_id': int(chat_id),
                            'thread_id': int(message_thread_id),
                            'message_id': int(msg_id),
                            'reply_to_message_id': int(reply_to) if reply_to is not None else None,
                            'filename': str(filename),
                            'document_path': _relpath(repo_root, p),
                            'deferred': False,
                            'meta': {'kind': 'mcp_send_files', 'job_id': str(job.id), 'index': int(job.sent_files)},
                        },
                    )
                except Exception:
                    pass
                job.sent_files += 1
                job.last_error = ''
                self._store.update_job(job)
            except Exception as e:
                self._fail_or_retry(job, f'send_document: {e!r}')
                return

        job.status = 'done'
        self._store.update_job(job)
        # Keep queue file small.
        self._store.remove_job(job.id)

    def _fail_or_retry(self, job: TelegramSendJob, err: str) -> None:
        job.attempt += 1
        job.last_error = err[:1000]
        if job.attempt >= job.max_retries:
            job.status = 'failed'
        else:
            job.status = 'retry'
            job.next_attempt_ts = _now() + self._compute_backoff(job.attempt)
        self._store.update_job(job)
        try:
            if job.attempt == 1 or job.status == 'failed':
                _topic_log_append(
                    root=self._topic_log_root,
                    chat_id=int(job.chat_id),
                    message_thread_id=int(job.message_thread_id or 0),
                    item={
                        'ts': float(_now()),
                        'dir': 'out',
                        'op': 'mcp_send_files_error',
                        'chat_id': int(job.chat_id),
                        'thread_id': int(job.message_thread_id or 0),
                        'deferred': bool(job.status == 'retry'),
                        'error': str(err)[:400],
                        'meta': {
                            'kind': 'mcp_send_files',
                            'job_id': str(job.id),
                            'attempt': int(job.attempt),
                            'status': str(job.status),
                        },
                    },
                )
        except Exception:
            pass


class MCPServer:
    def __init__(
        self,
        *,
        store: QueueStore,
        worker: TelegramSenderWorker,
        repo_root: Path,
        token: str,
        bot_state_path: Path,
        followups_ack_path: Path,
        default_chat_id: int,
        default_max_retries: int,
        default_parse_mode: str,
        topic_log_root: Path | None = None,
        topic_log_max_chars: int = 2000,
        sender_enabled: bool,
        sender_disabled_reason: str,
        followups_enabled: bool,
        followups_disabled_reason: str,
    ) -> None:
        self._store = store
        self._worker = worker
        self._repo_root = repo_root
        self._token = str(token or '').strip()
        self._bot_state_path = bot_state_path
        self._followups_ack_path = followups_ack_path
        self._default_chat_id = int(default_chat_id)
        self._default_max_retries = max(1, int(default_max_retries))
        self._default_parse_mode = str(default_parse_mode or '').strip()
        if self._default_parse_mode.lower() in {'none', 'off'}:
            self._default_parse_mode = ''
        self._topic_log_root = topic_log_root
        self._topic_log_max_chars = max(0, int(topic_log_max_chars))
        self._sender_enabled = bool(sender_enabled)
        self._sender_disabled_reason = str(sender_disabled_reason or '').strip()
        self._followups_enabled = bool(followups_enabled)
        self._followups_disabled_reason = str(followups_disabled_reason or '').strip()

        if str(self._repo_root) not in sys.path:
            sys.path.insert(0, str(self._repo_root))

    def _write(self, obj: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + '\n')
        sys.stdout.flush()

    def _ok_tool_result(self, *, text: str, structured: dict[str, Any]) -> dict[str, Any]:
        return {
            'content': [{'type': 'text', 'text': text}],
            'structuredContent': structured,
        }

    def _read_json_dict(self, path: Path) -> dict[str, Any]:
        try:
            raw = path.read_text(encoding='utf-8', errors='replace')
        except OSError:
            return {}
        try:
            obj = json.loads(raw or '{}')
        except Exception:
            return {}
        return obj if isinstance(obj, dict) else {}

    def _atomic_write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + '.tmp')
        tmp.write_text(content, encoding='utf-8')
        os.replace(tmp, path)

    def _scope_key(self, *, chat_id: int, message_thread_id: int) -> str:
        return f'{int(chat_id)}:{int(message_thread_id or 0)}'

    def _tools(self) -> list[dict[str, Any]]:
        send_files_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'paths': {'type': 'array', 'items': {'type': 'string'}, 'minItems': 1},
                'caption': {'type': 'string'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
            },
            'required': ['paths'],
            'additionalProperties': False,
        }
        send_files_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'job_id': {'type': 'string'},
                'queued_files': {'type': 'integer'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'caption': {'type': 'string'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'job_id', 'queued_files', 'chat_id', 'message_thread_id', 'caption', 'note'],
            'additionalProperties': False,
        }

        status_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {},
            'additionalProperties': False,
        }
        status_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'total': {'type': 'integer'},
                'pending': {'type': 'integer'},
                'retry': {'type': 'integer'},
                'done': {'type': 'integer'},
                'failed': {'type': 'integer'},
                'next_attempt_in_sec': {'type': 'number'},
            },
            'required': ['total', 'pending', 'retry', 'done', 'failed', 'next_attempt_in_sec'],
            'additionalProperties': False,
        }

        send_message_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'text': {'type': 'string'},
                'parse_mode': {'type': 'string', 'default': self._default_parse_mode},
                'reply_to_message_id': {'type': 'integer'},
            },
            'required': ['text'],
            'additionalProperties': False,
        }
        send_message_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'message_id': {'type': 'integer'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'chat_id', 'message_thread_id', 'message_id', 'note'],
            'additionalProperties': False,
        }

        edit_forum_topic_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'name': {'type': 'string'},
            },
            'required': ['chat_id', 'message_thread_id', 'name'],
            'additionalProperties': False,
        }
        edit_forum_topic_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'name': {'type': 'string'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'chat_id', 'message_thread_id', 'name', 'note'],
            'additionalProperties': False,
        }

        followups_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'after_message_id': {'type': 'integer'},
                'limit': {'type': 'integer'},
                'timeout_seconds': {'type': 'number'},
            },
            'additionalProperties': False,
        }
        followups_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'followups': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': True}},
                'latest_message_id': {'type': 'integer'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'chat_id', 'message_thread_id', 'followups', 'latest_message_id', 'note'],
            'additionalProperties': False,
        }

        ack_followups_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'last_message_id': {'type': 'integer'},
            },
            'required': ['last_message_id'],
            'additionalProperties': False,
        }
        ack_followups_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'last_message_id': {'type': 'integer'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'chat_id', 'message_thread_id', 'last_message_id', 'note'],
            'additionalProperties': False,
        }

        tools: list[dict[str, Any]] = [
            {
                'name': 'queue_status',
                'title': 'Telegram Send Queue Status',
                'description': 'Get current send queue status (pending/retry/failed).',
                'inputSchema': status_schema,
                'outputSchema': status_out,
            },
        ]

        if self._sender_enabled and self._token:
            tools.extend(
                [
                    {
                        'name': 'send_files',
                        'title': 'Send Files to Telegram (Async)',
                        'description': (
                            'Queue one or more local files to be sent as Telegram documents. Always returns OK immediately; '
                            'sending happens asynchronously with retries/backoff.'
                        ),
                        'inputSchema': send_files_schema,
                        'outputSchema': send_files_out,
                    },
                    {
                        'name': 'send_message',
                        'title': 'Send Message to Telegram',
                        'description': 'Send a text message to Telegram (optionally into a specific topic/thread).',
                        'inputSchema': send_message_schema,
                        'outputSchema': send_message_out,
                    },
                    {
                        'name': 'edit_forum_topic',
                        'title': 'Edit Telegram Forum Topic',
                        'description': 'Rename a Telegram forum topic (thread).',
                        'inputSchema': edit_forum_topic_schema,
                        'outputSchema': edit_forum_topic_out,
                    },
                ]
            )

        if self._followups_enabled:
            tools.extend(
                [
                    {
                        'name': 'get_followups',
                        'title': 'Get Telegram Follow-ups',
                        'description': 'Read pending follow-up messages captured by tg_bot while Codex is running in a scope.',
                        'inputSchema': followups_schema,
                        'outputSchema': followups_out,
                    },
                    {
                        'name': 'wait_followups',
                        'title': 'Wait for Telegram Follow-ups',
                        'description': 'Block up to timeout_seconds waiting for new follow-ups, then return them.',
                        'inputSchema': followups_schema,
                        'outputSchema': followups_out,
                    },
                    {
                        'name': 'ack_followups',
                        'title': 'Acknowledge Telegram Follow-ups',
                        'description': 'Mark follow-ups as processed up to last_message_id for this scope (used to dedupe queued events).',
                        'inputSchema': ack_followups_schema,
                        'outputSchema': ack_followups_out,
                    },
                ]
            )

        return tools

    def serve_forever(self) -> None:
        for raw_line in sys.stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except Exception:
                continue
            if not isinstance(req, dict):
                continue

            method = req.get('method')
            req_id = req.get('id')
            if not method or not isinstance(method, str):
                continue

            # Notifications have no id: do not respond.
            if req_id is None:
                continue

            if method == 'initialize':
                self._write(
                    {
                        'jsonrpc': '2.0',
                        'id': req_id,
                        'result': {
                            'protocolVersion': '2024-11-05',
                            'capabilities': {'tools': {'listChanged': False}},
                            'serverInfo': {'name': 'telegram-send-mcp', 'version': '0.2.0'},
                        },
                    }
                )
                continue

            if method == 'tools/list':
                self._write({'jsonrpc': '2.0', 'id': req_id, 'result': {'tools': self._tools()}})
                continue

            if method == 'tools/call':
                params = req.get('params') or {}
                name = params.get('name') if isinstance(params, dict) else None
                args = (params.get('arguments') or {}) if isinstance(params, dict) else {}
                if not isinstance(name, str) or not isinstance(args, dict):
                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK (invalid arguments; nothing queued).',
                                structured={
                                    'ok': True,
                                    'job_id': '',
                                    'queued_files': 0,
                                    'chat_id': int(self._default_chat_id),
                                    'message_thread_id': 0,
                                    'caption': '',
                                    'note': 'invalid_arguments',
                                },
                            ),
                        }
                    )
                    continue

                if name == 'queue_status':
                    st = self._store.status()
                    self._write(
                        {'jsonrpc': '2.0', 'id': req_id, 'result': self._ok_tool_result(text='OK', structured=st)}
                    )
                    continue

                if name == 'send_message':
                    text = str(args.get('text') or '').strip()
                    chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    reply_to_message_id = int(args.get('reply_to_message_id') or 0)
                    if 'parse_mode' in args:
                        parse_mode = str(args.get('parse_mode') or '').strip()
                        if parse_mode.lower() in {'none', 'off'}:
                            parse_mode = ''
                    else:
                        parse_mode = self._default_parse_mode

                    if not self._sender_enabled or not self._token:
                        note = 'sender_disabled'
                        if self._sender_disabled_reason:
                            note = f'{note}: {self._sender_disabled_reason}'
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (sender disabled; message not sent).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'message_id': 0,
                                        'note': note,
                                    },
                                ),
                            }
                        )
                        continue

                    if not text or chat_id == 0:
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (nothing sent).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'message_id': 0,
                                        'note': 'missing_text_or_chat_id',
                                    },
                                ),
                            }
                        )
                        continue

                    msg_id = 0
                    note = 'send_failed: unknown'
                    used_parse_mode = parse_mode or ''
                    try:
                        from tg_bot.telegram_api import TelegramAPI  # local import

                        api = TelegramAPI(token=self._token)
                        try:
                            resp = api.send_message(
                                chat_id=int(chat_id),
                                message_thread_id=(int(message_thread_id) if int(message_thread_id) > 0 else None),
                                text=text,
                                reply_to_message_id=(
                                    int(reply_to_message_id) if int(reply_to_message_id) > 0 else None
                                ),
                                parse_mode=(parse_mode or None),
                                timeout=20,
                            )
                            msg_id = int(
                                ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id') or 0
                            )
                            note = 'sent'
                        except Exception as e:
                            low = str(e).lower()
                            if parse_mode and "can't parse entities" in low:
                                try:
                                    resp = api.send_message(
                                        chat_id=int(chat_id),
                                        message_thread_id=(
                                            int(message_thread_id) if int(message_thread_id) > 0 else None
                                        ),
                                        text=text,
                                        reply_to_message_id=(
                                            int(reply_to_message_id) if int(reply_to_message_id) > 0 else None
                                        ),
                                        parse_mode=None,
                                        timeout=20,
                                    )
                                    msg_id = int(
                                        ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id')
                                        or 0
                                    )
                                    note = 'sent_plain_fallback (cant_parse_entities)'
                                    used_parse_mode = ''
                                except Exception as e2:
                                    msg_id = 0
                                    note = f'send_failed: {e2!r}'
                            else:
                                msg_id = 0
                                note = f'send_failed: {e!r}'
                    except Exception as e:
                        msg_id = 0
                        note = f'send_failed: {e!r}'

                    try:
                        _topic_log_append(
                            root=self._topic_log_root,
                            chat_id=int(chat_id),
                            message_thread_id=int(message_thread_id),
                            item={
                                'ts': float(_now()),
                                'dir': 'out',
                                'op': 'send_message',
                                'chat_id': int(chat_id),
                                'thread_id': int(message_thread_id),
                                'message_id': int(msg_id),
                                'reply_to_message_id': int(reply_to_message_id)
                                if int(reply_to_message_id) > 0
                                else None,
                                'parse_mode': used_parse_mode or None,
                                'text': _preview_text_redacted(text, max_chars=int(self._topic_log_max_chars)),
                                'deferred': False,
                                'meta': {'kind': 'mcp_send_message', 'note': str(note)[:200]},
                            },
                        )
                    except Exception:
                        pass

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK',
                                structured={
                                    'ok': True,
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'message_id': int(msg_id),
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                if name == 'edit_forum_topic':
                    chat_id = int(args.get('chat_id') or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    topic_name = str(args.get('name') or '').strip()

                    if not self._sender_enabled or not self._token:
                        note = 'sender_disabled'
                        if self._sender_disabled_reason:
                            note = f'{note}: {self._sender_disabled_reason}'
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (sender disabled; topic not edited).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'name': topic_name,
                                        'note': note,
                                    },
                                ),
                            }
                        )
                        continue

                    if chat_id == 0 or message_thread_id <= 0 or not topic_name:
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (nothing edited).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'name': topic_name,
                                        'note': 'missing_chat_id_or_message_thread_id_or_name',
                                    },
                                ),
                            }
                        )
                        continue

                    try:
                        from tg_bot.telegram_api import TelegramAPI  # local import

                        api = TelegramAPI(token=self._token)
                        _ = api._request_json(
                            'editForumTopic',
                            params={
                                'chat_id': int(chat_id),
                                'message_thread_id': int(message_thread_id),
                                'name': topic_name,
                            },
                            timeout=20,
                        )
                        note = 'edited'
                    except Exception as e:
                        note = f'edit_failed: {e!r}'

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK',
                                structured={
                                    'ok': True,
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'name': topic_name,
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                if name in {'get_followups', 'wait_followups'}:
                    if not self._followups_enabled:
                        note = 'followups_disabled'
                        if self._followups_disabled_reason:
                            note = f'{note}: {self._followups_disabled_reason}'
                        chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                        message_thread_id = int(args.get('message_thread_id') or 0)
                        message_thread_id = max(0, int(message_thread_id or 0))
                        after_message_id = int(args.get('after_message_id') or 0)
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (followups disabled).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'followups': [],
                                        'latest_message_id': int(after_message_id),
                                        'note': note,
                                    },
                                ),
                            }
                        )
                        continue
                    chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    after_message_id = int(args.get('after_message_id') or 0)
                    limit = int(args.get('limit') or 50)
                    limit = max(1, min(200, int(limit or 50)))
                    timeout_s = float(args.get('timeout_seconds') or 0.0)
                    timeout_s = max(0.0, min(300.0, timeout_s))

                    note = 'ok'
                    followups: list[dict[str, Any]] = []
                    latest_mid = 0
                    sk = self._scope_key(chat_id=chat_id, message_thread_id=message_thread_id)

                    start = _now()
                    while True:
                        try:
                            st = self._read_json_dict(self._bot_state_path)
                            raw = st.get('pending_followups_by_scope') or {}
                            per_scope = raw.get(sk) if isinstance(raw, dict) else None
                            items = per_scope if isinstance(per_scope, list) else []
                            cleaned: list[dict[str, Any]] = []
                            for it in items:
                                if not isinstance(it, dict):
                                    continue
                                try:
                                    mid = int(it.get('message_id') or 0)
                                except Exception:
                                    mid = 0
                                if mid <= 0 or mid <= after_message_id:
                                    continue
                                cleaned.append(dict(it))
                            cleaned.sort(key=lambda x: int(x.get('message_id') or 0))
                            if cleaned:
                                followups = cleaned[:limit]
                                try:
                                    latest_mid = int(followups[-1].get('message_id') or 0)
                                except Exception:
                                    latest_mid = 0
                            else:
                                followups = []
                                latest_mid = int(after_message_id or 0)
                        except Exception as e:
                            note = f'read_failed: {e!r}'
                            followups = []
                            latest_mid = int(after_message_id or 0)

                        if name == 'get_followups':
                            break
                        if followups:
                            break
                        if timeout_s <= 0:
                            break
                        if (_now() - start) >= timeout_s:
                            break
                        time.sleep(0.5)

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK',
                                structured={
                                    'ok': True,
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'followups': followups,
                                    'latest_message_id': int(latest_mid),
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                if name == 'ack_followups':
                    if not self._followups_enabled:
                        note = 'followups_disabled'
                        if self._followups_disabled_reason:
                            note = f'{note}: {self._followups_disabled_reason}'
                        chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                        message_thread_id = int(args.get('message_thread_id') or 0)
                        message_thread_id = max(0, int(message_thread_id or 0))
                        last_message_id = int(args.get('last_message_id') or 0)
                        last_message_id = max(0, int(last_message_id or 0))
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (followups disabled).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'last_message_id': int(last_message_id),
                                        'note': note,
                                    },
                                ),
                            }
                        )
                        continue
                    chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    last_message_id = int(args.get('last_message_id') or 0)
                    last_message_id = max(0, int(last_message_id or 0))
                    sk = self._scope_key(chat_id=chat_id, message_thread_id=message_thread_id)

                    note = 'ack_ok'
                    try:
                        cur = self._read_json_dict(self._followups_ack_path)
                        version = int(cur.get('version') or 1)
                        acked = cur.get('acked_by_scope') if isinstance(cur.get('acked_by_scope'), dict) else {}
                        if not isinstance(acked, dict):
                            acked = {}
                        prev = 0
                        try:
                            prev = int(acked.get(sk) or 0)
                        except Exception:
                            prev = 0
                        if last_message_id > prev:
                            acked[sk] = int(last_message_id)
                        payload = {'version': int(version), 'acked_by_scope': acked}
                        self._atomic_write_text(
                            self._followups_ack_path, json.dumps(payload, ensure_ascii=False, indent=2)
                        )
                    except Exception as e:
                        note = f'ack_failed: {e!r}'

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK',
                                structured={
                                    'ok': True,
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'last_message_id': int(last_message_id),
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                if name == 'send_files':
                    if not self._sender_enabled:
                        note = 'sender_disabled'
                        if self._sender_disabled_reason:
                            note = f'{note}: {self._sender_disabled_reason}'
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (sender disabled; nothing queued).',
                                    structured={
                                        'ok': True,
                                        'job_id': '',
                                        'queued_files': 0,
                                        'chat_id': int(self._default_chat_id),
                                        'message_thread_id': 0,
                                        'caption': '',
                                        'note': note,
                                    },
                                ),
                            }
                        )
                        continue

                    paths_raw = args.get('paths')
                    caption = str(args.get('caption') or '').strip()
                    chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    files: list[dict[str, Any]] = []
                    if isinstance(paths_raw, list):
                        for p in paths_raw:
                            if isinstance(p, str) and p.strip():
                                p0 = Path(p.strip()).expanduser()
                                if not p0.is_absolute():
                                    p0 = (Path.cwd() / p0).resolve()
                                filename = p0.name
                                files.append({'path': str(p0), 'filename': filename})
                    if not files or chat_id == 0:
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (nothing queued).',
                                    structured={
                                        'ok': True,
                                        'job_id': '',
                                        'queued_files': 0,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'caption': caption,
                                        'note': 'missing_paths_or_chat_id',
                                    },
                                ),
                            }
                        )
                        continue

                    job = TelegramSendJob(
                        id=str(uuid4()),
                        created_ts=_now(),
                        chat_id=int(chat_id),
                        message_thread_id=int(message_thread_id),
                        caption=caption,
                        files=files,
                    )
                    job.max_retries = int(self._default_max_retries)
                    try:
                        self._store.add_job(job)
                        self._worker.wake()
                        note = 'accepted_and_queued'
                    except Exception as e:
                        # User asked for "always OK": don't fail the tool call.
                        note = f'queue_write_failed: {e!r}'

                    try:
                        repo_root = Path(__file__).resolve().parents[1]
                        file_paths: list[str] = []
                        for entry in files:
                            p = Path(str(entry.get('path') or '')).expanduser()
                            if not p.is_absolute():
                                p = (Path.cwd() / p).resolve()
                            file_paths.append(_relpath(repo_root, p))
                        _topic_log_append(
                            root=self._topic_log_root,
                            chat_id=int(chat_id),
                            message_thread_id=int(message_thread_id),
                            item={
                                'ts': float(_now()),
                                'dir': 'out',
                                'op': 'mcp_send_files_enqueue',
                                'chat_id': int(chat_id),
                                'thread_id': int(message_thread_id),
                                'job_id': str(job.id),
                                'caption': _preview_text_redacted(caption, max_chars=int(self._topic_log_max_chars)),
                                'files': file_paths,
                                'deferred': True,
                                'meta': {'kind': 'mcp_send_files', 'note': str(note)[:200]},
                            },
                        )
                    except Exception:
                        pass

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text=f'OK queued {len(files)} file(s) (job_id={job.id}).',
                                structured={
                                    'ok': True,
                                    'job_id': job.id,
                                    'queued_files': len(files),
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'caption': caption,
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                self._write(
                    {
                        'jsonrpc': '2.0',
                        'id': req_id,
                        'error': {'code': -32601, 'message': f'Unknown tool: {name}'},
                    }
                )
                continue

            self._write(
                {
                    'jsonrpc': '2.0',
                    'id': req_id,
                    'error': {'code': -32601, 'message': f'Method not found: {method}'},
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description='MCP server: async Telegram file sender with retries/backoff.')
    parser.add_argument(
        '--queue-path',
        default='',
        help='Queue JSON path (default: $TG_MCP_QUEUE_PATH or <repo>/.mcp/telegram-send-queue.json).',
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=_env_int('TG_MCP_MAX_RETRIES', 100),
        help='Max retries per job (default: $TG_MCP_MAX_RETRIES or 100).',
    )
    parser.add_argument(
        '--backoff-base-seconds',
        type=float,
        default=_env_float('TG_MCP_BACKOFF_BASE_SECONDS', 2.0),
        help='Backoff base seconds (default: $TG_MCP_BACKOFF_BASE_SECONDS or 2.0).',
    )
    parser.add_argument(
        '--backoff-max-seconds',
        type=float,
        default=_env_float('TG_MCP_BACKOFF_MAX_SECONDS', 300.0),
        help='Backoff max seconds (default: $TG_MCP_BACKOFF_MAX_SECONDS or 300.0).',
    )
    parser.add_argument(
        '--max-mb',
        type=int,
        default=_env_int('TG_MCP_SEND_MAX_MB', _env_int('TG_SEND_MAX_MB', 50)),
        help='Telegram document max size MB (default: $TG_MCP_SEND_MAX_MB or $TG_SEND_MAX_MB or 50).',
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    if _env_bool('TG_MCP_LOAD_DOTENV', False):
        _load_dotenv(repo_root / 'tg_bot' / '.env')
        _load_dotenv(repo_root / '.env.tg_bot')

    token = (os.getenv('TG_BOT_TOKEN') or '').strip()
    default_chat_id = _env_int('TG_MCP_DEFAULT_CHAT_ID', _env_int('TG_OWNER_CHAT_ID', 0))
    mcp_enabled = _env_bool('TG_MCP_ENABLED', True)
    if 'TG_MCP_DEFAULT_PARSE_MODE' in os.environ:
        default_parse_mode = (os.getenv('TG_MCP_DEFAULT_PARSE_MODE') or '').strip()
    else:
        default_parse_mode = 'Markdown'

    sender_enabled = False
    followups_enabled = False
    sender_disabled_reason = ''
    followups_disabled_reason = ''
    if not mcp_enabled:
        sender_disabled_reason = 'disabled_by_env (TG_MCP_ENABLED=0)'
        followups_disabled_reason = 'disabled_by_env (TG_MCP_ENABLED=0)'
    elif not token:
        sender_disabled_reason = 'missing TG_BOT_TOKEN (env may be restricted for non-owner chats)'
        followups_disabled_reason = 'missing TG_BOT_TOKEN (env may be restricted for non-owner chats)'
    else:
        sender_enabled = _env_bool('TG_MCP_SENDER_ENABLED', True)
        followups_enabled = _env_bool('TG_MCP_FOLLOWUPS_ENABLED', True)
        if not sender_enabled:
            sender_disabled_reason = 'disabled_by_env (TG_MCP_SENDER_ENABLED=0)'
        if not followups_enabled:
            followups_disabled_reason = 'disabled_by_env (TG_MCP_FOLLOWUPS_ENABLED=0)'

    queue_path = (args.queue_path or '').strip() or (os.getenv('TG_MCP_QUEUE_PATH') or '').strip()
    if queue_path:
        queue_file = Path(queue_path).expanduser()
    else:
        queue_file = repo_root / '.mcp' / 'telegram-send-queue.json'

    store = QueueStore(queue_file)

    # Apply default max retries to all existing jobs that have weird values.
    for j in store.snapshot():
        if j is None:
            continue
        if j.max_retries <= 0 or j.max_retries > 1000:
            j.max_retries = max(1, int(args.max_retries))
            store.update_job(j)

    topic_log_root_raw = (os.getenv('TG_MCP_TOPIC_LOG_ROOT') or '').strip()
    if topic_log_root_raw:
        p = Path(topic_log_root_raw).expanduser()
        topic_log_root = (p if p.is_absolute() else (repo_root / p)).resolve()
    else:
        topic_log_root = (repo_root / 'logs' / 'tg-bot' / 'topics').resolve()
    topic_log_max_chars = _env_int('TG_MCP_TOPIC_LOG_MAX_CHARS', 2000)

    worker = TelegramSenderWorker(
        store=store,
        token=token,
        topic_log_root=topic_log_root,
        topic_log_max_chars=int(topic_log_max_chars),
        backoff_base_seconds=float(args.backoff_base_seconds),
        backoff_max_seconds=float(args.backoff_max_seconds),
        max_bytes=int(args.max_mb) * 1024 * 1024,
    )
    if sender_enabled and token:
        worker.start()

    server = MCPServer(
        store=store,
        worker=worker,
        repo_root=repo_root,
        token=token,
        bot_state_path=Path(os.getenv('TG_BOT_STATE_PATH', str(repo_root / 'logs' / 'tg-bot' / 'state.json'))),
        followups_ack_path=Path(
            os.getenv('TG_MCP_FOLLOWUPS_ACK_PATH', str(repo_root / '.mcp' / 'telegram-followups-ack.json'))
        ),
        default_chat_id=int(default_chat_id),
        default_max_retries=int(args.max_retries),
        default_parse_mode=default_parse_mode,
        topic_log_root=topic_log_root,
        topic_log_max_chars=int(topic_log_max_chars),
        sender_enabled=sender_enabled,
        sender_disabled_reason=sender_disabled_reason,
        followups_enabled=followups_enabled,
        followups_disabled_reason=followups_disabled_reason,
    )
    server.serve_forever()
    worker.stop()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
