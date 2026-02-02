from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .codex_runner import CodexProfile, CodexRunner
from .config import BotConfig
from .mattermost_watch import MattermostWatcher
from .router import Router
from .scheduler import ParallelScheduler, SchedulableEvent
from .state import BotState
from .telegram_api import TelegramAPI, TelegramDeliveryAPI
from .ui_labels import codex_resume_label
from .watch import Watcher
from .workspaces import WorkspaceManager


@dataclass(frozen=True)
class Event:
    kind: str  # "text" | "callback"
    chat_id: int
    chat_type: str  # "private" | "group" | "supergroup" | "channel" (best-effort)
    user_id: int
    text: str
    message_thread_id: int = 0
    chat_meta: dict[str, object] | None = None
    user_meta: dict[str, object] | None = None
    attachments: tuple[dict[str, object], ...] = ()
    reply_to: dict[str, object] | None = None
    message_id: int = 0
    callback_query_id: str = ''
    received_ts: float = 0.0
    ack_message_id: int = 0
    synthetic: bool = False
    queued_from_disk: bool = False
    spool_file: str = ''


def _queue_spool_path(state_path: Path) -> Path:
    return state_path.with_name('queue.jsonl')


def _spool_append(path: Path, ev: Event) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        'version': 2,
        'kind': ev.kind,
        'chat_id': int(ev.chat_id),
        'chat_type': str(ev.chat_type or ''),
        'message_thread_id': int(ev.message_thread_id or 0),
        'user_id': int(ev.user_id),
        'text': ev.text,
        'chat_meta': ev.chat_meta or None,
        'user_meta': ev.user_meta or None,
        'attachments': list(ev.attachments) if ev.attachments else [],
        'reply_to': ev.reply_to or None,
        'message_id': int(ev.message_id),
        'callback_query_id': ev.callback_query_id,
        'received_ts': float(ev.received_ts),
        'ack_message_id': int(ev.ack_message_id),
        'synthetic': bool(ev.synthetic),
        'ts': float(time.time()),
    }
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')


_SPOOL_DRAIN_RE = re.compile(r'\.drain\.(\d+)\.jsonl$')


def _spool_drain_sort_key(path: Path) -> tuple[int, float, str]:
    """Order drain files deterministically (oldest first), best-effort."""
    ts = 0
    m = _SPOOL_DRAIN_RE.search(path.name)
    if m:
        try:
            ts = int(m.group(1))
        except Exception:
            ts = 0
    try:
        st = path.stat()
        mtime = float(st.st_mtime)
    except Exception:
        mtime = 0.0
    return (ts, mtime, path.name)


def _spool_load(
    path: Path,
    *,
    max_events: int | None = 1000,
    rename_to_drain: bool = True,
) -> tuple[list[Event], Path | None]:
    """Load spooled events and return (events, drain_path).

    We rename the spool file to a drain file first so the next instance can keep appending safely.
    Drain file is intended to be deleted after the loaded events are processed.
    """
    if max_events is not None:
        try:
            max_events = int(max_events)
        except Exception:
            max_events = None
    if max_events is not None and max_events <= 0:
        max_events = None
    if not path.exists():
        return ([], None)

    drain: Path | None = None
    if rename_to_drain:
        ts = int(time.time())
        drain = path.with_name(f'{path.name}.drain.{ts}.jsonl')
        try:
            os.replace(path, drain)
        except OSError:
            return ([], None)
    else:
        drain = path

    events: list[Event] = []
    try:
        raw = drain.read_text(encoding='utf-8', errors='replace') if drain is not None else ''
    except OSError:
        return ([], drain)

    for line in raw.splitlines():
        if max_events is not None and len(events) >= max_events:
            break
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        kind = str(obj.get('kind') or '').strip()
        if kind not in {'text', 'callback'}:
            continue
        try:
            chat_id = int(obj.get('chat_id') or 0)
            user_id = int(obj.get('user_id') or 0)
        except Exception:
            continue
        try:
            message_thread_id = int(obj.get('message_thread_id') or 0)
        except Exception:
            message_thread_id = 0
        chat_type = obj.get('chat_type')
        chat_type_s = str(chat_type or '') if isinstance(chat_type, str) else ''
        text = obj.get('text')
        if not isinstance(text, str) or not text.strip():
            continue
        try:
            message_id = int(obj.get('message_id') or 0)
            ack_message_id = int(obj.get('ack_message_id') or 0)
        except Exception:
            message_id = 0
            ack_message_id = 0
        synthetic_raw = obj.get('synthetic')
        synthetic = bool(synthetic_raw) if isinstance(synthetic_raw, bool) else False
        cb_id = obj.get('callback_query_id')
        callback_query_id = str(cb_id or '') if isinstance(cb_id, str) else ''
        try:
            received_ts = float(obj.get('received_ts') or 0.0)
        except Exception:
            received_ts = 0.0
        attachments_raw = obj.get('attachments') or []
        attachments: list[dict[str, object]] = []
        if isinstance(attachments_raw, list):
            for item in attachments_raw:
                if not isinstance(item, dict):
                    continue
                path_v = item.get('path')
                name_v = item.get('name')
                if not isinstance(path_v, str) or not path_v.strip():
                    continue
                if not isinstance(name_v, str) or not name_v.strip():
                    name_v = Path(path_v).name
                kind_v = item.get('kind')
                kind_s = str(kind_v or '').strip()[:32]
                size_v = item.get('size_bytes')
                try:
                    size_bytes = int(size_v or 0)
                except Exception:
                    size_bytes = 0
                attachments.append(
                    {'path': path_v.strip(), 'name': str(name_v).strip(), 'kind': kind_s, 'size_bytes': size_bytes}
                )

        reply_to_raw = obj.get('reply_to')
        reply_to: dict[str, object] | None = None
        if isinstance(reply_to_raw, dict):
            cleaned: dict[str, object] = {}
            mid = reply_to_raw.get('message_id')
            sent_ts = reply_to_raw.get('sent_ts')
            reply_text = reply_to_raw.get('text')
            attachments_rt = reply_to_raw.get('attachments') or []
            if isinstance(mid, int):
                cleaned['message_id'] = int(mid)
            elif isinstance(mid, str) and mid.strip().isdigit():
                cleaned['message_id'] = int(mid.strip())
            if isinstance(sent_ts, (int, float)):
                cleaned['sent_ts'] = float(sent_ts)
            if isinstance(reply_text, str):
                cleaned['text'] = reply_text
            if isinstance(reply_to_raw.get('from_is_bot'), bool):
                cleaned['from_is_bot'] = bool(reply_to_raw.get('from_is_bot'))
            if isinstance(reply_to_raw.get('from_user_id'), (int, float)):
                cleaned['from_user_id'] = int(reply_to_raw.get('from_user_id') or 0)
            if isinstance(reply_to_raw.get('from_name'), str):
                cleaned['from_name'] = str(reply_to_raw.get('from_name') or '')
            quote_raw = reply_to_raw.get('quote')
            if isinstance(quote_raw, dict):
                quote_cleaned: dict[str, object] = {}
                q_text = quote_raw.get('text')
                if isinstance(q_text, str) and q_text.strip():
                    quote_cleaned['text'] = q_text.strip()
                q_pos = quote_raw.get('position')
                if isinstance(q_pos, (int, float)):
                    quote_cleaned['position'] = int(q_pos)
                q_is_manual = quote_raw.get('is_manual')
                if isinstance(q_is_manual, bool):
                    quote_cleaned['is_manual'] = bool(q_is_manual)
                if quote_cleaned:
                    cleaned['quote'] = quote_cleaned
            if isinstance(attachments_rt, list):
                cleaned_list: list[dict[str, object]] = []
                for a in attachments_rt:
                    if isinstance(a, dict):
                        path_v = a.get('path')
                        if isinstance(path_v, str) and path_v.strip():
                            cleaned_list.append({k: v for k, v in a.items()})
                if cleaned_list:
                    cleaned['attachments'] = cleaned_list
            if cleaned:
                reply_to = cleaned

        chat_meta_raw = obj.get('chat_meta')
        chat_meta: dict[str, object] | None = None
        if isinstance(chat_meta_raw, dict):
            cleaned_cm: dict[str, object] = {}
            for key in ('type', 'title', 'username', 'name'):
                v = chat_meta_raw.get(key)
                if isinstance(v, str) and v.strip():
                    cleaned_cm[key] = v.strip()
            if cleaned_cm:
                chat_meta = cleaned_cm

        user_meta_raw = obj.get('user_meta')
        user_meta: dict[str, object] | None = None
        if isinstance(user_meta_raw, dict):
            cleaned_um: dict[str, object] = {}
            for key in ('username', 'first_name', 'last_name', 'name'):
                v = user_meta_raw.get(key)
                if isinstance(v, str) and v.strip():
                    cleaned_um[key] = v.strip()
            if cleaned_um:
                user_meta = cleaned_um

        events.append(
            Event(
                kind=kind,
                chat_id=chat_id,
                chat_type=chat_type_s,
                message_thread_id=message_thread_id,
                user_id=user_id,
                text=text.strip(),
                chat_meta=chat_meta,
                user_meta=user_meta,
                attachments=tuple(attachments),
                reply_to=reply_to,
                message_id=message_id,
                callback_query_id=callback_query_id,
                received_ts=received_ts,
                ack_message_id=ack_message_id,
                synthetic=synthetic,
                queued_from_disk=True,
                spool_file=str(drain),
            )
        )

    return (events, drain)


def _normalize_cmd_token(s: str) -> str:
    def _force_prefixes() -> tuple[str, str, str]:
        # Keep in sync with BotConfig defaults; read env directly so spool helpers work before config init.
        danger = (os.getenv('ROUTER_FORCE_DANGEROUS_PREFIX') or '∆').strip() or '∆'
        write = (os.getenv('ROUTER_FORCE_WRITE_PREFIX') or '!').strip() or '!'
        read = (os.getenv('ROUTER_FORCE_READ_PREFIX') or '?').strip() or '?'
        return (danger, write, read)

    def _strip_prefixes(text: str) -> str:
        out = (text or '').lstrip()
        prefixes = _force_prefixes()
        while True:
            changed = False
            for p in prefixes:
                pref = (p or '').strip()
                if pref and out.startswith(pref):
                    out = out[len(pref) :].lstrip()
                    changed = True
            if not changed:
                break
        return out

    tok = _strip_prefixes(s).strip().split(maxsplit=1)[0].strip()
    if not tok.startswith('/'):
        return tok.casefold()
    if '@' in tok:
        tok = tok.split('@', 1)[0]
    return tok.casefold()


def _strip_bot_mention(text: str, *, bot_username: str) -> str:
    """Best-effort remove @BotName mention from a group message."""
    u = (bot_username or '').strip().lstrip('@')
    if not u:
        return (text or '').strip()
    pat = re.compile(rf'(?<!\w)@{re.escape(u)}(?!\w)', flags=re.IGNORECASE)
    out = pat.sub('', text or '', count=1)
    out = re.sub(r'^[\s,;:—–-]+', '', out)
    out = re.sub(r'\s{2,}', ' ', out)
    return out.strip()


def _spool_record_is_restart(obj: dict[str, object]) -> bool:
    kind = str(obj.get('kind') or '').strip()
    if kind != 'text':
        return False
    chat_id_raw = obj.get('chat_id')
    if isinstance(chat_id_raw, bool):
        chat_id = 0
    elif isinstance(chat_id_raw, (int, float)):
        chat_id = int(chat_id_raw)
    elif isinstance(chat_id_raw, str):
        try:
            chat_id = int(chat_id_raw.strip() or 0)
        except Exception:
            chat_id = 0
    else:
        chat_id = 0
    # Restart barrier should only apply to private chats (avoid group noise causing restart loops).
    if chat_id <= 0:
        return False
    text = obj.get('text')
    if not isinstance(text, str):
        return False
    return _normalize_cmd_token(text) == '/restart'


def _event_is_restart(ev: Event) -> bool:
    if ev.kind != 'text':
        return False
    # Restart barrier should only apply to private chats (avoid group noise causing restart loops).
    if int(ev.chat_id) <= 0:
        return False
    return _normalize_cmd_token(ev.text) == '/restart'


def _should_spool_during_restart(ev: Event) -> bool:
    """Return True if this event should be persisted during a graceful restart.

    Rationale: while `restart_pending` is active we spool new user work to disk to replay it after the restart.
    A repeated `/restart` is a no-op and must not be spooled, otherwise it can create restart loops after reboot.
    """
    try:
        if ev.kind == 'text' and _normalize_cmd_token(str(ev.text or '')) == '/restart':
            return False
    except Exception:
        pass
    return True


def _restart_ack_coalesce_key(*, chat_id: int, restart_message_id: int) -> str:
    cid = int(chat_id or 0)
    mid = int(restart_message_id or 0)
    if cid == 0 or mid <= 0:
        return ''
    return f'ack:{cid}:{mid}'


def _restart_ack_message_id_from_state(state: BotState, *, chat_id: int, restart_message_id: int) -> int:
    ck = _restart_ack_coalesce_key(chat_id=chat_id, restart_message_id=restart_message_id)
    if not ck:
        return 0
    try:
        mid = int(state.tg_message_id_for_coalesce_key(chat_id=int(chat_id), coalesce_key=ck) or 0)
    except Exception:
        mid = 0
    return int(mid) if int(mid) > 0 else 0


def _restart_queue_drained(*, scheduler: ParallelScheduler[SchedulableEvent]) -> bool:
    """Return True when the scheduler queue is empty (ignores in-flight running jobs).

    Rationale: `/restart` should not hang forever on stuck/hung running jobs; once the queue is drained,
    it's safer to restart and let the new process resume work (or let the user retry) than to wait forever.
    """
    try:
        snap_counts = dict(scheduler.snapshot(max_items=0))
    except Exception:
        return False

    def _i(v: object) -> int:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return int(v)
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str):
            try:
                return int(v.strip() or 0)
            except Exception:
                return 0
        return 0

    return (
        _i(snap_counts.get('main_n')) == 0
        and _i(snap_counts.get('prio_n')) == 0
        and _i(snap_counts.get('paused_n')) == 0
    )


def _atomic_write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        for line in lines:
            s = str(line or '')
            if not s:
                continue
            if not s.endswith('\n'):
                s += '\n'
            f.write(s)
    os.replace(tmp, path)


def _spool_consolidate_for_startup(spool_path: Path) -> Path | None:
    """Merge stale drain files + queue.jsonl into at most one drain file + optional remainder.

    Semantics:
    - We process ONLY up to the first `/restart` command (inclusive) to make `/restart` a barrier.
    - Everything after that stays in `queue.jsonl` for the next process after restart.
    """
    drain_glob = f'{spool_path.name}.drain.*.jsonl'
    drain_files = list(spool_path.parent.glob(drain_glob))
    drain_files.sort(key=_spool_drain_sort_key)
    sources: list[Path] = list(drain_files)
    if spool_path.exists():
        sources.append(spool_path)
    if not sources:
        return None

    lines: list[str] = []
    restart_idx: int | None = None
    for src in sources:
        try:
            raw = src.read_text(encoding='utf-8', errors='replace')
        except Exception:
            continue
        for line in raw.splitlines():
            s = (line or '').strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            kind = str(obj.get('kind') or '').strip()
            if kind not in {'text', 'callback'}:
                continue
            text = obj.get('text')
            if not isinstance(text, str) or not text.strip():
                continue
            try:
                if int(obj.get('chat_id') or 0) == 0 or int(obj.get('user_id') or 0) == 0:
                    continue
            except Exception:
                continue

            if restart_idx is None and _spool_record_is_restart(obj):
                restart_idx = len(lines)
            lines.append(s)

    # Cleanup garbage sources (best-effort).
    if not lines:
        for src in sources:
            try:
                src.unlink()
            except Exception:
                pass
        return None

    if restart_idx is None:
        prefix = lines
        suffix: list[str] = []
    else:
        prefix = lines[: restart_idx + 1]
        suffix = lines[restart_idx + 1 :]

    ts = int(time.time())
    drain_now = spool_path.with_name(f'{spool_path.name}.drain.{ts}.jsonl')
    _atomic_write_lines(drain_now, prefix)
    if suffix:
        _atomic_write_lines(spool_path, suffix)
    else:
        try:
            spool_path.unlink()
        except Exception:
            pass

    # Remove old sources (we just wrote consolidated files).
    for src in sources:
        if src == drain_now or src == spool_path:
            continue
        try:
            src.unlink()
        except Exception:
            pass

    return drain_now


def _sanitize_filename(name: str, *, default: str = 'file') -> str:
    name = (name or '').strip()
    if not name:
        name = default
    name = name.replace('\\', '/').split('/')[-1].strip()
    name = re.sub(r'[^\w.\-]+', '_', name, flags=re.UNICODE).strip('._')
    if not name:
        name = default
    if len(name) > 160:
        root, dot, ext = name.rpartition('.')
        if dot and ext:
            ext = ext[:12]
            root = root[: 160 - (len(ext) + 1)]
            name = f'{root}.{ext}'
        else:
            name = name[:160]
    return name


def _extract_tg_quote(msg: dict[str, object]) -> dict[str, object] | None:
    """Extract Telegram quote metadata (selected fragment in reply), best-effort."""
    quote_raw: object = None
    for key in ('quote', 'text_quote', 'reply_quote'):
        if key in msg:
            quote_raw = msg.get(key)
            break
    if not isinstance(quote_raw, dict):
        return None

    out: dict[str, object] = {}
    text = quote_raw.get('text')
    if isinstance(text, str) and text.strip():
        out['text'] = text.strip()

    pos = quote_raw.get('position')
    if not isinstance(pos, (int, float)):
        pos = quote_raw.get('offset')
    if isinstance(pos, (int, float)):
        out['position'] = int(pos)

    is_manual = quote_raw.get('is_manual')
    if isinstance(is_manual, bool):
        out['is_manual'] = bool(is_manual)

    return out or None


def _best_photo_file_id(photo_list: object) -> tuple[str, int]:
    """Return (file_id, file_size) for the best photo variant."""
    if not isinstance(photo_list, list) or not photo_list:
        return ('', 0)
    best_id = ''
    best_score = -1
    best_size = 0
    for item in photo_list:
        if not isinstance(item, dict):
            continue
        fid = item.get('file_id')
        if not isinstance(fid, str) or not fid.strip():
            continue
        try:
            w = int(item.get('width') or 0)
            h = int(item.get('height') or 0)
        except Exception:
            w = 0
            h = 0
        try:
            fs = int(item.get('file_size') or 0)
        except Exception:
            fs = 0
        score = fs if fs > 0 else (w * h)
        if score > best_score:
            best_score = score
            best_id = fid.strip()
            best_size = fs
    return (best_id, best_size)


def _download_tg_attachments(
    *,
    api: TelegramDeliveryAPI,
    repo_root: Path,
    uploads_root: Path,
    chat_id: int,
    message_id: int,
    msg: dict[str, Any],
    max_bytes: int,
) -> tuple[list[dict[str, object]], list[str]]:
    """Download known Telegram attachments in this message and return (attachments, errors)."""
    out: list[dict[str, object]] = []
    errors: list[str] = []
    ts = time.strftime('%Y%m%d-%H%M%S')
    chat_dir = uploads_root / str(int(chat_id))

    def _save_one(*, file_id: str, name_hint: str, kind: str, size_hint: int = 0) -> None:
        try:
            if not file_id:
                return
            # Pre-check based on hint (Telegram may provide file_size in the message).
            if max_bytes > 0 and size_hint > 0 and size_hint > max_bytes:
                raise RuntimeError(f'file too large ({size_hint} bytes > {max_bytes})')

            info = api.get_file(file_id=file_id, timeout=30)
            file_path = info.get('file_path')
            if not isinstance(file_path, str) or not file_path.strip():
                raise RuntimeError('getFile returned empty file_path')
            file_path = file_path.strip()

            name = name_hint.strip() if name_hint else ''
            if not name:
                name = Path(file_path).name
            name = _sanitize_filename(name, default=f'{kind}_{file_id}')
            prefix = f'{ts}_{int(message_id) or 0}_{file_id[:8]}'
            dest = chat_dir / f'{prefix}_{name}'
            # Avoid collisions (best-effort).
            if dest.exists():
                dest = chat_dir / f'{prefix}_{int(time.time() * 1000)}_{name}'

            try:
                size_bytes = int(info.get('file_size') or 0)
            except Exception:
                size_bytes = 0
            api.download_file_to(file_path=file_path, dest_path=dest, timeout=120, max_bytes=max_bytes)

            try:
                rel = str(dest.relative_to(repo_root))
            except Exception:
                rel = str(dest)
            out.append({'path': rel, 'name': name, 'kind': kind, 'size_bytes': size_bytes})
        except Exception as e:
            errors.append(f'{kind}: {e}')

    # 1) document
    doc = msg.get('document')
    if isinstance(doc, dict):
        fid = doc.get('file_id')
        name = doc.get('file_name')
        try:
            size_hint = int(doc.get('file_size') or 0)
        except Exception:
            size_hint = 0
        if isinstance(fid, str) and fid.strip():
            _save_one(file_id=fid.strip(), name_hint=str(name or ''), kind='document', size_hint=size_hint)

    # 2) photo (choose best variant)
    photos = msg.get('photo')
    fid, size_hint = _best_photo_file_id(photos)
    if fid:
        _save_one(file_id=fid, name_hint=f'photo_{fid[:12]}.jpg', kind='photo', size_hint=size_hint)

    # 3) video
    video = msg.get('video')
    if isinstance(video, dict):
        fid = video.get('file_id')
        name = video.get('file_name') or ''
        try:
            size_hint = int(video.get('file_size') or 0)
        except Exception:
            size_hint = 0
        if isinstance(fid, str) and fid.strip():
            _save_one(file_id=fid.strip(), name_hint=str(name or ''), kind='video', size_hint=size_hint)

    # 4) audio
    audio = msg.get('audio')
    if isinstance(audio, dict):
        fid = audio.get('file_id')
        name = audio.get('file_name') or ''
        try:
            size_hint = int(audio.get('file_size') or 0)
        except Exception:
            size_hint = 0
        if isinstance(fid, str) and fid.strip():
            _save_one(file_id=fid.strip(), name_hint=str(name or ''), kind='audio', size_hint=size_hint)

    # 5) voice (no file_name)
    voice = msg.get('voice')
    if isinstance(voice, dict):
        fid = voice.get('file_id')
        try:
            size_hint = int(voice.get('file_size') or 0)
        except Exception:
            size_hint = 0
        if isinstance(fid, str) and fid.strip():
            _save_one(file_id=fid.strip(), name_hint=f'voice_{fid[:12]}.ogg', kind='voice', size_hint=size_hint)

    return (out, errors)


def _tg_msg_is_forum_topic_created(msg: dict[str, object]) -> bool:
    val = msg.get('forum_topic_created')
    return isinstance(val, dict)


def _tg_msg_is_forum_topic_edited(msg: dict[str, object]) -> bool:
    val = msg.get('forum_topic_edited')
    return isinstance(val, dict)


def _tg_msg_has_known_attachments(msg: dict[str, object]) -> bool:
    if isinstance(msg.get('document'), dict):
        return True
    photos = msg.get('photo')
    if isinstance(photos, list) and bool(photos):
        return True
    if isinstance(msg.get('video'), dict):
        return True
    if isinstance(msg.get('audio'), dict):
        return True
    if isinstance(msg.get('voice'), dict):
        return True
    return False


def _merge_pending_attachments(
    *,
    state: BotState,
    chat_id: int,
    message_thread_id: int,
    attachments: list[dict[str, object]],
    reply_to: dict[str, object] | None,
) -> tuple[list[dict[str, object]], dict[str, object] | None]:
    pending: list[dict[str, object]] = []
    try:
        pending = state.take_pending_attachments(chat_id=chat_id, message_thread_id=message_thread_id)
    except Exception:
        pending = []

    pending_reply: dict[str, object] | None = None
    if pending:
        try:
            pending_reply = state.take_pending_reply_to(chat_id=chat_id, message_thread_id=message_thread_id)
        except Exception:
            pending_reply = None

    if pending_reply and not reply_to:
        reply_to = pending_reply

    if pending:
        return (list(pending) + list(attachments), reply_to)
    return (attachments, reply_to)


def _compact_speech2text_transcript(raw: str) -> str:
    """Convert Speech2Text raw export to a one-line user text.

    Expected formats:
      - "TEXT| TIME" + per-segment lines: "<text>| <start> - <end>"
      - diarization variants may include speaker column: "<text>| <start> - <end>| SPEAKER_01"
    """
    raw = (raw or '').replace('\r\n', '\n').strip()
    if not raw:
        return ''

    parts: list[str] = []
    for ln in raw.splitlines():
        s = ln.strip()
        if not s:
            continue
        if s.upper().startswith('TEXT|'):
            continue
        if '|' in s:
            seg = (s.split('|', 1)[0] or '').strip()
            if seg:
                parts.append(seg)
                continue
        parts.append(s)

    out = ' '.join(parts)
    out = re.sub(r'\s+', ' ', out).strip()
    return out


def _load_typos_glossary(path: Path) -> dict[str, str]:
    try:
        if not path.exists():
            return {}
        content = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return {}

    out: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.rstrip('\n')
        if not line.strip():
            continue
        if line.lstrip().startswith('#'):
            continue
        if '\t' not in line:
            continue
        typo, fix = line.split('\t', 1)
        typo = typo.strip()
        fix = fix.strip()
        if not typo or not fix:
            continue
        out[typo] = fix
    return out


def _apply_typos_glossary(text: str, entries: dict[str, str]) -> tuple[str, list[tuple[str, str]]]:
    if not text or not entries:
        return (text, [])

    updated = text
    applied: list[tuple[str, str]] = []
    for typo in sorted(entries.keys(), key=len, reverse=True):
        fix = entries[typo]
        if not typo or not fix:
            continue
        escaped = re.escape(typo).replace(r'\ ', r'\s+')
        pattern = re.compile(rf'(?<!\w){escaped}(?!\w)', flags=re.IGNORECASE)
        updated2, n = pattern.subn(fix, updated)
        if n > 0:
            updated = updated2
            applied.append((typo, fix))
    return (updated, applied)


def _speech2text_transcribe_via_cli(
    *,
    repo_root: Path,
    media_path: Path,
    timeout_s: int,
) -> str:
    script = (repo_root / 'scripts' / 'speech2text.py').resolve()
    if not script.exists():
        raise RuntimeError(f'speech2text script not found: {script}')
    if not media_path.exists():
        raise RuntimeError(f'media file not found: {media_path}')

    timeout_s = max(10, min(300, int(timeout_s)))
    cmd = [sys.executable, str(script), '--timeout', str(timeout_s), 'transcribe', str(media_path)]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s + 15, check=False)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f'speech2text timeout (>{timeout_s}s)') from e

    if p.returncode != 0:
        err = (p.stderr or p.stdout or '').strip()
        raise RuntimeError(err or f'speech2text failed with exit code {p.returncode}')

    return (p.stdout or '').strip()


def _tg_user_meta(frm: dict[str, object]) -> dict[str, object] | None:
    if not isinstance(frm, dict):
        return None
    first = frm.get('first_name')
    last = frm.get('last_name')
    username = frm.get('username')

    first_s = first.strip() if isinstance(first, str) else ''
    last_s = last.strip() if isinstance(last, str) else ''
    username_s = username.strip().lstrip('@') if isinstance(username, str) else ''

    name_parts = [p for p in [first_s, last_s] if p]
    display = ' '.join(name_parts).strip()
    if username_s:
        display = f'{display} (@{username_s})' if display else f'@{username_s}'

    out: dict[str, object] = {}
    if first_s:
        out['first_name'] = first_s
    if last_s:
        out['last_name'] = last_s
    if username_s:
        out['username'] = username_s
    if display:
        out['name'] = display
    return out or None


def _tg_chat_meta(chat: dict[str, object], *, chat_type: str) -> dict[str, object] | None:
    if not isinstance(chat, dict):
        return None
    ct = str(chat_type or '').strip().lower()
    title = ''
    username = ''
    if ct in {'group', 'supergroup', 'channel'}:
        t = chat.get('title')
        if isinstance(t, str) and t.strip():
            title = t.strip()
        u = chat.get('username')
        if isinstance(u, str) and u.strip():
            username = u.strip().lstrip('@')
    else:
        first = chat.get('first_name')
        last = chat.get('last_name')
        parts = [p.strip() for p in [first, last] if isinstance(p, str) and p.strip()]
        if parts:
            title = ' '.join(parts).strip()
        u = chat.get('username')
        if isinstance(u, str) and u.strip():
            username = u.strip().lstrip('@')
    display = title
    if username:
        display = f'{display} (@{username})' if display else f'@{username}'
    out: dict[str, object] = {}
    if ct:
        out['type'] = ct
    if title:
        out['title'] = title
    if username:
        out['username'] = username
    if display:
        out['name'] = display
    return out or None


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if v in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return default


def _acquire_single_instance_lock(lock_path: os.PathLike[str] | str) -> object | None:
    """Best-effort single-instance guard (Linux/macOS).

    Returns an open file handle which must be kept alive for the lock lifetime.
    """
    try:
        import fcntl
    except Exception:
        return None

    try:
        from pathlib import Path

        path = Path(lock_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        f = path.open('a+', encoding='utf-8')
    except Exception:
        return None

    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        try:
            f.close()
        except Exception:
            pass
        return None
    except Exception:
        try:
            f.close()
        except Exception:
            pass
        return None

    try:
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()))
        f.flush()
    except Exception:
        pass

    return f


def main() -> int:
    cfg = BotConfig.from_env()

    state = BotState(path=cfg.state_path)
    state.load()
    # Restart is a per-process action; if the previous instance exited with restart_pending=true,
    # clear the flag now so the fresh process can accept messages again (but keep enough info to
    # finalize the last "restarting…" notice after we have Telegram API available).
    restart_pending_on_boot = False
    try:
        restart_pending_on_boot = bool(state.is_restart_pending())
    except Exception:
        restart_pending_on_boot = False
    if restart_pending_on_boot:
        state.clear_restart_pending(preserve_request=True)

    tg_api_log_path = cfg.repo_root / 'logs' / 'tg-bot' / 'tg-api-endpoint.log'
    api_raw = TelegramAPI(
        token=cfg.tg_token,
        local_root_url=cfg.tg_bot_api_local_url,
        remote_root_url=cfg.tg_bot_api_remote_url,
        prefer_local=cfg.tg_bot_api_prefer_local,
        local_probe_seconds=cfg.tg_bot_api_probe_seconds,
        log_path=tg_api_log_path,
    )
    net_log_path = cfg.repo_root / 'logs' / 'tg-bot' / 'net.log'
    topic_log_root = cfg.repo_root / 'logs' / 'tg-bot' / 'topics'
    api = TelegramDeliveryAPI(api=api_raw, state=state, log_path=net_log_path, topic_log_root=topic_log_root)
    cb_log_path = cfg.repo_root / 'logs' / 'tg-bot' / 'callbacks.log'

    def _log_cb(event: dict[str, object]) -> None:
        """Append a compact callback-related debug record (best-effort)."""
        try:
            item = dict(event)
            item['ts'] = float(time.time())
            cb_log_path.parent.mkdir(parents=True, exist_ok=True)
            with cb_log_path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception:
            pass

    msg_log_path = cfg.repo_root / 'logs' / 'tg-bot' / 'messages.log'

    def _preview_text(v: object, max_chars: int = 240) -> str:
        if not isinstance(v, str):
            return ''
        s = v.replace('\n', ' ').strip()
        if max_chars > 0 and len(s) > max_chars:
            return s[: max(0, max_chars - 1)] + '…'
        return s

    def _log_msg(event: dict[str, object]) -> None:
        """Append a compact message-related debug record (best-effort)."""
        try:
            item = dict(event)
            item['ts'] = float(time.time())
            msg_log_path.parent.mkdir(parents=True, exist_ok=True)
            with msg_log_path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception:
            pass

    # Quick token validation (will raise if invalid).
    me = api.get_me()
    me_result = me.get('result') if isinstance(me, dict) else None
    if not isinstance(me_result, dict):
        me_result = {}
    username = str(me_result.get('username') or '?')
    bot_user_id = 0
    try:
        bot_user_id = int(me_result.get('id') or 0)
    except Exception:
        bot_user_id = 0
    bot_username = str(username or '').strip().lstrip('@')
    bot_username_cf = bot_username.casefold() if bot_username else ''
    bot_mention_re = (
        re.compile(rf'(?<!\w)@{re.escape(bot_username)}(?!\w)', flags=re.IGNORECASE) if bot_username else None
    )
    _log_cb({'kind': 'startup', 'pid': int(os.getpid()), 'bot_username': bot_username})

    # If the previous instance exited via graceful /restart and left the "restarting…" notice,
    # finalize it now (best-effort, queued via outbox on transient network issues).
    try:
        restart_chat_id, restart_thread_id, restart_reply_id, restart_ack_id = state.restart_target()
        restart_requested_ts = float(state.restart_requested_at() or 0.0)
        has_restart_target = int(restart_chat_id) > 0 and (int(restart_ack_id) > 0 or int(restart_reply_id) > 0)
        # Backward-compat: older versions didn't persist a shutdown marker.
        should_finalize_restart = bool(restart_requested_ts > 0 and has_restart_target)

        if should_finalize_restart:
            done_text = '✅ Перезапуск завершён. Я снова на связи. /status'
            finalized = False
            restart_ack_mid = _restart_ack_message_id_from_state(
                state, chat_id=int(restart_chat_id), restart_message_id=int(restart_reply_id)
            )
            if int(restart_ack_mid) <= 0 and int(restart_ack_id) > 0:
                restart_ack_mid = int(restart_ack_id)
            if int(restart_ack_mid) > 0:
                try:
                    api.edit_message_text(
                        chat_id=int(restart_chat_id),
                        message_id=int(restart_ack_mid),
                        text=done_text,
                    )
                    finalized = True
                except Exception:
                    finalized = False
            if not finalized:
                try:
                    api.send_message(
                        chat_id=int(restart_chat_id),
                        message_thread_id=(int(restart_thread_id) if int(restart_thread_id) > 0 else None),
                        text=done_text,
                        reply_to_message_id=int(restart_reply_id) or None,
                        timeout=10,
                    )
                    finalized = True
                except Exception:
                    finalized = False
            if finalized:
                state.clear_restart_pending()
    except Exception:
        pass

    log_path = cfg.repo_root / 'logs' / 'tg-bot' / 'codex.log'

    codex = CodexRunner(
        codex_bin=cfg.codex_bin,
        repo_root=cfg.repo_root,
        model=cfg.codex_model,
        timeout_seconds=cfg.codex_timeout_seconds,
        chat_profile=CodexProfile(
            name='chat',
            codex_home=cfg.codex_home_chat,
            sandbox=cfg.codex_chat_sandbox,
            full_auto=False,
        ),
        auto_profile=CodexProfile(
            name='auto',
            codex_home=cfg.codex_home_auto,
            sandbox=None,
            full_auto=cfg.codex_auto_full_auto,
        ),
        router_profile=CodexProfile(
            name='router',
            codex_home=cfg.codex_home_router,
            sandbox=cfg.codex_router_sandbox,
            full_auto=False,
        ),
        danger_profile=CodexProfile(
            name='danger',
            codex_home=cfg.codex_home_danger,
            sandbox='danger-full-access',
            full_auto=False,
        ),
        log_path=log_path,
    )

    watcher = Watcher(
        repo_root=cfg.repo_root,
        reminders_file=cfg.watch_reminders_file,
        owner_chat_id=cfg.tg_owner_chat_id,
        reminder_broadcast_chat_ids=cfg.watch_reminder_broadcast_chat_ids,
        reminders_include_weekends=cfg.watch_reminders_include_weekends,
        work_hours=cfg.watch_work_hours,
        include_weekends=cfg.watch_include_weekends,
        idle_minutes=cfg.watch_idle_minutes,
        ack_minutes=cfg.watch_ack_minutes,
        idle_stage_minutes=cfg.watch_idle_stage_minutes,
        grace_minutes=cfg.watch_reminder_grace_minutes,
        gentle_default_minutes=cfg.gentle_default_minutes,
        gentle_auto_idle_minutes=cfg.gentle_auto_idle_minutes,
        gentle_ping_cooldown_minutes=cfg.gentle_ping_cooldown_minutes,
        gentle_stage_cap=cfg.gentle_stage_cap,
        history_max_events=cfg.history_max_events,
        history_entry_max_chars=cfg.history_entry_max_chars,
    )

    mm_watcher: MattermostWatcher | None = None
    try:
        if bool(getattr(cfg, 'mm_enabled', False)):
            mm_watcher = MattermostWatcher(cfg)
    except Exception:
        mm_watcher = None

    workspaces = WorkspaceManager(
        main_repo_root=cfg.repo_root,
        owner_chat_id=cfg.tg_owner_chat_id,
        workspaces_dir=cfg.tg_workspaces_dir,
        owner_uploads_dir=cfg.tg_uploads_dir,
    )

    # Runtime queue admin hooks (/queue, /drop queue).
    # We keep them here (app.py) because queue state is in-memory.
    queue_admin_refs: dict[str, object] = {}
    queue_edit_lock = threading.Lock()
    queue_edit_state: dict[str, object] = {'active': False, 'ts': 0.0}
    spool_lock = threading.Lock()

    def _runtime_queue_edit_active() -> bool:
        with queue_edit_lock:
            return bool(queue_edit_state.get('active') or False)

    def _runtime_queue_edit_set(active: bool) -> None:
        a = bool(active)
        with queue_edit_lock:
            queue_edit_state['active'] = a
            queue_edit_state['ts'] = float(time.time()) if a else 0.0

    def _queue_event_summary(ev: SchedulableEvent) -> str:
        kind = str(getattr(ev, 'kind', None) or '?').strip() or '?'
        chat_id = int(getattr(ev, 'chat_id', 0) or 0)
        thread_id = int(getattr(ev, 'message_thread_id', 0) or 0)
        message_id = int(getattr(ev, 'message_id', 0) or 0)
        ack_id = int(getattr(ev, 'ack_message_id', 0) or 0)
        from_disk = bool(getattr(ev, 'queued_from_disk', False) or False)
        prefix = 'disk ' if from_disk else ''
        tid_s = f' tid={thread_id}' if thread_id > 0 else ''
        ack_s = f' ack={ack_id}' if ack_id > 0 else ''
        preview = _preview_text(str(getattr(ev, 'text', '') or ''), 120)
        if kind == 'callback':
            try:
                from . import keyboards

                label = keyboards.describe_callback_data(str(getattr(ev, 'text', '') or ''))
                if label:
                    preview = label
            except Exception:
                pass
        if preview:
            return f'{prefix}{kind} chat={chat_id}{tid_s} mid={message_id}{ack_s}: {preview}'
        return f'{prefix}{kind} chat={chat_id}{tid_s} mid={message_id}{ack_s}'

    def _runtime_queue_snapshot(max_items: int) -> dict[str, object]:
        try:
            lim = int(max_items)
        except Exception:
            lim = 5
        lim = max(0, lim)

        def _i(v: object) -> int:
            if isinstance(v, bool):
                return int(v)
            if isinstance(v, int):
                return int(v)
            if isinstance(v, float):
                return int(v)
            if isinstance(v, str):
                try:
                    return int(v.strip() or 0)
                except Exception:
                    return 0
            return 0

        in_flight = ''
        main_n = 0
        prio_n = 0
        paused_n = 0
        main_head: list[str] = []
        prio_head: list[str] = []
        paused_head: list[str] = []

        sched0 = queue_admin_refs.get('scheduler')
        if isinstance(sched0, ParallelScheduler):
            try:
                snap_sched = dict(sched0.snapshot(max_items=lim))
            except Exception:
                snap_sched = {}
            try:
                in_flight = str(snap_sched.get('in_flight') or '').strip()
            except Exception:
                in_flight = ''
            main_n = _i(snap_sched.get('main_n'))
            prio_n = _i(snap_sched.get('prio_n'))
            paused_n = _i(snap_sched.get('paused_n'))
            mh = snap_sched.get('main_head')
            ph = snap_sched.get('prio_head')
            pah = snap_sched.get('paused_head')
            if isinstance(mh, list):
                main_head = [str(x) for x in mh if isinstance(x, str)]
            if isinstance(ph, list):
                prio_head = [str(x) for x in ph if isinstance(x, str)]
            if isinstance(pah, list):
                paused_head = [str(x) for x in pah if isinstance(x, str)]

        restart_pending = False
        try:
            restart_pending = bool(state.is_restart_pending())
        except Exception:
            restart_pending = False

        # Best-effort: count + preview queued events in spool file (during /restart).
        spool_n = 0
        spool_truncated = False
        spool_head: list[str] = []
        try:
            from . import spool_admin

            spool_path = state.path.with_name('queue.jsonl')
            with spool_lock:
                spool_snap = dict(spool_admin.preview_spool(path=spool_path, max_items=lim))
            spool_n = _i(spool_snap.get('n'))
            try:
                head_raw = spool_snap.get('head') or []
                if isinstance(head_raw, list):
                    spool_head = [str(s).strip() for s in head_raw if isinstance(s, str) and s.strip()]
            except Exception:
                spool_head = []
            spool_truncated = bool(spool_snap.get('truncated') or False)
        except Exception:
            pass

        return {
            'in_flight': in_flight,
            'main_n': int(main_n),
            'prio_n': int(prio_n),
            'paused_n': int(paused_n),
            'main_head': main_head,
            'prio_head': prio_head,
            'paused_head': paused_head,
            'spool_n': int(spool_n),
            'spool_truncated': bool(spool_truncated),
            'spool_head': spool_head,
            'restart_pending': bool(restart_pending),
            'edit_active': bool(_runtime_queue_edit_active()),
        }

    def _runtime_queue_drop(kind: str) -> dict[str, object]:
        k = str(kind or '').strip().lower()
        if k != 'queue':
            return {}

        sched0 = queue_admin_refs.get('scheduler')
        if not isinstance(sched0, ParallelScheduler):
            return {'main': 0, 'prio': 0, 'paused': 0}
        try:
            return dict(sched0.drop_all())
        except Exception:
            return {'main': 0, 'prio': 0, 'paused': 0}

    def _runtime_queue_mutate(bucket: str, action: str, index: int) -> dict[str, object]:
        b = str(bucket or '').strip().lower()
        if b == 'spool':
            a = str(action or '').strip().lower()
            if a not in {'del', 'delete', 'rm'}:
                return {'ok': False, 'error': 'bad_action'}
            try:
                from . import spool_admin

                spool_path = state.path.with_name('queue.jsonl')
                with spool_lock:
                    return dict(spool_admin.delete_spool_item(path=spool_path, index=index))
            except Exception:
                return {'ok': False, 'error': 'exception'}

        if b != 'main':
            return {'ok': False, 'error': 'readonly_bucket'}

        sched0 = queue_admin_refs.get('scheduler')
        if not isinstance(sched0, ParallelScheduler):
            return {'ok': False, 'error': 'queue_missing'}
        return dict(sched0.mutate_main(action=str(action or ''), index=index))

    scheduler: ParallelScheduler[SchedulableEvent] = ParallelScheduler(
        max_parallel_jobs=cfg.tg_max_parallel_jobs,
        summarize=_queue_event_summary,
    )
    queue_admin_refs['scheduler'] = scheduler

    router = Router(
        api=api,
        state=state,
        codex=codex,
        watcher=watcher,
        workspaces=workspaces,
        owner_chat_id=cfg.tg_owner_chat_id,
        router_mode=cfg.router_mode,
        min_profile=cfg.router_min_profile,
        force_write_prefix=cfg.router_force_write_prefix,
        force_read_prefix=cfg.router_force_read_prefix,
        force_danger_prefix=cfg.router_force_danger_prefix,
        confidence_threshold=cfg.router_confidence_threshold,
        debug=cfg.router_debug,
        dangerous_auto=cfg.router_dangerous_auto,
        tg_typing_enabled=cfg.tg_typing_enabled,
        tg_typing_interval_seconds=cfg.tg_typing_interval_seconds,
        tg_progress_edit_enabled=cfg.tg_progress_edit_enabled,
        tg_progress_edit_interval_seconds=cfg.tg_progress_edit_interval_seconds,
        tg_codex_parse_mode=cfg.tg_codex_parse_mode,
        fallback_patterns=cfg.automation_patterns,
        gentle_default_minutes=cfg.gentle_default_minutes,
        gentle_auto_mute_window_minutes=cfg.gentle_auto_mute_window_minutes,
        gentle_auto_mute_count=cfg.gentle_auto_mute_count,
        history_max_events=cfg.history_max_events,
        history_context_limit=cfg.history_context_limit,
        history_entry_max_chars=cfg.history_entry_max_chars,
        codex_followup_sandbox=cfg.codex_followup_sandbox,
        tg_voice_route_choice_timeout_seconds=cfg.tg_voice_route_choice_timeout_seconds,
        runtime_queue_snapshot=_runtime_queue_snapshot,
        runtime_queue_drop=_runtime_queue_drop,
        runtime_queue_mutate=_runtime_queue_mutate,
        runtime_queue_edit_active=_runtime_queue_edit_active,
        runtime_queue_edit_set=_runtime_queue_edit_set,
    )

    # Guard against running multiple bot processes at once:
    # parallel pollers can cause "lost" updates and parallel Codex runs can corrupt per-scope resume state.
    lock_handle: object | None = None
    if _env_bool('TG_SINGLE_INSTANCE', True):
        lock_path = cfg.state_path.with_suffix(cfg.state_path.suffix + '.lock')
        lock_handle = _acquire_single_instance_lock(lock_path)
        if lock_handle is None:
            print(f'tg_bot: another instance is already running (lock: {lock_path})')
            return 2

    stop = threading.Event()

    pause_lock = threading.Lock()
    pause_state: dict[str, bool | float] = {'active': False, 'ts': 0.0}

    def _pause_is_active() -> tuple[bool, float]:
        with pause_lock:
            active = bool(pause_state.get('active') or False)
            ts = float(pause_state.get('ts') or 0.0)
        return (active, ts)

    def _pause_activate(*, barrier_ts: float) -> None:
        ts = float(barrier_ts or 0.0)
        if ts <= 0:
            ts = time.time()
        with pause_lock:
            pause_state['active'] = True
            pause_state['ts'] = max(float(pause_state.get('ts') or 0.0), ts)

    def _pause_clear() -> None:
        with pause_lock:
            pause_state['active'] = False
            pause_state['ts'] = 0.0

    spool_path = _queue_spool_path(cfg.state_path)
    spool_drain_remaining_by_path: dict[str, int] = {}
    spool_drain_remaining_lock = threading.Lock()
    restart_queue_next_pos = 0
    try:
        # Consolidate stale drains + queue.jsonl, but stop at the first /restart (barrier semantics).
        drain_now = _spool_consolidate_for_startup(spool_path)
        if drain_now is not None and drain_now.exists():
            spooled, drain = _spool_load(drain_now, max_events=None, rename_to_drain=False)
            if spooled and drain is not None:
                for ev in spooled:
                    try:
                        scheduler.enqueue(ev)
                    except Exception:
                        break
                spool_drain_remaining_by_path[str(drain)] = len(spooled)
                # If the loaded batch includes a queued `/restart`, treat the process as already "restarting":
                # any new Telegram messages should be spooled to disk and processed after the restart barrier.
                try:
                    last = spooled[-1]
                    if _event_is_restart(last):
                        state.request_restart(
                            chat_id=last.chat_id,
                            message_thread_id=last.message_thread_id,
                            user_id=last.user_id,
                            message_id=last.message_id,
                            ack_message_id=last.ack_message_id,
                        )
                except Exception:
                    pass
            else:
                try:
                    drain_now.unlink()
                except Exception:
                    pass
    except Exception:
        pass

    def authorized(chat_id: int, user_id: int, chat_type: str) -> bool:
        chat_id_i = int(chat_id)
        user_id_i = int(user_id)
        chat_type_s = str(chat_type or '').strip().lower()

        owner_chat_id = int(cfg.tg_owner_chat_id or 0)
        if owner_chat_id and chat_id_i == owner_chat_id:
            return True

        allowed_by_user = bool(cfg.tg_allowed_user_ids and user_id_i in cfg.tg_allowed_user_ids)
        allowed_by_chat = bool(cfg.tg_allowed_chat_ids and chat_id_i in cfg.tg_allowed_chat_ids)

        # Multi-tenant mode: default deny for non-owner chats unless explicitly allowed.
        if owner_chat_id:
            if chat_type_s in {'group', 'supergroup', 'channel'}:
                return allowed_by_chat
            return allowed_by_user or allowed_by_chat

        # Single-tenant mode: honor allow lists when present; otherwise fail-open.
        if cfg.tg_allowed_user_ids or cfg.tg_allowed_chat_ids:
            return allowed_by_user or allowed_by_chat

        return True

    def poll_loop() -> None:
        nonlocal restart_queue_next_pos

        while not stop.is_set():
            try:
                updates = api.get_updates(offset=state.tg_offset or None, timeout=cfg.tg_poll_timeout_seconds)
            except Exception:
                time.sleep(2.0)
                continue

            if not updates:
                continue

            max_update_id = None
            for upd in updates:
                uid = upd.get('update_id')
                if isinstance(uid, int):
                    max_update_id = max(max_update_id or 0, uid)

                # 1) Normal text messages
                msg = upd.get('message')
                if isinstance(msg, dict):
                    if _tg_msg_is_forum_topic_created(msg) or _tg_msg_is_forum_topic_edited(msg):
                        continue

                    text = msg.get('text')
                    if not isinstance(text, str):
                        # Many Telegram messages carry the text in `caption` (photos, docs, etc).
                        text = msg.get('caption')

                    chat = msg.get('chat') or {}
                    frm = msg.get('from') or {}
                    chat_id = int(chat.get('id') or 0)
                    chat_type_s = str(chat.get('type') or '').strip().lower()
                    user_id = int(frm.get('id') or 0)
                    message_id = int(msg.get('message_id') or 0)
                    try:
                        message_thread_id = int(msg.get('message_thread_id') or 0)
                    except Exception:
                        message_thread_id = 0
                    msg_thread_id = message_thread_id if int(message_thread_id or 0) > 0 else None
                    if chat_id == 0 or user_id == 0:
                        continue

                    chat_meta = _tg_chat_meta(chat, chat_type=chat_type_s)
                    user_meta = _tg_user_meta(frm)

                    cmd_token = ''
                    if isinstance(text, str) and text.strip():
                        cand = _normalize_cmd_token(text)
                        if cand.startswith('/'):
                            cmd_token = cand

                    is_auth = authorized(chat_id, user_id, chat_type_s)
                    if not is_auth:
                        # Avoid spamming group chats: reply only to commands.
                        if cmd_token in {'/id', '/whoami'}:
                            try:
                                api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=msg_thread_id,
                                    text=(
                                        '🪪 Идентификаторы\n'
                                        f'- chat_id: {int(chat_id)}\n'
                                        f'- chat_type: {chat_type_s or "?"}\n'
                                        f'- user_id: {int(user_id)}\n\n'
                                        'Чтобы разрешить личку:\n'
                                        f'TG_ALLOWED_USER_IDS="{int(user_id)}"\n'
                                        'Чтобы разрешить групповой чат:\n'
                                        f'TG_ALLOWED_CHAT_IDS="{int(chat_id)}"'
                                    ),
                                    reply_to_message_id=message_id or None,
                                    timeout=10,
                                )
                            except Exception:
                                pass
                        elif cmd_token:
                            try:
                                api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=msg_thread_id,
                                    text=(
                                        '⛔️ Not authorized.\n'
                                        "Открой /id (работает без авторизации) и попроси owner'а добавить этот чат/пользователя в whitelist."
                                    ),
                                    reply_to_message_id=message_id or None,
                                    timeout=10,
                                )
                            except Exception:
                                pass
                        continue

                    # Group chats can be noisy: only react to explicit triggers.
                    # - commands (`/...`)
                    # - mentions (`@BotName ...`)
                    # - replies to the bot (conversation continuation)
                    if chat_type_s in {'group', 'supergroup'}:
                        text_s = text if isinstance(text, str) else ''
                        is_command_msg = bool(_normalize_cmd_token(text_s).startswith('/'))
                        has_mention = bool(bot_mention_re.search(text_s)) if (bot_mention_re and text_s) else False
                        is_reply_to_bot = False
                        rt0 = msg.get('reply_to_message')
                        if isinstance(rt0, dict):
                            frm0 = rt0.get('from') or {}
                            if isinstance(frm0, dict) and bool(frm0.get('is_bot') or False):
                                reply_from_id = 0
                                try:
                                    reply_from_id = int(frm0.get('id') or 0)
                                except Exception:
                                    reply_from_id = 0
                                reply_user = frm0.get('username')
                                is_reply_to_bot = bool(
                                    (bot_user_id and reply_from_id == bot_user_id)
                                    or (
                                        isinstance(reply_user, str)
                                        and bot_username_cf
                                        and reply_user.strip().casefold() == bot_username_cf
                                    )
                                )

                        if not (is_command_msg or has_mention or is_reply_to_bot):
                            continue

                        if has_mention and isinstance(text, str):
                            stripped = _strip_bot_mention(text, bot_username=bot_username)
                            if not stripped and not _tg_msg_has_known_attachments(msg):
                                continue
                            text = stripped

                    sent_ts = 0.0
                    try:
                        d = msg.get('date')
                        if isinstance(d, (int, float)) and float(d) > 0:
                            sent_ts = float(d)
                    except Exception:
                        sent_ts = 0.0
                    received_ts = sent_ts if sent_ts > 0 else time.time()

                    paths = workspaces.ensure_workspace(chat_id)

                    # Download attachments (documents/photos/etc) if present.
                    attachments: list[dict[str, object]] = []
                    attachment_errors: list[str] = []
                    try:
                        attachments, attachment_errors = _download_tg_attachments(
                            api=api,
                            repo_root=paths.repo_root,
                            uploads_root=paths.uploads_root,
                            chat_id=chat_id,
                            message_id=message_id,
                            msg=msg,
                            max_bytes=cfg.tg_upload_max_bytes,
                        )
                    except Exception as e:
                        attachments = []
                        attachment_errors = [str(e)]
                    if attachment_errors:
                        err = attachment_errors[0]
                        if len(attachment_errors) > 1:
                            err = f'{err} (+{len(attachment_errors) - 1})'
                        try:
                            api.send_message(
                                chat_id=chat_id,
                                message_thread_id=msg_thread_id,
                                text=f'⚠️ Не смог скачать вложение: {err}',
                                reply_to_message_id=message_id or None,
                                timeout=10,
                            )
                        except Exception:
                            pass

                    if attachments and message_id > 0:
                        try:
                            state.remember_message_attachments(
                                chat_id=chat_id, message_id=message_id, attachments=attachments
                            )
                        except Exception:
                            pass

                    pending_total = 0
                    has_text = isinstance(text, str) and bool(text.strip())

                    is_voice_msg = isinstance(msg.get('voice'), dict)
                    voice_attachment = None
                    if attachments:
                        for a in attachments:
                            if not isinstance(a, dict):
                                continue
                            if str(a.get('kind') or '') != 'voice':
                                continue
                            voice_attachment = a
                            break
                    voice_autoroute_allowed = (not workspaces.is_multi_tenant()) or workspaces.is_owner_chat(chat_id)
                    voice_autoroute = bool(
                        cfg.tg_voice_auto_transcribe
                        and is_voice_msg
                        and (not has_text)
                        and bool(voice_attachment)
                        and voice_autoroute_allowed
                    )

                    if attachments and not has_text:
                        # "Waiting mode": user sent files without a caption.
                        try:
                            counts_for_watch = (
                                not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id)
                            ) and int(chat_id) > 0
                            state.mark_user_activity(
                                chat_id=chat_id,
                                user_id=user_id,
                                counts_for_watch=counts_for_watch,
                            )
                        except Exception:
                            pass
                        if not voice_autoroute:
                            try:
                                pending_total = state.add_pending_attachments(
                                    chat_id=chat_id, message_thread_id=message_thread_id, attachments=attachments
                                )
                            except Exception:
                                pending_total = 0

                    cmd = ''
                    text_for_cmd = ''
                    if isinstance(text, str):
                        text_stripped = text.strip()
                        text_for_cmd = text_stripped
                        prefixes = (
                            cfg.router_force_danger_prefix,
                            cfg.router_force_write_prefix,
                            cfg.router_force_read_prefix,
                        )
                        while True:
                            changed = False
                            for p in prefixes:
                                pref = (p or '').strip()
                                if pref and text_for_cmd.startswith(pref):
                                    text_for_cmd = text_for_cmd[len(pref) :].lstrip()
                                    changed = True
                            if not changed:
                                break
                        cand = _normalize_cmd_token(text_for_cmd)
                        if cand.startswith('/'):
                            cmd = cand
                    else:
                        text_stripped = ''
                        text_for_cmd = ''

                    # Immediate interrupt: /pause cancels the currently running Codex subprocess.
                    # Must be handled here (poll thread), otherwise it would wait in the queue behind the long Codex run.
                    # In multi-tenant mode this is owner-only to avoid cross-chat interference.
                    if (
                        cmd == '/pause'
                        and int(chat_id) > 0
                        and chat_type_s == 'private'
                        and (not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id))
                    ):
                        try:
                            counts_for_watch = (
                                not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id)
                            ) and int(chat_id) > 0
                            state.mark_user_activity(
                                chat_id=chat_id,
                                user_id=user_id,
                                counts_for_watch=counts_for_watch,
                            )
                        except Exception:
                            pass
                        try:
                            session_key = (
                                f'{int(chat_id)}:{int(message_thread_id)}'
                                if int(message_thread_id or 0) > 0
                                else str(int(chat_id))
                            )
                            res = codex.cancel_current_run(chat_id=chat_id, session_key=session_key)
                            ok = bool(isinstance(res, dict) and res.get('ok') is True)
                        except Exception:
                            ok = False
                        # Queue barrier: after /pause we should not start draining already queued messages
                        # until the user sends a follow-up (finish current thread first).
                        if ok and not state.is_restart_pending():
                            try:
                                _pause_activate(barrier_ts=float(received_ts or 0.0))
                            except Exception:
                                pass
                        try:
                            if ok:
                                api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=msg_thread_id,
                                    text=(
                                        '⏸️ Остановил текущий запуск Codex (SIGTERM). '
                                        'Очередь пока не продолжаю — сначала разберёмся с текущим сообщением. '
                                        'Пришли уточнение/повтори команду — обработаю их первыми.'
                                    ),
                                    reply_to_message_id=message_id or None,
                                    timeout=10,
                                )
                            else:
                                api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=msg_thread_id,
                                    text='⏸️ Сейчас Codex ничего не выполняет.',
                                    reply_to_message_id=message_id or None,
                                    timeout=10,
                                )
                        except Exception:
                            pass
                        continue

                    # Immediate control-plane commands (no Codex).
                    # Must be handled here (poll thread), otherwise they become useless when the main worker
                    # is busy with a long Codex run (they'd sit in the queue and show a stale state).
                    #
                    # NOTE: /restart and /reset intentionally respect the queue (handled by the worker).
                    immediate_cmds_public = {'/start', '/help', '/id', '/whoami', '/status'}
                    immediate_cmds_owner_private = {
                        '/admin',
                        '/queue',
                        '/drop',
                        '/stats',
                        '/doctor',
                        '/upload',
                        '/mute',
                        '/back',
                        '/lunch',
                        '/gentle',
                        '/settings',
                    }

                    if cmd in immediate_cmds_public or (
                        cmd in immediate_cmds_owner_private
                        and int(chat_id) > 0
                        and chat_type_s == 'private'
                        and (not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id))
                    ):
                        try:
                            router.handle_text(
                                chat_id=chat_id,
                                message_thread_id=message_thread_id,
                                user_id=user_id,
                                text=text_stripped,
                                attachments=attachments,
                                reply_to=None,
                                message_id=message_id,
                                received_ts=received_ts,
                                ack_message_id=0,
                                tg_chat=chat_meta,
                                tg_user=user_meta,
                            )
                        except Exception:
                            pass
                        continue

                    reply_to: dict[str, object] | None = None
                    rt = msg.get('reply_to_message')
                    if isinstance(rt, dict):
                        quote = None
                        try:
                            quote = _extract_tg_quote(msg)
                        except Exception:
                            quote = None
                        try:
                            reply_mid = int(rt.get('message_id') or 0)
                        except Exception:
                            reply_mid = 0
                        reply_sent_ts = 0.0
                        try:
                            d = rt.get('date')
                            if isinstance(d, (int, float)) and float(d) > 0:
                                reply_sent_ts = float(d)
                        except Exception:
                            reply_sent_ts = 0.0
                        reply_text = rt.get('text')
                        if not isinstance(reply_text, str):
                            reply_text = rt.get('caption')
                        reply_text_s = reply_text.strip() if isinstance(reply_text, str) else ''

                        frm2 = rt.get('from') or {}
                        from_is_bot = bool(frm2.get('is_bot') or False) if isinstance(frm2, dict) else False
                        from_user_id = 0
                        from_name = ''
                        if isinstance(frm2, dict):
                            try:
                                from_user_id = int(frm2.get('id') or 0)
                            except Exception:
                                from_user_id = 0
                            if isinstance(frm2.get('username'), str) and frm2.get('username'):
                                from_name = f'@{frm2.get("username")}'
                            else:
                                first = frm2.get('first_name')
                                last = frm2.get('last_name')
                                parts = [p for p in [first, last] if isinstance(p, str) and p.strip()]
                                from_name = ' '.join(parts).strip()

                        reply_attachments: list[dict[str, object]] = []
                        if reply_mid > 0:
                            try:
                                reply_attachments = list(
                                    state.get_message_attachments(chat_id=chat_id, message_id=reply_mid)
                                )
                            except Exception:
                                reply_attachments = []
                        if reply_mid > 0 and not reply_attachments:
                            ra, ra_errs = _download_tg_attachments(
                                api=api,
                                repo_root=paths.repo_root,
                                uploads_root=paths.uploads_root,
                                chat_id=chat_id,
                                message_id=reply_mid,
                                msg=rt,
                                max_bytes=cfg.tg_upload_max_bytes,
                            )
                            if ra:
                                reply_attachments = list(ra)
                                try:
                                    state.remember_message_attachments(
                                        chat_id=chat_id, message_id=reply_mid, attachments=ra
                                    )
                                except Exception:
                                    pass
                            if ra_errs:
                                err = ra_errs[0]
                                if len(ra_errs) > 1:
                                    err = f'{err} (+{len(ra_errs) - 1})'
                                try:
                                    api.send_message(
                                        chat_id=chat_id,
                                        message_thread_id=msg_thread_id,
                                        text=f'⚠️ Не смог скачать вложение из цитируемого сообщения: {err}',
                                        reply_to_message_id=message_id or None,
                                        timeout=10,
                                    )
                                except Exception:
                                    pass

                        reply_to = {
                            'message_id': int(reply_mid),
                            'sent_ts': float(reply_sent_ts),
                            'from_is_bot': bool(from_is_bot),
                            'from_user_id': int(from_user_id),
                            'from_name': str(from_name),
                            'text': str(reply_text_s),
                            'attachments': list(reply_attachments),
                        }
                        if quote:
                            reply_to['quote'] = quote

                    if attachments and not has_text and reply_to:
                        try:
                            state.set_pending_reply_to(chat_id=chat_id, reply_to=reply_to)
                        except Exception:
                            pass

                    # /ask: send a prompt to Codex via a command (useful in group chats with Telegram privacy mode).
                    # Example: /ask@BotName please summarize ...
                    if cmd == '/ask':
                        base = text_for_cmd or text_stripped
                        tok = ''
                        if isinstance(base, str) and base:
                            try:
                                tok = (base.split(maxsplit=1)[0] or '').strip()
                            except Exception:
                                tok = ''
                        rest = (base[len(tok) :] if tok else '').lstrip()

                        if not rest:
                            if attachments:
                                try:
                                    counts_for_watch = (
                                        not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id)
                                    ) and int(chat_id) > 0
                                    state.mark_user_activity(
                                        chat_id=chat_id,
                                        user_id=user_id,
                                        counts_for_watch=counts_for_watch,
                                    )
                                except Exception:
                                    pass
                                try:
                                    pending_total = state.add_pending_attachments(
                                        chat_id=chat_id, message_thread_id=message_thread_id, attachments=attachments
                                    )
                                except Exception:
                                    pending_total = 0
                                if reply_to:
                                    try:
                                        state.set_pending_reply_to(
                                            chat_id=chat_id, message_thread_id=message_thread_id, reply_to=reply_to
                                        )
                                    except Exception:
                                        pass
                                try:
                                    api.send_message(
                                        chat_id=chat_id,
                                        message_thread_id=msg_thread_id,
                                        text=(
                                            f'📎 Файлы сохранил (в ожидании: {int(pending_total)}).\n'
                                            'Пришли следующий текст командой /ask — отправлю его вместе с файлами.'
                                        ),
                                        reply_to_message_id=message_id or None,
                                        timeout=10,
                                    )
                                except Exception:
                                    pass
                            else:
                                try:
                                    api.send_message(
                                        chat_id=chat_id,
                                        message_thread_id=msg_thread_id,
                                        text=(
                                            'ℹ️ Как отправить запрос в Codex:\n'
                                            '- /ask <текст> (в группах: /ask@BotName <текст>)\n'
                                            '- либо reply на сообщение бота'
                                        ),
                                        reply_to_message_id=message_id or None,
                                        timeout=10,
                                    )
                                except Exception:
                                    pass
                            continue

                        text = rest
                        text_stripped = rest
                        has_text = True
                        cmd = ''

                    was_restart_pending = state.is_restart_pending()
                    restart_pending = was_restart_pending
                    # /restart is a barrier and must be owner-only in multi-tenant mode.
                    if (
                        cmd == '/restart'
                        and int(chat_id) > 0
                        and not was_restart_pending
                        and (not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id))
                    ):
                        try:
                            snap_counts = dict(scheduler.snapshot(max_items=0))

                            def _i(v: object) -> int:
                                if isinstance(v, bool):
                                    return int(v)
                                if isinstance(v, int):
                                    return int(v)
                                if isinstance(v, float):
                                    return int(v)
                                if isinstance(v, str):
                                    try:
                                        return int(v.strip() or 0)
                                    except Exception:
                                        return 0
                                return 0

                            pending_total = (
                                _i(snap_counts.get('main_n'))
                                + _i(snap_counts.get('prio_n'))
                                + _i(snap_counts.get('paused_n'))
                            )
                            restart_queue_next_pos = int(pending_total) + 2
                        except Exception:
                            restart_queue_next_pos = 2
                        state.request_restart(
                            chat_id=chat_id,
                            message_thread_id=message_thread_id,
                            user_id=user_id,
                            message_id=message_id,
                        )
                        restart_pending = True

                    scope_running = False
                    try:
                        scope_running = (
                            int(chat_id),
                            int(message_thread_id or 0),
                        ) in scheduler.running_scopes_snapshot()
                    except Exception:
                        scope_running = False

                    # Fast ack (including commands) so user sees that the bot is alive even if Codex is busy.
                    ack_message_id = 0
                    ack_reply_markup: dict[str, object] | None = None
                    voice_route_voice_message_id = 0
                    if cfg.tg_ack_enabled or (
                        cfg.tg_voice_route_choice_menu_enabled
                        and voice_autoroute
                        and attachments
                        and not has_text
                        and int(message_id or 0) > 0
                    ):
                        pos = 0
                        try:
                            snap_counts = dict(scheduler.snapshot(max_items=0))

                            def _i(v: object) -> int:
                                if isinstance(v, bool):
                                    return int(v)
                                if isinstance(v, int):
                                    return int(v)
                                if isinstance(v, float):
                                    return int(v)
                                if isinstance(v, str):
                                    try:
                                        return int(v.strip() or 0)
                                    except Exception:
                                        return 0
                                return 0

                            pending_total = (
                                _i(snap_counts.get('main_n'))
                                + _i(snap_counts.get('prio_n'))
                                + _i(snap_counts.get('paused_n'))
                            )
                            pos = int(pending_total) + 1
                            running_n = int(scheduler.running_count())
                        except Exception:
                            pos = 0
                            running_n = 0
                        if was_restart_pending:
                            if restart_queue_next_pos <= 0:
                                restart_queue_next_pos = max(pos, 1)
                            pos = int(restart_queue_next_pos)
                        queue_info = (
                            f'\nОчередь: #{pos} | В работе: {running_n}/{int(cfg.tg_max_parallel_jobs)}'
                            if (cfg.tg_ack_include_queue and pos > 0)
                            else ''
                        )
                        try:
                            if cmd:
                                base_cmd = text_for_cmd or (text.strip() if isinstance(text, str) else '')
                                cmd_s = (base_cmd.split(maxsplit=1)[0] or '/').strip()
                                cmd_norm = str(cmd or '').strip().casefold()
                                if (
                                    cmd_norm == '/restart'
                                    and int(chat_id) > 0
                                    and (not workspaces.is_multi_tenant() or workspaces.is_owner_chat(chat_id))
                                ):
                                    if was_restart_pending:
                                        ack_text = (
                                            '🕓 Уже перезапускаюсь. Повторный /restart игнорирую.'
                                            f'{queue_info}\n'
                                            'Дождись «Перезапуск завершён» или проверь /status.'
                                        )
                                    else:
                                        other_running = max(0, int(running_n) - (1 if scope_running else 0))
                                        wait_note = ''
                                        if other_running > 0:
                                            wait_note = f'🕓 Жду завершения задач в других топиках: {other_running}.\n'
                                        elif running_n > 0:
                                            wait_note = '🕓 Жду завершения активной задачи.\n'
                                        ack_text = (
                                            f'🔄 Принял команду {cmd_s}.{queue_info}\n'
                                            + wait_note
                                            + 'Дожидаюсь завершения очереди и активных задач, затем перезапускаюсь. '
                                            'Новые сообщения сохраню и обработаю после рестарта.'
                                        )
                                elif restart_pending:
                                    ack_text = (
                                        f'🕓 Перезапускаюсь. Команду {cmd_s} сохранил.{queue_info}\n'
                                        'Обработаю после рестарта.'
                                    )
                                else:
                                    ack_text = f'✅ Принял команду {cmd_s}.{queue_info}'
                            elif isinstance(text, str) and text.strip():
                                if restart_pending:
                                    ack_text = (
                                        f'🕓 Перезапускаюсь. Сообщение сохранил.{queue_info}\nОбработаю после рестарта.'
                                    )
                                else:
                                    if scope_running:
                                        ack_text = f'💬 Принял follow-up (в текущую задачу).{queue_info}'
                                    else:
                                        router_policy = f'router (--sandbox {cfg.codex_router_sandbox})'
                                        read_policy = f'read → chat (--sandbox {cfg.codex_chat_sandbox})'
                                        write_policy = (
                                            'write → auto (--full-auto)'
                                            if cfg.codex_auto_full_auto
                                            else 'write → auto (no --full-auto)'
                                        )
                                        ack_text = (
                                            '✅ Принял.'
                                            f'{queue_info}\n'
                                            f'План: {router_policy} → {read_policy} | {write_policy}\n'
                                            f'Сессия: {codex_resume_label(message_thread_id=message_thread_id)}'
                                        )
                            else:
                                if attachments and not has_text:
                                    if voice_autoroute:
                                        if cfg.tg_voice_route_choice_menu_enabled and int(message_id or 0) > 0:
                                            from . import keyboards

                                            voice_route_voice_message_id = int(message_id or 0)
                                            ack_reply_markup = keyboards.voice_route_menu(
                                                voice_message_id=voice_route_voice_message_id,
                                            )
                                        if restart_pending:
                                            ack_text = (
                                                '🕓 Перезапускаюсь. 🎙️ Голосовое принял.'
                                                f'{queue_info}\n'
                                                'Распознаю и обработаю после рестарта.\n'
                                                'Выбери режим в кнопках ниже (если не выбрать — по умолчанию без префикса).'
                                            )
                                        else:
                                            ack_text = (
                                                '🎙️ Принял голосовое.'
                                                f'{queue_info}\n'
                                                'Распознаю (Voice Recognition) и отправлю как текстовый запрос.\n'
                                                'Выбери режим в кнопках ниже (если не выбрать — по умолчанию без префикса).'
                                            )
                                    elif restart_pending:
                                        ack_text = (
                                            f'🕓 Перезапускаюсь. Файлы сохранил (в ожидании: {pending_total}).\n'
                                            'Пришли текст после рестарта — отправлю его вместе с файлами.'
                                        )
                                    else:
                                        ack_text = (
                                            f'📎 Файлы сохранил (в ожидании: {pending_total}).\n'
                                            'Пришли текст — отправлю его вместе с файлами.'
                                        )
                                else:
                                    ack_text = '✅ Принял. (Пока понимаю только текст и файлы с подписью.)'

                                resp = api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=(message_thread_id if int(message_thread_id or 0) > 0 else None),
                                    text=ack_text,
                                    reply_to_message_id=message_id or None,
                                    reply_markup=(
                                        dict(ack_reply_markup) if isinstance(ack_reply_markup, dict) else None
                                    ),
                                    coalesce_key=(
                                        f'ack:{int(chat_id)}:{int(message_id)}' if int(message_id or 0) > 0 else None
                                    ),
                                    timeout=10,
                                )
                            ack_message_id = int(
                                ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id') or 0
                            )
                            if ack_message_id > 0 and voice_route_voice_message_id > 0 and ack_reply_markup is not None:
                                try:
                                    state.init_pending_voice_route(
                                        chat_id=chat_id,
                                        message_thread_id=message_thread_id,
                                        voice_message_id=voice_route_voice_message_id,
                                    )
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    if (
                        voice_autoroute
                        and (not isinstance(text, str) or not text.strip())
                        and isinstance(voice_attachment, dict)
                    ):
                        voice_rel = str(voice_attachment.get('path') or '').strip()
                        if voice_rel:
                            try:
                                voice_path = Path(voice_rel)
                                if not voice_path.is_absolute():
                                    voice_path = (paths.repo_root / voice_path).resolve()

                                raw = _speech2text_transcribe_via_cli(
                                    repo_root=cfg.repo_root,
                                    media_path=voice_path,
                                    timeout_s=cfg.tg_voice_transcribe_timeout_seconds,
                                )
                                compact_raw = _compact_speech2text_transcript(raw)
                                if not compact_raw:
                                    raise RuntimeError('empty transcript')

                                compact = compact_raw
                                applied_typos: list[tuple[str, str]] = []
                                if cfg.tg_voice_apply_typos:
                                    typos_path = cfg.repo_root / 'notes' / 'work' / 'typos.md'
                                    typos = _load_typos_glossary(typos_path)
                                    compact, applied_typos = _apply_typos_glossary(compact, typos)
                                    compact = compact.strip()

                                if cfg.tg_voice_echo_transcript:
                                    preview = compact
                                    if len(preview) > 3000:
                                        preview = preview[:2999] + '…'
                                    msg = f'📝 Расшифровка:\n{preview}'
                                    if applied_typos:
                                        changes = ', '.join(f'{t}→{f}' for t, f in applied_typos[:5])
                                        suffix = f' (+{len(applied_typos) - 5})' if len(applied_typos) > 5 else ''
                                        msg += f'\n\n🛠️ Исправления (typos.md): {changes}{suffix}'
                                    api.send_message(
                                        chat_id=chat_id,
                                        message_thread_id=msg_thread_id,
                                        text=msg,
                                        reply_to_message_id=message_id or None,
                                        timeout=10,
                                    )

                                note = (
                                    '🎙️ Голосовое → Voice Recognition transcript (`speech2text`, diarization=false). '
                                    'Возможны ASR-опечатки; если смысл/задача не ясны — задай уточняющий вопрос.'
                                )
                                if applied_typos:
                                    note += f' (typo-fix: {len(applied_typos)})'

                                text = f'{note}\n\n{compact}'
                                has_text = True
                            except Exception as e:
                                try:
                                    api.send_message(
                                        chat_id=chat_id,
                                        message_thread_id=msg_thread_id,
                                        text=(
                                            '⚠️ Не смог распознать голосовое.\n'
                                            f'{e!r}\n\n'
                                            'Проверь токен: `python3 scripts/speech2text.py login ...` '
                                            '(или `SPEECH2TEXT_TOKEN=...`).'
                                        ),
                                        reply_to_message_id=message_id or None,
                                        timeout=10,
                                    )
                                except Exception:
                                    pass
                                continue

                    # File-only messages are handled via "waiting mode" above.
                    if not isinstance(text, str) or not text.strip():
                        continue

                    reply_for_event = reply_to
                    attachments_for_event = attachments
                    if not cmd:
                        attachments_for_event, reply_for_event = _merge_pending_attachments(
                            state=state,
                            chat_id=chat_id,
                            message_thread_id=message_thread_id,
                            attachments=attachments_for_event,
                            reply_to=reply_for_event,
                        )

                    ev_out = Event(
                        kind='text',
                        chat_id=chat_id,
                        chat_type=chat_type_s,
                        user_id=user_id,
                        text=text,
                        message_thread_id=message_thread_id,
                        chat_meta=chat_meta,
                        user_meta=user_meta,
                        attachments=tuple(attachments_for_event),
                        reply_to=reply_for_event,
                        message_id=message_id,
                        received_ts=received_ts,
                        ack_message_id=ack_message_id,
                    )

                    # Debug: log reply_to_message extraction vs the final event reply_to.
                    try:
                        rt0 = msg.get('reply_to_message') if isinstance(msg, dict) else None
                        raw_reply_mid = 0
                        raw_reply_text_preview = ''
                        raw_reply_from_is_bot = False
                        raw_reply_from_id = 0
                        raw_reply_from_username = ''
                        if isinstance(rt0, dict):
                            try:
                                raw_reply_mid = int(rt0.get('message_id') or 0)
                            except Exception:
                                raw_reply_mid = 0
                            raw_rt_text = rt0.get('text')
                            if not isinstance(raw_rt_text, str):
                                raw_rt_text = rt0.get('caption')
                            raw_reply_text_preview = _preview_text(raw_rt_text, 240)
                            frm0 = rt0.get('from') or {}
                            if isinstance(frm0, dict):
                                raw_reply_from_is_bot = bool(frm0.get('is_bot') or False)
                                try:
                                    raw_reply_from_id = int(frm0.get('id') or 0)
                                except Exception:
                                    raw_reply_from_id = 0
                                u = frm0.get('username')
                                if isinstance(u, str) and u.strip():
                                    raw_reply_from_username = f'@{u.strip()}'

                        ev_reply_mid = 0
                        ev_reply_text_len = 0
                        ev_reply_quote_len = 0
                        ev_reply_attachments = 0
                        if isinstance(reply_for_event, dict):
                            mid_raw = reply_for_event.get('message_id')
                            if isinstance(mid_raw, bool) or isinstance(mid_raw, (int, float)):
                                ev_reply_mid = int(mid_raw)
                            elif isinstance(mid_raw, str):
                                try:
                                    ev_reply_mid = int(mid_raw.strip() or 0)
                                except Exception:
                                    ev_reply_mid = 0
                            else:
                                ev_reply_mid = 0
                            ev_rt_text = reply_for_event.get('text')
                            if isinstance(ev_rt_text, str):
                                ev_reply_text_len = len(ev_rt_text.strip())
                            quote0 = reply_for_event.get('quote')
                            if isinstance(quote0, dict):
                                qt = quote0.get('text')
                                if isinstance(qt, str):
                                    ev_reply_quote_len = len(qt.strip())
                            at0 = reply_for_event.get('attachments') or []
                            if isinstance(at0, list):
                                ev_reply_attachments = len([a for a in at0 if isinstance(a, dict)])

                        _log_msg(
                            {
                                'kind': 'incoming_message',
                                'update_id': uid,
                                'chat_id': int(chat_id),
                                'chat_type': str(chat_type_s or ''),
                                'user_id': int(user_id),
                                'message_id': int(message_id or 0),
                                'cmd': str(cmd or ''),
                                'text': _preview_text(text, 240),
                                'attachments': len(attachments_for_event),
                                'raw_reply_present': bool(isinstance(rt0, dict)),
                                'raw_reply_mid': int(raw_reply_mid),
                                'raw_reply_from_is_bot': bool(raw_reply_from_is_bot),
                                'raw_reply_from_id': int(raw_reply_from_id),
                                'raw_reply_from_username': str(raw_reply_from_username),
                                'raw_reply_text': str(raw_reply_text_preview),
                                'event_reply_present': bool(
                                    isinstance(reply_for_event, dict) and bool(reply_for_event)
                                ),
                                'event_reply_mid': int(ev_reply_mid),
                                'event_reply_text_len': int(ev_reply_text_len),
                                'event_reply_quote_len': int(ev_reply_quote_len),
                                'event_reply_attachments': int(ev_reply_attachments),
                                'restart_pending': bool(was_restart_pending),
                            }
                        )
                        try:
                            uname = ''
                            if isinstance(user_meta, dict):
                                u = user_meta.get('username')
                                if isinstance(u, str) and u.strip():
                                    uname = u.strip()
                            api.log_incoming_message(
                                chat_id=int(chat_id),
                                message_thread_id=msg_thread_id,
                                chat_type=str(chat_type_s or ''),
                                user_id=int(user_id),
                                username=str(uname or ''),
                                message_id=int(message_id or 0),
                                cmd=str(cmd or ''),
                                text=text,
                                attachments=list(attachments_for_event) if attachments_for_event else None,
                                reply_to_message_id=(int(raw_reply_mid) if int(raw_reply_mid or 0) > 0 else None),
                            )
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # If we are ALREADY restarting, spool new work to disk so it can be replayed after restart.
                    if was_restart_pending:
                        if not _should_spool_during_restart(ev_out):
                            state.metric_inc('queue.text.ignored')
                            if not cfg.tg_ack_enabled or ack_message_id == 0:
                                try:
                                    api.send_message(
                                        chat_id=chat_id,
                                        message_thread_id=msg_thread_id,
                                        text='🕓 Уже перезапускаюсь. Повторный /restart игнорирую.',
                                        reply_to_message_id=message_id or None,
                                        timeout=10,
                                    )
                                except Exception:
                                    pass
                            continue
                        try:
                            with spool_lock:
                                _spool_append(spool_path, ev_out)
                            restart_queue_next_pos += 1
                            state.metric_inc('queue.text.spooled')
                        except Exception:
                            try:
                                api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=msg_thread_id,
                                    text='⚠️ Не смог сохранить сообщение на диск (во время рестарта). Сообщение может потеряться.',
                                    reply_to_message_id=message_id or None,
                                    timeout=10,
                                )
                            except Exception:
                                pass
                        if not cfg.tg_ack_enabled or ack_message_id == 0:
                            try:
                                api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=msg_thread_id,
                                    text='🕓 Перезапускаюсь. Сообщение сохранено и будет обработано после рестарта.',
                                    reply_to_message_id=message_id or None,
                                    timeout=10,
                                )
                            except Exception:
                                pass
                        continue

                    if scope_running and not cmd and state.ux_mcp_live_enabled(chat_id=chat_id):
                        try:
                            state.record_pending_followup(
                                chat_id=chat_id,
                                message_thread_id=message_thread_id,
                                message_id=message_id,
                                user_id=user_id,
                                received_ts=received_ts,
                                text=text,
                                attachments=list(attachments_for_event) if attachments_for_event else None,
                                reply_to=(reply_for_event if isinstance(reply_for_event, dict) else None),
                            )
                            state.metric_inc('followups.recorded')
                        except Exception:
                            pass

                    try:
                        scheduler.enqueue(ev_out)
                        state.metric_inc('queue.text.enqueued')
                    except Exception:
                        try:
                            api.send_message(
                                chat_id=chat_id,
                                message_thread_id=msg_thread_id,
                                text='⚠️ Не смог поставить сообщение в очередь. Попробуй ещё раз.',
                                reply_to_message_id=message_id or None,
                                timeout=10,
                            )
                        except Exception:
                            pass

                # 2) Callback queries from inline buttons
                cb = upd.get('callback_query')
                if isinstance(cb, dict):
                    _log_cb(
                        {
                            'kind': 'callback_query_received',
                            'update_id': int(uid) if isinstance(uid, int) else 0,
                            'cb_id': str(cb.get('id') or '')[:64],
                            'has_message': isinstance(cb.get('message'), dict),
                            'has_inline_message_id': bool(cb.get('inline_message_id')),
                            'data': str(cb.get('data') or '')[:80],
                        }
                    )
                    data = cb.get('data')
                    cb_id = cb.get('id')
                    frm = cb.get('from') or {}
                    user_id = int(frm.get('id') or 0)
                    msg2 = cb.get('message') or {}
                    chat = (msg2.get('chat') or {}) if isinstance(msg2, dict) else {}
                    chat_id = int(chat.get('id') or 0)
                    chat_type_s = str(chat.get('type') or '').strip().lower()
                    message_id = int((msg2.get('message_id') or 0) if isinstance(msg2, dict) else 0)
                    try:
                        message_thread_id = int((msg2.get('message_thread_id') or 0) if isinstance(msg2, dict) else 0)
                    except Exception:
                        message_thread_id = 0
                    if (
                        isinstance(data, str)
                        and data
                        and isinstance(cb_id, str)
                        and cb_id
                        and chat_id != 0
                        and user_id != 0
                    ):
                        from . import keyboards

                        if not authorized(chat_id, user_id, chat_type_s):
                            _log_cb(
                                {
                                    'kind': 'callback_drop',
                                    'reason': 'not_authorized',
                                    'chat_id': int(chat_id),
                                    'user_id': int(user_id),
                                    'chat_type': str(chat_type_s),
                                    'data': data[:80],
                                }
                            )
                            try:
                                api.answer_callback_query(callback_query_id=cb_id, text='Not authorized')
                            except Exception:
                                pass
                            continue
                        is_danger_confirm = data.startswith(keyboards.CB_DANGER_ALLOW_PREFIX) or data.startswith(
                            keyboards.CB_DANGER_DENY_PREFIX
                        )
                        is_queue_ui = (
                            data.startswith(keyboards.CB_QUEUE_PAGE_PREFIX)
                            or data.startswith(keyboards.CB_QUEUE_EDIT_PREFIX)
                            or data.startswith(keyboards.CB_QUEUE_DONE_PREFIX)
                            or data.startswith(keyboards.CB_QUEUE_CLEAR_PREFIX)
                            or data.startswith(keyboards.CB_QUEUE_ITEM_PREFIX)
                            or data.startswith(keyboards.CB_QUEUE_ACT_PREFIX)
                        )
                        is_voice_route = data.startswith(keyboards.CB_VOICE_ROUTE_PREFIX)
                        is_control_plane = (
                            is_voice_route or is_queue_ui or data in keyboards.CONTROL_PLANE_CALLBACK_DATA
                        )
                        if state.is_restart_pending() and not is_danger_confirm and not is_control_plane:
                            _log_cb(
                                {
                                    'kind': 'callback_drop',
                                    'reason': 'restart_pending',
                                    'chat_id': int(chat_id),
                                    'user_id': int(user_id),
                                    'chat_type': str(chat_type_s),
                                    'data': data[:80],
                                }
                            )
                            try:
                                api.answer_callback_query(callback_query_id=cb_id, text='🔄 Перезапускаюсь')
                            except Exception:
                                pass
                            continue
                        # If the main worker is busy, acknowledge the button press with a normal message too.
                        # CallbackQuery "answer" is easy to miss and disappears quickly.
                        running_n = 0
                        pending_total = 0
                        scope_pending = 0
                        try:
                            running_n = int(scheduler.running_count())
                            snap_counts = dict(scheduler.snapshot(max_items=0))

                            def _i(v: object) -> int:
                                if isinstance(v, bool):
                                    return int(v)
                                if isinstance(v, int):
                                    return int(v)
                                if isinstance(v, float):
                                    return int(v)
                                if isinstance(v, str):
                                    try:
                                        return int(v.strip() or 0)
                                    except Exception:
                                        return 0
                                return 0

                            pending_total = (
                                _i(snap_counts.get('main_n'))
                                + _i(snap_counts.get('prio_n'))
                                + _i(snap_counts.get('paused_n'))
                            )
                            scope_pending = int(
                                scheduler.scope_queue_len(chat_id=chat_id, message_thread_id=message_thread_id)
                            )
                        except Exception:
                            running_n = 0
                            pending_total = 0
                            scope_pending = 0

                        busy = bool(
                            running_n >= int(cfg.tg_max_parallel_jobs) or scope_pending > 0 or pending_total > 0
                        )
                        # Stop Telegram spinner ASAP (even if main worker is busy).
                        try:
                            api.answer_callback_query(
                                callback_query_id=cb_id, text='🕓 В очереди' if busy else '✅ Принял'
                            )
                        except Exception as e:
                            _log_cb(
                                {
                                    'kind': 'callback_answer_error',
                                    'chat_id': int(chat_id),
                                    'user_id': int(user_id),
                                    'cb_id': str(cb_id)[:64],
                                    'data': data[:80],
                                    'error': repr(e)[:240],
                                }
                            )
                            pass

                        # Control plane: handle immediately (poll thread) so it works even while Codex is busy.
                        # IMPORTANT: keep /reset and any Codex-triggering callbacks in the queue.
                        if is_control_plane:
                            try:
                                chat_meta = _tg_chat_meta(chat, chat_type=chat_type_s)
                                user_meta = _tg_user_meta(frm) if isinstance(frm, dict) else None
                                _log_cb(
                                    {
                                        'kind': 'callback_bypass',
                                        'chat_id': int(chat_id),
                                        'user_id': int(user_id),
                                        'cb_id': str(cb_id)[:64],
                                        'data': data[:80],
                                        'busy': bool(busy),
                                    }
                                )
                                router.handle_callback(
                                    chat_id=chat_id,
                                    message_thread_id=message_thread_id,
                                    user_id=user_id,
                                    data=data,
                                    callback_query_id=cb_id,
                                    message_id=message_id,
                                    tg_chat=chat_meta,
                                    tg_user=user_meta,
                                )
                                state.metric_inc('queue.cb.bypassed')
                            except Exception as e:
                                _log_cb(
                                    {
                                        'kind': 'callback_bypass_error',
                                        'chat_id': int(chat_id),
                                        'user_id': int(user_id),
                                        'cb_id': str(cb_id)[:64],
                                        'data': data[:80],
                                        'error': repr(e)[:240],
                                    }
                                )
                            continue

                        ack_message_id = 0
                        if busy:
                            rt = message_id if message_id > 0 else None
                            try:
                                pos = int(scope_pending) + 1
                            except Exception:
                                pos = 0
                            queue_info = (
                                f'\nОчередь (этот топик): #{pos} | В работе: {running_n}/{int(cfg.tg_max_parallel_jobs)}'
                                if (cfg.tg_ack_include_queue and pos > 0)
                                else ''
                            )
                            try:
                                btn = keyboards.describe_callback_data(data) or data
                                resp = api.send_message(
                                    chat_id=chat_id,
                                    message_thread_id=(message_thread_id if int(message_thread_id or 0) > 0 else None),
                                    text=f'🕓 Принял кнопку: {btn}. Выполню после текущей задачи.{queue_info}',
                                    reply_to_message_id=rt,
                                    coalesce_key=(
                                        f'ackcb:{int(chat_id)}:{str(cb_id)[-16:]}'
                                        if isinstance(cb_id, str) and cb_id
                                        else None
                                    ),
                                    timeout=10,
                                )
                                ack_message_id = int(
                                    ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id')
                                    or 0
                                )
                            except Exception:
                                pass
                        try:
                            chat_meta = _tg_chat_meta(chat, chat_type=chat_type_s)
                            user_meta = _tg_user_meta(frm) if isinstance(frm, dict) else None
                            ev_out = Event(
                                kind='callback',
                                chat_id=chat_id,
                                chat_type=chat_type_s,
                                user_id=user_id,
                                text=data,
                                message_thread_id=message_thread_id,
                                chat_meta=chat_meta,
                                user_meta=user_meta,
                                message_id=message_id,
                                callback_query_id=cb_id,
                                received_ts=time.time(),
                                ack_message_id=int(ack_message_id or 0),
                            )
                            # Dangerous confirmations should bypass the regular queue.
                            scheduler.enqueue(ev_out, priority=bool(is_danger_confirm))
                            state.metric_inc('queue.cb.enqueued')
                            if is_danger_confirm:
                                state.metric_inc('queue.cb.enqueued_prio')
                            _log_cb(
                                {
                                    'kind': 'callback_enqueued',
                                    'queue': 'prio' if is_danger_confirm else 'main',
                                    'chat_id': int(chat_id),
                                    'user_id': int(user_id),
                                    'cb_id': str(cb_id)[:64],
                                    'data': data[:80],
                                }
                            )
                        except Exception:
                            pass
                    else:
                        _log_cb(
                            {
                                'kind': 'callback_drop',
                                'reason': 'missing_fields',
                                'chat_id': int(chat_id),
                                'user_id': int(user_id),
                                'chat_type': str(chat_type_s),
                                'message_id': int(message_id),
                                'cb_id': str(cb_id or '')[:64],
                                'data': str(data or '')[:80],
                            }
                        )

            if max_update_id is not None:
                state.set_tg_offset(int(max_update_id) + 1)

    def worker_loop() -> None:
        nonlocal spool_drain_remaining_by_path

        pause_processed_any_after = False
        last_outbox_flush_ts = 0.0
        last_idle_retry_ts = 0.0
        idle_retry_cooldown_s = 10.0
        last_waiting_ping_check_ts = 0.0
        waiting_ping_check_interval_s = 2.0
        waiting_ping_schedule_s = (5 * 60.0, 10 * 60.0, 15 * 60.0)

        def _maybe_flush_outbox(*, max_ops: int = 20, min_interval_s: float = 0.3) -> None:
            nonlocal last_outbox_flush_ts
            now_ts = time.time()
            if (now_ts - last_outbox_flush_ts) < float(min_interval_s):
                return
            last_outbox_flush_ts = now_ts
            try:
                api.flush_outbox(max_ops=int(max_ops))
            except Exception:
                pass

        def _cleanup_spool(ev: Event) -> None:
            if not ev.queued_from_disk or not ev.spool_file:
                return
            key = str(ev.spool_file or '')
            if not key:
                return
            with spool_drain_remaining_lock:
                rem = spool_drain_remaining_by_path.get(key)
                if rem is None:
                    return
                rem -= 1
                if rem <= 0:
                    try:
                        Path(key).unlink()
                    except Exception:
                        pass
                    spool_drain_remaining_by_path.pop(key, None)
                else:
                    spool_drain_remaining_by_path[key] = rem

        def _parse_scope_key(scope_key: str) -> tuple[int, int] | None:
            s = str(scope_key or '').strip()
            if not s:
                return None
            if ':' not in s:
                return None
            a, b = s.split(':', 1)
            try:
                chat_id = int(a.strip())
                message_thread_id = int(b.strip() or 0)
            except Exception:
                return None
            if chat_id == 0:
                return None
            return (int(chat_id), max(0, int(message_thread_id or 0)))

        def _maybe_ping_waiting_for_user(*, pause_active: bool, restart_pending: bool) -> None:
            nonlocal last_waiting_ping_check_ts
            now_ts = time.time()
            if (now_ts - last_waiting_ping_check_ts) < float(waiting_ping_check_interval_s):
                return
            last_waiting_ping_check_ts = now_ts

            try:
                waiting = state.waiting_for_user_snapshot()
            except Exception:
                waiting = {}

            if not waiting:
                return

            for scope_key, job in waiting.items():
                parsed = _parse_scope_key(str(scope_key or ''))
                if not parsed:
                    continue
                chat_id, message_thread_id = parsed

                try:
                    scope_pending = int(scheduler.scope_queue_len(chat_id=chat_id, message_thread_id=message_thread_id))
                except Exception:
                    scope_pending = 0
                if scope_pending > 0:
                    continue

                try:
                    asked_ts = float(job.get('asked_ts') or 0.0)
                except Exception:
                    asked_ts = 0.0
                if asked_ts <= 0:
                    continue

                try:
                    ping_count = int(job.get('ping_count') or 0)
                except Exception:
                    ping_count = 0
                ping_count = max(0, ping_count)
                question = str(job.get('question') or '').strip()
                default = str(job.get('default') or '').strip()
                mode = str(job.get('mode') or '').strip().lower()
                try:
                    origin_message_id = int(job.get('origin_message_id') or 0)
                except Exception:
                    origin_message_id = 0
                try:
                    origin_ack_message_id = int(job.get('origin_ack_message_id') or 0)
                except Exception:
                    origin_ack_message_id = 0
                try:
                    origin_user_id = int(job.get('origin_user_id') or 0)
                except Exception:
                    origin_user_id = 0

                reply_to = origin_message_id if origin_message_id > 0 else None

                def _auto_continue_by_default(
                    *,
                    chat_id: int = chat_id,
                    message_thread_id: int = message_thread_id,
                    mode: str = mode,
                    question: str = question,
                    default: str = default,
                    origin_user_id: int = origin_user_id,
                    origin_message_id: int = origin_message_id,
                    origin_ack_message_id: int = origin_ack_message_id,
                    now_ts: float = now_ts,
                ) -> None:
                    try:
                        state.set_waiting_for_user(chat_id=chat_id, message_thread_id=message_thread_id, job=None)
                        state.metric_inc('user_in_loop.timeout.cleared')
                    except Exception:
                        pass

                    prefix = ''
                    try:
                        if mode == 'write':
                            prefix = str(cfg.router_force_write_prefix or '!')
                        elif mode == 'read':
                            prefix = str(cfg.router_force_read_prefix or '')
                    except Exception:
                        prefix = '!' if mode == 'write' else ''

                    q_short = question
                    if len(q_short) > 300:
                        q_short = q_short[:299] + '…'
                    d_short = default
                    if len(d_short) > 300:
                        d_short = d_short[:299] + '…'

                    auto_lines = [
                        'Timeout blocking-вопроса (~15 минут, ответа нет).',
                        f'Вопрос: {q_short}' if q_short else 'Вопрос: (пусто)',
                        f'Продолжаю по дефолту: {d_short}' if d_short else 'Продолжаю по дефолту: (пусто)',
                        'Продолжай исходную задачу с учётом этого.',
                    ]
                    auto_payload = (prefix + '\n'.join(auto_lines)).strip()

                    try:
                        uid = int(origin_user_id or 0)
                    except Exception:
                        uid = 0
                    if uid <= 0 and int(chat_id) > 0:
                        uid = int(chat_id)

                    try:
                        scheduler.enqueue(
                            Event(
                                kind='text',
                                chat_id=int(chat_id),
                                chat_type='private',
                                user_id=int(uid),
                                text=auto_payload,
                                message_thread_id=int(message_thread_id or 0),
                                message_id=int(origin_message_id or 0),
                                received_ts=float(now_ts),
                                ack_message_id=int(origin_ack_message_id or 0),
                                synthetic=True,
                            ),
                            priority=True,
                        )
                        state.metric_inc('user_in_loop.timeout.enqueued')
                    except Exception:
                        state.metric_inc('user_in_loop.timeout.enqueue_fail')

                def _cancel_waiting_no_default(
                    *,
                    reason: str,
                    chat_id: int = chat_id,
                    message_thread_id: int = message_thread_id,
                    question: str = question,
                    reply_to: int | None = reply_to,
                ) -> None:
                    try:
                        state.set_waiting_for_user(chat_id=chat_id, message_thread_id=message_thread_id, job=None)
                        state.metric_inc('user_in_loop.timeout.cancelled')
                    except Exception:
                        pass
                    q_short = question
                    if len(q_short) > 300:
                        q_short = q_short[:299] + '…'
                    msg = (
                        '⛔️ Ответа нет и дефолта нет — отменяю задачу, чтобы она не висела.\n'
                        f'{reason}\n'
                        + (f'Вопрос: {q_short}\n' if q_short else '')
                        + 'Когда будет время — перезапусти: /new <текст>'
                    ).strip()
                    try:
                        api.send_message(
                            chat_id=chat_id,
                            message_thread_id=(message_thread_id if message_thread_id > 0 else None),
                            text=msg,
                            reply_to_message_id=reply_to,
                            timeout=10,
                        )
                    except Exception:
                        pass

                if ping_count >= len(waiting_ping_schedule_s):
                    if pause_active or restart_pending:
                        continue
                    if default and mode in {'read', 'write'}:
                        _auto_continue_by_default()
                    elif not default:
                        _cancel_waiting_no_default(reason='(таймаут blocking-вопроса)')
                    continue

                due_ts = float(asked_ts) + float(waiting_ping_schedule_s[ping_count])
                if now_ts < due_ts:
                    continue

                if ping_count == 0:
                    ping_text = '👀 Похоже ты отошёл. Нужен ответ на вопрос выше. Можно одной строкой.'
                elif ping_count == 1:
                    ping_text = '⏳ Всё ещё жду ответ на вопрос выше. Если быстрее — можно просто A/B.'
                else:
                    if default and mode in {'read', 'write'}:
                        ping_text = f'⏰ Последний пинг. Если ответа не будет — продолжаю по дефолту: {default}'
                    elif (not default) and (not pause_active) and (not restart_pending):
                        ping_text = '⛔️ Последний пинг. Ответа нет и дефолта нет — отменяю задачу. Когда будет время — /new <текст>'
                    else:
                        ping_text = '⏰ Последний пинг. Без ответа дальше не двигаюсь; напиши ответ, и продолжу.'

                try:
                    api.send_message(
                        chat_id=chat_id,
                        message_thread_id=(message_thread_id if message_thread_id > 0 else None),
                        text=ping_text,
                        reply_to_message_id=reply_to,
                        timeout=10,
                    )
                    state.metric_inc('user_in_loop.ping.sent')
                except Exception:
                    state.metric_inc('user_in_loop.ping.send_fail')

                try:
                    new_count = int(
                        state.bump_waiting_for_user_ping(
                            chat_id=chat_id, message_thread_id=message_thread_id, now_ts=float(now_ts)
                        )
                        or 0
                    )
                except Exception:
                    new_count = ping_count + 1

                # After the last ping (~15 minutes): auto-continue with default (only for read/write).
                if new_count >= len(waiting_ping_schedule_s) and (not pause_active) and (not restart_pending):
                    if default and mode in {'read', 'write'}:
                        _auto_continue_by_default()
                    elif not default:
                        try:
                            state.set_waiting_for_user(chat_id=chat_id, message_thread_id=message_thread_id, job=None)
                            state.metric_inc('user_in_loop.timeout.cancelled')
                        except Exception:
                            pass

        followups_ack_path = Path(
            os.getenv('TG_MCP_FOLLOWUPS_ACK_PATH', str(cfg.repo_root / '.mcp' / 'telegram-followups-ack.json'))
        ).expanduser()
        followups_ack_lock = threading.Lock()
        followups_ack_cache: dict[str, object] = {'mtime': 0.0, 'acked_by_scope': {}}

        def _followups_acked_upto(*, chat_id: int, message_thread_id: int) -> int:
            try:
                st = followups_ack_path.stat()
                mtime = float(st.st_mtime)
            except Exception:
                return 0
            with followups_ack_lock:
                cached_mtime_raw = followups_ack_cache.get('mtime')
                if isinstance(cached_mtime_raw, (int, float)):
                    cached_mtime = float(cached_mtime_raw)
                elif isinstance(cached_mtime_raw, str):
                    try:
                        cached_mtime = float(cached_mtime_raw.strip() or 0.0)
                    except Exception:
                        cached_mtime = 0.0
                else:
                    cached_mtime = 0.0
                if mtime != cached_mtime:
                    try:
                        raw = followups_ack_path.read_text(encoding='utf-8', errors='replace')
                        obj = json.loads(raw or '{}')
                    except Exception:
                        obj = {}
                    acked_raw = obj.get('acked_by_scope') if isinstance(obj, dict) else {}
                    cleaned: dict[str, int] = {}
                    if isinstance(acked_raw, dict):
                        for k, v in acked_raw.items():
                            if not isinstance(k, str) or not k.strip():
                                continue
                            try:
                                cleaned[k.strip()] = int(v)
                            except Exception:
                                continue
                    followups_ack_cache['acked_by_scope'] = cleaned
                    followups_ack_cache['mtime'] = float(mtime)
                acked = followups_ack_cache.get('acked_by_scope')
                if not isinstance(acked, dict):
                    return 0
                sk = f'{int(chat_id)}:{int(message_thread_id or 0)}'
                try:
                    return int(acked.get(sk) or 0)
                except Exception:
                    return 0

        def _run_one(item: SchedulableEvent) -> None:
            try:
                if isinstance(item, Event):
                    if item.kind == 'text':
                        if not authorized(item.chat_id, item.user_id, item.chat_type):
                            return
                        try:
                            mid = int(item.message_id or 0)
                        except Exception:
                            mid = 0
                        if (not item.synthetic) and mid > 0 and not _normalize_cmd_token(item.text).startswith('/'):
                            try:
                                acked_upto = _followups_acked_upto(
                                    chat_id=item.chat_id, message_thread_id=item.message_thread_id
                                )
                            except Exception:
                                acked_upto = 0
                            if acked_upto > 0 and mid <= acked_upto:
                                state.metric_inc('followups.deduped')
                                return
                        try:
                            router.handle_text(
                                chat_id=item.chat_id,
                                message_thread_id=item.message_thread_id,
                                user_id=item.user_id,
                                text=item.text,
                                attachments=list(item.attachments) if item.attachments else None,
                                reply_to=item.reply_to,
                                message_id=item.message_id,
                                received_ts=item.received_ts,
                                ack_message_id=item.ack_message_id,
                                tg_chat=(item.chat_meta if isinstance(item.chat_meta, dict) else None),
                                tg_user=(item.user_meta if isinstance(item.user_meta, dict) else None),
                            )
                        except Exception as e:
                            try:
                                api.send_message(
                                    chat_id=item.chat_id,
                                    message_thread_id=(
                                        item.message_thread_id if int(item.message_thread_id or 0) > 0 else None
                                    ),
                                    text=f'[bot error]\n{e!r}',
                                )
                            except Exception:
                                pass
                        return

                    if item.kind == 'callback':
                        if not authorized(item.chat_id, item.user_id, item.chat_type):
                            try:
                                api.answer_callback_query(
                                    callback_query_id=item.callback_query_id, text='Not authorized'
                                )
                            except Exception:
                                pass
                            return
                        try:
                            _log_cb(
                                {
                                    'kind': 'callback_dequeued',
                                    'chat_id': int(item.chat_id),
                                    'user_id': int(item.user_id),
                                    'cb_id': str(item.callback_query_id)[:64],
                                    'data': str(item.text or '')[:80],
                                    'restart_pending': bool(state.is_restart_pending()),
                                }
                            )
                            router.handle_callback(
                                chat_id=item.chat_id,
                                message_thread_id=item.message_thread_id,
                                user_id=item.user_id,
                                data=item.text,
                                message_id=item.message_id,
                                ack_message_id=item.ack_message_id,
                                callback_query_id=item.callback_query_id,
                                tg_chat=(item.chat_meta if isinstance(item.chat_meta, dict) else None),
                                tg_user=(item.user_meta if isinstance(item.user_meta, dict) else None),
                            )
                        except Exception as e:
                            _log_cb(
                                {
                                    'kind': 'callback_handler_error',
                                    'chat_id': int(item.chat_id),
                                    'user_id': int(item.user_id),
                                    'cb_id': str(item.callback_query_id)[:64],
                                    'data': str(item.text or '')[:80],
                                    'error': repr(e)[:240],
                                }
                            )
                            try:
                                api.answer_callback_query(callback_query_id=item.callback_query_id)
                            except Exception:
                                pass
                            try:
                                api.send_message(
                                    chat_id=item.chat_id,
                                    message_thread_id=(
                                        item.message_thread_id if int(item.message_thread_id or 0) > 0 else None
                                    ),
                                    text=f'[bot error]\n{e!r}',
                                )
                            except Exception:
                                pass
                        return
            finally:
                try:
                    if isinstance(item, Event):
                        _cleanup_spool(item)
                except Exception:
                    pass
                try:
                    scheduler.mark_done(chat_id=int(item.chat_id), message_thread_id=int(item.message_thread_id or 0))
                except Exception:
                    pass

        while not stop.is_set():
            if _runtime_queue_edit_active():
                _maybe_flush_outbox(max_ops=20, min_interval_s=0.1)
                scheduler.wait(timeout_seconds=0.2)
                continue

            restart_pending = bool(state.is_restart_pending())
            pause_active, pause_ts = _pause_is_active()

            # If restart is pending, don't keep the pause barrier around: it could prevent draining.
            if restart_pending:
                _pause_clear()
                pause_processed_any_after = False
                pause_active = False

            started_any = False
            while not stop.is_set():
                item = scheduler.try_dispatch_next(pause_active=pause_active, pause_ts=pause_ts)
                if item is None:
                    break
                started_any = True

                if pause_active and str(getattr(item, 'kind', '') or '') != 'callback':
                    pause_processed_any_after = True

                if isinstance(item, Event):
                    try:
                        ev_ts = float(item.received_ts or 0.0)
                    except Exception:
                        ev_ts = 0.0
                    if ev_ts > 0:
                        state.metric_observe_ms('queue.wait', (time.time() - ev_ts) * 1000.0)
                    if item.kind == 'text':
                        state.metric_inc('queue.text.dequeued')
                    elif item.kind == 'callback':
                        state.metric_inc('queue.cb.dequeued')

                threading.Thread(target=_run_one, args=(item,), name='tg-job', daemon=True).start()

            _maybe_flush_outbox(max_ops=20, min_interval_s=0.2)

            # Auto-release the pause barrier once we processed all post-/pause events currently queued.
            if pause_active and pause_processed_any_after:
                try:
                    snap_counts = dict(scheduler.snapshot(max_items=0))

                    def _i(v: object) -> int:
                        if isinstance(v, bool):
                            return int(v)
                        if isinstance(v, int):
                            return int(v)
                        if isinstance(v, float):
                            return int(v)
                        if isinstance(v, str):
                            try:
                                return int(v.strip() or 0)
                            except Exception:
                                return 0
                        return 0

                    main_n = _i(snap_counts.get('main_n'))
                    prio_n = _i(snap_counts.get('prio_n'))
                    if main_n == 0 and prio_n == 0 and scheduler.running_count() == 0:
                        _pause_clear()
                        pause_processed_any_after = False
                except Exception:
                    pass

            # Deferred Codex jobs: retry only when idle (avoid concurrent resume for the same scope).
            if not pause_active and not restart_pending and scheduler.running_count() == 0:
                now_ts = time.time()
                if (now_ts - last_idle_retry_ts) >= idle_retry_cooldown_s:
                    try:
                        has_jobs = bool(state.pending_codex_jobs_snapshot())
                    except Exception:
                        has_jobs = False
                    if has_jobs:
                        last_idle_retry_ts = now_ts
                        try:
                            router.retry_pending_codex_jobs(max_jobs=1)
                        except Exception:
                            pass

            if not restart_pending:
                try:
                    _maybe_ping_waiting_for_user(pause_active=pause_active, restart_pending=restart_pending)
                except Exception:
                    pass

            # Graceful-ish restart: once the scheduler queue is drained AND no jobs are running, stop the process
            # (systemd can restart it). We intentionally wait for other topics too to avoid interrupting work.
            if state.is_restart_pending():
                queue_drained = _restart_queue_drained(scheduler=scheduler)
                running_now = 0
                try:
                    running_now = int(scheduler.running_count())
                except Exception:
                    running_now = 0

                if queue_drained and running_now == 0:
                    try:
                        restart_chat_id, restart_thread_id, restart_reply_id, restart_ack_id = state.restart_target()
                        if restart_chat_id:
                            edited = False
                            restart_ack_mid = _restart_ack_message_id_from_state(
                                state, chat_id=int(restart_chat_id), restart_message_id=int(restart_reply_id)
                            )
                            if int(restart_ack_mid) <= 0 and int(restart_ack_id) > 0:
                                restart_ack_mid = int(restart_ack_id)
                            if int(restart_ack_mid) > 0:
                                try:
                                    api.edit_message_text(
                                        chat_id=int(restart_chat_id),
                                        message_id=int(restart_ack_mid),
                                        text='🔄 Очередь пуста. Перезапускаюсь…',
                                    )
                                    edited = True
                                except Exception:
                                    edited = False
                            restart_status_message_id = int(restart_ack_mid) if edited else 0
                            if not edited:
                                resp = api.send_message(
                                    chat_id=int(restart_chat_id),
                                    message_thread_id=(int(restart_thread_id) if int(restart_thread_id) > 0 else None),
                                    text='🔄 Очередь пуста. Перезапускаюсь…',
                                    reply_to_message_id=int(restart_reply_id) or None,
                                    timeout=10,
                                )
                                try:
                                    restart_status_message_id = int(
                                        ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id')
                                        or 0
                                    )
                                except Exception:
                                    restart_status_message_id = 0
                            try:
                                state.mark_restart_shutting_down(status_message_id=int(restart_status_message_id or 0))
                            except Exception:
                                pass
                    except Exception:
                        pass
                    stop.set()
                    break

            scheduler.wait(timeout_seconds=0.3 if not started_any else 0.05)

    def watch_loop() -> None:
        # Small startup delay so poller can learn chat_id from first message.
        time.sleep(3.0)
        while not stop.is_set():
            try:
                watcher.tick(api=api, state=state)
            except Exception:
                pass
            for _ in range(int(cfg.watch_interval_seconds * 10)):
                if stop.is_set():
                    break
                time.sleep(0.1)

    def mm_watch_loop() -> None:
        if mm_watcher is None:
            return
        # Small startup delay so poller can learn chat_id from first message.
        time.sleep(3.0)
        while not stop.is_set():
            try:
                mm_watcher.tick(api=api, state=state)
            except Exception:
                pass
            time.sleep(1.0)

    t_poll = threading.Thread(target=poll_loop, name='tg-poll', daemon=True)
    t_work = threading.Thread(target=worker_loop, name='tg-worker', daemon=True)
    t_watch = threading.Thread(target=watch_loop, name='tg-watch', daemon=True)
    t_mm = threading.Thread(target=mm_watch_loop, name='tg-mm-watch', daemon=True) if mm_watcher is not None else None

    t_poll.start()
    t_work.start()
    t_watch.start()
    if t_mm is not None:
        t_mm.start()

    # Print a minimal startup line for logs.
    print(f'tg_bot running as @{username} (repo_root={cfg.repo_root})')

    try:
        while not stop.is_set():
            time.sleep(1.0)
            # Safety: if a critical worker dies (e.g. poller exception), restart the process via systemd.
            if not t_poll.is_alive():
                print('tg_bot: poll thread died; requesting restart')
                stop.set()
                break
            if not t_work.is_alive():
                print('tg_bot: worker thread died; requesting restart')
                stop.set()
                break
    except KeyboardInterrupt:
        stop.set()
    finally:
        try:
            if lock_handle is not None:
                lock_handle.close()  # type: ignore[attr-defined]
        except Exception:
            pass
    return 0
