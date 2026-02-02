from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _preview_text(v: object, max_chars: int) -> str:
    if not isinstance(v, str):
        return ''
    s = v.replace('\n', ' ').strip()
    if max_chars > 0 and len(s) > max_chars:
        return s[: max(0, max_chars - 1)] + '…'
    return s


def _is_valid_record(obj: dict[str, Any]) -> bool:
    kind = str(obj.get('kind') or '').strip()
    if kind not in {'text', 'callback'}:
        return False
    text = obj.get('text')
    if not isinstance(text, str) or not text.strip():
        return False
    try:
        chat_id = int(obj.get('chat_id') or 0)
        user_id = int(obj.get('user_id') or 0)
    except Exception:
        return False
    return chat_id != 0 and user_id != 0


def _record_summary(obj: dict[str, Any], *, max_chars: int = 120) -> str:
    kind = str(obj.get('kind') or '?').strip() or '?'
    try:
        chat_id = int(obj.get('chat_id') or 0)
    except Exception:
        chat_id = 0
    try:
        thread_id = int(obj.get('message_thread_id') or 0)
    except Exception:
        thread_id = 0
    try:
        message_id = int(obj.get('message_id') or 0)
    except Exception:
        message_id = 0
    try:
        ack_id = int(obj.get('ack_message_id') or 0)
    except Exception:
        ack_id = 0
    tid_s = f' tid={thread_id}' if thread_id > 0 else ''
    ack_s = f' ack={ack_id}' if ack_id > 0 else ''
    preview = _preview_text(obj.get('text'), max_chars)
    if kind == 'callback':
        try:
            from . import keyboards

            label = keyboards.describe_callback_data(preview)
            if label:
                preview = label
        except Exception:
            pass
    prefix = 'spool '
    if preview:
        return f'{prefix}{kind} chat={chat_id}{tid_s} mid={message_id}{ack_s}: {preview}'
    return f'{prefix}{kind} chat={chat_id}{tid_s} mid={message_id}{ack_s}'.strip()


def preview_spool(*, path: Path, max_items: int, max_scan_lines: int = 2000) -> dict[str, object]:
    """Return a compact spool snapshot: {n, head, truncated}.

    - `n` is best-effort count of valid queued records.
    - `head` is the first `max_items` summaries.
    - `truncated` means we stopped scanning at `max_scan_lines`.
    """
    try:
        lim = int(max_items)
    except Exception:
        lim = 5
    lim = max(0, lim)
    try:
        max_scan = int(max_scan_lines)
    except Exception:
        max_scan = 2000
    max_scan = max(1, max_scan)

    if not path.exists():
        return {'n': 0, 'head': [], 'truncated': False}

    n = 0
    head: list[str] = []
    truncated = False
    try:
        with path.open('r', encoding='utf-8', errors='replace') as f:
            for line in f:
                s = (line or '').strip()
                if not s:
                    continue
                if n >= max_scan:
                    truncated = True
                    break
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                if not isinstance(obj, dict) or not _is_valid_record(obj):
                    continue
                n += 1
                if lim > 0 and len(head) < lim:
                    head.append(_record_summary(obj))
    except Exception:
        return {'n': 0, 'head': [], 'truncated': False}

    return {'n': int(n), 'head': head, 'truncated': bool(truncated)}


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


def delete_spool_item(*, path: Path, index: int) -> dict[str, object]:
    """Delete one item from a JSONL spool file by its *valid record* index.

    Returns a compact dict: {ok, changed, n, error?}
    """
    try:
        i = int(index)
    except Exception:
        return {'ok': False, 'error': 'bad_index'}

    if not path.exists():
        return {'ok': False, 'error': 'missing'}

    try:
        lines = path.read_text(encoding='utf-8', errors='replace').splitlines()
    except Exception:
        return {'ok': False, 'error': 'read_failed'}

    valid_line_indexes: list[int] = []
    for li, line in enumerate(lines):
        s = (line or '').strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if not isinstance(obj, dict) or not _is_valid_record(obj):
            continue
        valid_line_indexes.append(int(li))

    n = len(valid_line_indexes)
    if i < 0 or i >= n:
        return {'ok': False, 'error': 'out_of_range', 'n': int(n)}

    del lines[valid_line_indexes[i]]

    if not any(str(line or '').strip() for line in lines):
        try:
            path.unlink()
        except Exception:
            try:
                _atomic_write_lines(path, [])
            except Exception:
                return {'ok': False, 'error': 'write_failed'}
        return {'ok': True, 'changed': True, 'n': 0}

    try:
        _atomic_write_lines(path, lines)
    except Exception:
        return {'ok': False, 'error': 'write_failed'}

    return {'ok': True, 'changed': True, 'n': int(n - 1)}
