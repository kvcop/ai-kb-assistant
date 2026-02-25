from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any

from tg_bot.state import BotState


def _to_int(value: Any, default: int = 0) -> int:
    """Best-effort cast helper for packet item fields."""
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    """Best-effort cast helper for timestamps and metrics."""
    try:
        return float(value)
    except Exception:
        return default


def _normalize_author(item: Mapping[str, Any]) -> str:
    """Extract stable author-like string from a collect entry."""
    author = item.get('author')
    if not isinstance(author, str) or not author.strip():
        author = item.get('author_name')
    if not isinstance(author, str) or not author.strip():
        author = item.get('user_name')
    if not isinstance(author, str) or not author.strip():
        author = item.get('user_id')
    if author is None:
        return ''
    return str(author).strip()


def _attachments_summary(attachments: object) -> str:
    """Build short textual summary for item attachments."""
    if not isinstance(attachments, list):
        return ''
    names: list[str] = []
    for attachment in attachments:
        if isinstance(attachment, Mapping):
            raw_name = attachment.get('name') or attachment.get('filename') or attachment.get('file_name')
            if isinstance(raw_name, str):
                name = raw_name.strip()
            else:
                continue
        else:
            name = str(attachment).strip()
        if name:
            names.append(name)
    if not names:
        return ''
    if len(names) <= 3:
        return ', '.join(names)
    return f"{', '.join(names[:3])} (+{len(names) - 3} more)"


def _scope_metadata(chat_id: int, message_thread_id: int = 0) -> dict[str, int]:
    """Build packet scope metadata."""
    return {
        'chat_id': int(chat_id),
        'message_thread_id': int(message_thread_id or 0),
    }


def _collect_packet_id(packet: dict[str, Any]) -> str:
    """Build deterministic packet id from instruction/scope/items."""
    normalized = {
        'instruction': str(packet.get('instruction') or '').strip(),
        'scope_metadata': packet.get('scope_metadata') or {},
        'items': packet.get('items') or [],
    }
    raw = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def _packet_id(packet: Mapping[str, Any]) -> str:
    """Return explicit packet_id if present, otherwise compute one."""
    explicit_id = str(packet.get('packet_id') or '').strip()
    if explicit_id:
        return explicit_id
    return _collect_packet_id(dict(packet))


def _scope_from_packet(packet: Mapping[str, Any]) -> tuple[int, int]:
    """Extract scope identifiers from packet metadata."""
    scope = packet.get('scope_metadata')
    if not isinstance(scope, Mapping):
        return 0, 0
    return _to_int(scope.get('chat_id')), _to_int(scope.get('message_thread_id'))


def _normalize_item_metadata(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return required item metadata in a compact normalized form."""
    return {
        'message_id': _to_int(raw.get('message_id')),
        'author': _normalize_author(raw),
        'attachments_summary': _attachments_summary(raw.get('attachments')),
    }


def _metadata_chars(packet: Mapping[str, Any]) -> int:
    """Count chars of packet metadata-only part for budget accounting."""
    scope = packet.get('scope_metadata')
    if not isinstance(scope, Mapping):
        scope = {}
    items = packet.get('items')
    if not isinstance(items, list):
        items = []

    metadata_items: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, Mapping):
            metadata = item.get('item_metadata')
            if not isinstance(metadata, Mapping):
                metadata = _normalize_item_metadata(dict(item))
            else:
                metadata = dict(metadata)
        else:
            metadata = {}
        metadata_items.append(
            {
                'message_id': _to_int(metadata.get('message_id')),
                'author': str(metadata.get('author') or '').strip(),
                'attachments_summary': str(metadata.get('attachments_summary') or '').strip(),
            }
        )

    metadata_payload = {
        'instruction': str(packet.get('instruction') or '').strip(),
        'scope_metadata': _scope_metadata(int(scope.get('chat_id') or 0), int(scope.get('message_thread_id') or 0)),
        'items_metadata': metadata_items,
    }
    return len(json.dumps(metadata_payload, ensure_ascii=False, sort_keys=True))


def build_collect_packet(
    *,
    instruction: str,
    items: Sequence[Mapping[str, Any]],
    chat_id: int,
    message_thread_id: int = 0,
    created_ts: float | None = None,
) -> dict[str, Any]:
    """Build normalized collect packet payload.

    Packet schema:
      instruction: str
      items: [{text, item_metadata: {message_id, author, attachments_summary}}]
      created_ts: float
      scope_metadata: {chat_id, message_thread_id}
      packet_id: str
    """
    normalized_items: list[dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, Mapping):
            continue
        metadata = _normalize_item_metadata(raw)
        item_text = str(raw.get('text') or raw.get('message') or '').strip()
        normalized_items.append({'text': item_text, 'item_metadata': metadata})

    scope_md = _scope_metadata(int(chat_id), int(message_thread_id or 0))
    packet: dict[str, Any] = {
        'instruction': str(instruction or '').strip(),
        'items': normalized_items,
        'created_ts': _to_float(created_ts, default=time.time()),
        'scope_metadata': scope_md,
    }
    packet['packet_id'] = _collect_packet_id(packet)
    return packet


def collect_preflight_budget_report(
    packet: Mapping[str, Any],
    *,
    max_payload_chars: int,
    max_items: int,
    max_metadata_chars: int,
) -> dict[str, Any]:
    """Return payload budget preflight report for one collect packet.

    The report always contains `ok`, `over_limit`, `reasons`, and `metrics`.
    """
    packet_dict = dict(packet or {})
    items = packet_dict.get('items')
    if not isinstance(items, list):
        items = []

    metrics: dict[str, int] = {
        'payload_chars': len(json.dumps(packet_dict, ensure_ascii=False, sort_keys=True)),
        'items_count': len(items),
        'metadata_chars': _metadata_chars(packet_dict),
    }

    reasons: list[str] = []

    max_payload = int(max_payload_chars)
    max_items_limit = int(max_items)
    max_metadata = int(max_metadata_chars)

    if max_payload > 0 and metrics['payload_chars'] > max_payload:
        reasons.append(f'payload chars limit exceeded: {metrics["payload_chars"]} > {max_payload}')
    if max_items_limit > 0 and metrics['items_count'] > max_items_limit:
        reasons.append(f'items limit exceeded: {metrics["items_count"]} > {max_items_limit}')
    if max_metadata > 0 and metrics['metadata_chars'] > max_metadata:
        reasons.append(f'metadata chars limit exceeded: {metrics["metadata_chars"]} > {max_metadata}')

    return {
        'ok': len(reasons) == 0,
        'over_limit': len(reasons) > 0,
        'reasons': reasons,
        'metrics': metrics,
    }


def collect_packet_send_decision(
    packet: Mapping[str, Any],
    state: BotState,
    *,
    max_payload_chars: int,
    max_items: int,
    max_metadata_chars: int,
    force: bool = False,
) -> dict[str, Any]:
    """Resolve packet send decision with pending/retry/force semantics.

    Returns:
      - ok: bool (True if sending may proceed)
      - forced: bool (True if an over-limit packet was explicitly/previously forced)
      - decision: one of {'send', 'pending', 'forced'}
    """
    packet_id = _packet_id(packet)
    report = collect_preflight_budget_report(
        packet,
        max_payload_chars=max_payload_chars,
        max_items=max_items,
        max_metadata_chars=max_metadata_chars,
    )

    chat_id, message_thread_id = _scope_from_packet(packet)
    existing = state.collect_packet_decision(
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        packet_id=packet_id,
    )
    existing_forced = isinstance(existing, dict) and existing.get('status') == 'forced'

    if report['over_limit']:
        if force or existing_forced:
            state.set_collect_packet_decision(
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                packet_id=packet_id,
                status='forced',
                reasons=report['reasons'],
                report=report,
            )
            return {
                'ok': True,
                'forced': True,
                'decision': 'forced',
                'over_limit': True,
                'packet_id': packet_id,
                'report': report,
                'reasons': report['reasons'],
            }

        state.set_collect_packet_decision(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            packet_id=packet_id,
            status='pending',
            reasons=report['reasons'],
            report=report,
        )
        return {
            'ok': False,
            'forced': False,
            'decision': 'pending',
            'over_limit': True,
            'packet_id': packet_id,
            'report': report,
            'reasons': report['reasons'],
        }

    state.set_collect_packet_decision(
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        packet_id=packet_id,
        status=None,
    )
    return {
        'ok': True,
        'forced': False,
        'decision': 'send',
        'over_limit': False,
        'packet_id': packet_id,
        'report': report,
        'reasons': report['reasons'],
    }
