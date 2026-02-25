from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any


def _now_ts() -> float:
    return time.time()


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(content, encoding='utf-8')
    os.replace(tmp, path)


def _clamp_text(s: str, max_chars: int) -> str:
    s = (s or '').strip()
    if max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return s[: max(0, max_chars - 1)] + '…'


def _scope_key(*, chat_id: int, message_thread_id: int = 0) -> str:
    return f'{int(chat_id)}:{int(message_thread_id or 0)}'


def _normalize_scope_key(raw: object) -> str | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    s = raw.strip()
    if ':' in s:
        a, b = s.split(':', 1)
        try:
            cid = int(a.strip())
            tid = int(b.strip() or 0)
        except Exception:
            return None
        if cid == 0:
            return None
        return _scope_key(chat_id=cid, message_thread_id=tid)
    try:
        cid = int(s)
    except Exception:
        return None
    if cid == 0:
        return None
    return _scope_key(chat_id=cid, message_thread_id=0)


def _normalize_codex_mode(raw: object) -> str:
    v = str(raw or '').strip().casefold()
    return v if v in {'read', 'write'} else ''


def _normalize_codex_reasoning(raw: object) -> str:
    v = str(raw or '').strip().casefold()
    return v if v in {'low', 'medium', 'high', 'xhigh'} else ''


@dataclass
class BotState:
    """Persistent bot state (JSON file).

    Goals:
    - keep Telegram polling offset
    - track last user activity and watcher ping stages
    - support snooze (mute/lunch)
    - support sleep_until per scope (chat_id:thread_id)
    - support gentle mode ("щадящий режим")
    - keep a small ring-buffer of recent bot events for Codex context injection
    """

    path: Path
    lock: Lock = field(default_factory=Lock)

    tg_offset: int = 0

    last_chat_id: int = 0
    last_user_id: int = 0
    watch_chat_id: int = 0  # last known private chat_id for watcher/reminders
    reminders_chat_id: int = 0
    reminders_message_thread_id: int = 0  # Telegram forum topic id (0 = general)

    last_user_msg_ts: float = 0.0
    last_user_msg_ts_by_chat: dict[str, float] = field(default_factory=dict)
    snooze_until_ts: float = 0.0
    snooze_kind: str = ''
    sleep_until_by_scope: dict[str, float] = field(default_factory=dict)

    # Gentle mode ("щадящий режим")
    gentle_until_ts: float = 0.0
    gentle_reason: str = ''
    mute_events_ts: list[float] = field(default_factory=list)

    # Watch escalation
    last_ping_ts: float = 0.0
    last_ping_stage: int = 0
    touch_ts_at_ping: float = 0.0  # last_touch_ts when ping was sent

    # Reminders de-duplication
    reminders_sent: dict[str, list[str]] = field(default_factory=dict)  # date -> [ids]
    reminders_pending: dict[str, list[str]] = field(
        default_factory=dict
    )  # date -> [ids] (queued in outbox, not confirmed sent)

    # Mattermost de-duplication (unread forwarding)
    # channel_id -> last create_at (ms) forwarded
    mm_sent_up_to_ts_by_channel: dict[str, int] = field(default_factory=dict)
    # channel_id -> last create_at (ms) queued in outbox (not confirmed sent)
    mm_pending_up_to_ts_by_channel: dict[str, int] = field(default_factory=dict)
    # Login+password + 2FA support (one-time code, best-effort).
    mm_mfa_token: str = ''
    mm_mfa_token_set_ts: float = 0.0
    mm_mfa_prompt_ts: float = 0.0
    mm_mfa_required_ts: float = 0.0
    mm_session_token: str = ''
    mm_session_token_set_ts: float = 0.0

    # Main Codex session bookkeeping (not router classifier)
    last_codex_ts: float = 0.0
    last_codex_automation: bool = False
    last_codex_profile: str = ''  # "chat" | "auto" | "danger" (best-effort)
    last_codex_mode: str = ''  # "read" | "write" (best-effort)
    last_codex_model: str = ''  # Codex model override (best-effort)
    last_codex_reasoning: str = ''  # "low" | "medium" | "high" | "xhigh" (best-effort)

    # Per-chat Codex bookkeeping (prevents cross-chat leakage in prompt injection / follow-ups)
    last_codex_ts_by_chat: dict[str, float] = field(default_factory=dict)  # chat_id -> ts
    last_codex_automation_by_chat: dict[str, bool] = field(default_factory=dict)  # chat_id -> bool
    last_codex_profile_by_chat: dict[str, str] = field(default_factory=dict)  # chat_id -> profile_name
    last_codex_mode_by_chat: dict[str, str] = field(default_factory=dict)  # chat_id -> mode
    last_codex_model_by_chat: dict[str, str] = field(default_factory=dict)  # chat_id -> model
    last_codex_reasoning_by_chat: dict[str, str] = field(default_factory=dict)  # chat_id -> reasoning

    # Per-scope Codex bookkeeping (Telegram topics/threads: chat_id + message_thread_id).
    # scope_key ("<chat_id>:<thread_id>") -> value
    last_codex_ts_by_scope: dict[str, float] = field(default_factory=dict)
    last_codex_automation_by_scope: dict[str, bool] = field(default_factory=dict)
    last_codex_profile_by_scope: dict[str, str] = field(default_factory=dict)
    last_codex_mode_by_scope: dict[str, str] = field(default_factory=dict)  # scope_key -> mode
    last_codex_model_by_scope: dict[str, str] = field(default_factory=dict)  # scope_key -> model
    last_codex_reasoning_by_scope: dict[str, str] = field(default_factory=dict)  # scope_key -> reasoning

    # Graceful restart coordination.
    # We persist the flag so poll/worker threads can coordinate, but clear it on startup.
    restart_pending: bool = False
    restart_requested_ts: float = 0.0
    restart_shutting_down_ts: float = 0.0
    restart_requested_chat_id: int = 0
    restart_requested_message_thread_id: int = 0
    restart_requested_user_id: int = 0
    restart_requested_message_id: int = 0
    restart_requested_ack_message_id: int = 0

    # Pending attachments (when user sends files without a caption).
    # chat_id -> [{path, name, kind, size_bytes, ts}]
    pending_attachments_by_chat: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    pending_reply_to_by_chat: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Pending attachments (Telegram topics/threads: chat_id + message_thread_id).
    # scope_key -> [{path, name, kind, size_bytes, ts}]
    pending_attachments_by_scope: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    pending_reply_to_by_scope: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Telegram delivery outbox (when send/edit fails, e.g. VPN/DNS issues).
    # Items: {id, op, chat_id, params, created_ts, attempts, next_attempt_ts, last_error, coalesce_key?}
    tg_outbox: list[dict[str, Any]] = field(default_factory=list)
    tg_offline_since_by_chat: dict[str, float] = field(default_factory=dict)  # chat_id -> ts
    tg_offline_notice_sent_ts_by_chat: dict[str, float] = field(
        default_factory=dict
    )  # chat_id -> ts (sent once per offline epoch)

    # Telegram message_id bindings for deferred sends (coalesce_key -> message_id).
    # chat_id -> {coalesce_key -> message_id}
    tg_message_id_by_coalesce_key_by_chat: dict[str, dict[str, int]] = field(default_factory=dict)

    # Attachments index for reply resolution.
    # chat_id -> [{message_id, attachments:[{path,name,kind,size_bytes}], ts}]
    attachments_index_by_chat: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    # Deferred/in-flight Codex jobs (crash recovery + e.g. when network to chatgpt.com is down).
    # chat_id -> {payload, attachments, reply_to, sent_ts, automation, profile_name, attempts, next_attempt_ts, defer_reason, ...}
    pending_codex_jobs_by_chat: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Deferred/in-flight Codex jobs (Telegram topics/threads: chat_id + message_thread_id).
    # scope_key -> {payload, attachments, reply_to, sent_ts, automation, profile_name, attempts, next_attempt_ts, defer_reason, ...}
    pending_codex_jobs_by_scope: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Dangerous override confirmations (when Codex asks user to re-run with ∆).
    # chat_id -> {request_id -> {payload, attachments, reply_to, sent_ts, user_id, message_id, created_ts, expires_ts}}
    pending_dangerous_confirmations_by_chat: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    # Dangerous override confirmations (Telegram topics/threads: chat_id + message_thread_id).
    # scope_key -> {request_id -> {payload, attachments, reply_to, sent_ts, user_id, message_id, created_ts, expires_ts}}
    pending_dangerous_confirmations_by_scope: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    # Pending voice-route choices for auto-transcribed voice messages.
    # chat_id -> {voice_message_id -> {choice?, created_ts, selected_ts?, expires_ts}}
    pending_voice_routes_by_chat: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    # Pending voice-route choices for auto-transcribed voice messages (Telegram topics/threads: chat_id + message_thread_id).
    # scope_key -> {voice_message_id -> {choice?, created_ts, selected_ts?, expires_ts}}
    pending_voice_routes_by_scope: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    # Pending follow-ups captured while a Codex job is running for this scope.
    # scope_key -> [{message_id, user_id, received_ts, text, attachments?, reply_to?}]
    pending_followups_by_scope: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    # Per-scope collect queue state: one active item, queued pending items, and completed deferred items.
    # scope_key -> {payload}
    collect_active: dict[str, dict[str, Any]] = field(default_factory=dict)
    # scope_key -> [{payload}]
    collect_pending: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # scope_key -> [{payload}]
    collect_deferred: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # Per-scope collect packet decisions (pending / forced) for retries and oversize control.
    # scope_key -> {packet_id -> {status, created_ts, reasons, report, forced}}
    collect_packet_decisions_by_scope: dict[str, dict[str, dict[str, Any]]] = field(
        default_factory=dict
    )

    # user-in-the-loop: blocking question asked; scope is waiting for user's answer.
    # scope_key -> {asked_ts, question, default?, ping_count, last_ping_ts, ...}
    waiting_for_user_by_scope: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Live chatter throttling (Telegram topics/threads: chat_id + message_thread_id).
    # scope_key -> last_sent_ts
    live_chatter_last_sent_ts_by_scope: dict[str, float] = field(default_factory=dict)

    # Per-chat UX settings (Telegram delivery)
    # chat_id -> bool
    ux_prefer_edit_delivery_by_chat: dict[str, bool] = field(default_factory=dict)
    # chat_id -> bool (send a short "✅ Готово" notice when the final answer was delivered via edit)
    ux_done_notice_enabled_by_chat: dict[str, bool] = field(default_factory=dict)
    # chat_id -> seconds (auto-delete for the "✅ Готово" notice)
    ux_done_notice_delete_seconds_by_chat: dict[str, int] = field(default_factory=dict)
    # chat_id -> bool (allow proactive bot initiatives: watcher pings, auto gentle mode changes, etc.)
    ux_bot_initiatives_enabled_by_chat: dict[str, bool] = field(default_factory=dict)
    # chat_id -> bool (allow proactive "live chatter" messages inside task topics)
    ux_live_chatter_enabled_by_chat: dict[str, bool] = field(default_factory=dict)
    # chat_id -> bool (allow MCP live loop: telegram-send send_message + get_followups/ack_followups)
    ux_mcp_live_enabled_by_chat: dict[str, bool] = field(default_factory=dict)
    # chat_id -> bool (allow bot-side user-in-the-loop via `ask_user` control block)
    ux_user_in_loop_enabled_by_chat: dict[str, bool] = field(default_factory=dict)

    # Ring buffer of recent events/messages for context injection
    history: list[dict[str, Any]] = field(default_factory=list)

    # Lightweight counters/timers for observability (/stats).
    # Stored as a flat dict to keep schema flexible.
    metrics: dict[str, int | float] = field(default_factory=dict)

    def load(self) -> None:
        with self.lock:
            try:
                if not self.path.exists():
                    return
                data = json.loads(self.path.read_text(encoding='utf-8') or '{}')
            except Exception:
                return

            self.tg_offset = int(data.get('tg_offset') or 0)
            self.last_chat_id = int(data.get('last_chat_id') or 0)
            self.last_user_id = int(data.get('last_user_id') or 0)
            self.watch_chat_id = int(data.get('watch_chat_id') or 0)
            if self.watch_chat_id <= 0 and self.last_chat_id > 0:
                # Best-effort migration: keep watcher targeting a private chat even if `last_chat_id` later becomes a group.
                self.watch_chat_id = int(self.last_chat_id)
            self.reminders_chat_id = int(data.get('reminders_chat_id') or 0)
            self.reminders_message_thread_id = int(data.get('reminders_message_thread_id') or 0)
            if self.reminders_chat_id == 0:
                self.reminders_message_thread_id = 0
            self.last_user_msg_ts = float(data.get('last_user_msg_ts') or 0.0)
            lut = data.get('last_user_msg_ts_by_chat') or {}
            if isinstance(lut, dict):
                cleaned_lut: dict[str, float] = {}
                for k, v in lut.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    try:
                        ts = float(v or 0.0)
                    except Exception:
                        ts = 0.0
                    if ts > 0:
                        cleaned_lut[chat_key] = ts
                self.last_user_msg_ts_by_chat = cleaned_lut
            self.snooze_until_ts = float(data.get('snooze_until_ts') or 0.0)
            self.snooze_kind = str(data.get('snooze_kind') or '')
            sleep_map = data.get('sleep_until_by_scope') or {}
            if isinstance(sleep_map, dict):
                cleaned_sleep_until: dict[str, float] = {}
                for k, v in sleep_map.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    try:
                        ts = float(v or 0.0)
                    except Exception:
                        ts = 0.0
                    if ts > 0:
                        cleaned_sleep_until[sk] = ts
                if cleaned_sleep_until:
                    self.sleep_until_by_scope = cleaned_sleep_until

            self.gentle_until_ts = float(data.get('gentle_until_ts') or 0.0)
            self.gentle_reason = str(data.get('gentle_reason') or '')

            me = data.get('mute_events_ts')
            if isinstance(me, list):
                cleaned_mute_events: list[float] = []
                for x in me:
                    try:
                        cleaned_mute_events.append(float(x))
                    except Exception:
                        continue
                self.mute_events_ts = cleaned_mute_events

            self.last_ping_ts = float(data.get('last_ping_ts') or 0.0)
            self.last_ping_stage = int(data.get('last_ping_stage') or 0)
            self.touch_ts_at_ping = float(data.get('touch_ts_at_ping') or 0.0)

            rs = data.get('reminders_sent') or {}
            if isinstance(rs, dict):
                cleaned_rs: dict[str, list[str]] = {}
                for k, v in rs.items():
                    if isinstance(k, str) and isinstance(v, list):
                        cleaned_rs[k] = [str(x) for x in v]
                self.reminders_sent = cleaned_rs

            rp = data.get('reminders_pending') or {}
            if isinstance(rp, dict):
                cleaned_rp: dict[str, list[str]] = {}
                for k, v in rp.items():
                    if isinstance(k, str) and isinstance(v, list):
                        cleaned_rp[k] = [str(x) for x in v]
                self.reminders_pending = cleaned_rp

            mm_sent = data.get('mm_sent_up_to_ts_by_channel') or {}
            if isinstance(mm_sent, dict):
                cleaned_mm_sent: dict[str, int] = {}
                for k, v in mm_sent.items():
                    cid = str(k or '').strip()
                    if not cid:
                        continue
                    try:
                        ts = int(v)
                    except Exception:
                        continue
                    if ts > 0:
                        cleaned_mm_sent[cid] = int(ts)
                self.mm_sent_up_to_ts_by_channel = cleaned_mm_sent

            mm_pending = data.get('mm_pending_up_to_ts_by_channel') or {}
            if isinstance(mm_pending, dict):
                cleaned_mm_pending: dict[str, int] = {}
                for k, v in mm_pending.items():
                    cid = str(k or '').strip()
                    if not cid:
                        continue
                    try:
                        ts = int(v)
                    except Exception:
                        continue
                    if ts > 0:
                        cleaned_mm_pending[cid] = int(ts)
                self.mm_pending_up_to_ts_by_channel = cleaned_mm_pending

            self.mm_mfa_token = str(data.get('mm_mfa_token') or '').strip()
            self.mm_mfa_token_set_ts = float(data.get('mm_mfa_token_set_ts') or 0.0)
            self.mm_mfa_prompt_ts = float(data.get('mm_mfa_prompt_ts') or 0.0)
            self.mm_mfa_required_ts = float(data.get('mm_mfa_required_ts') or 0.0)
            self.mm_session_token = str(data.get('mm_session_token') or '').strip()
            self.mm_session_token_set_ts = float(data.get('mm_session_token_set_ts') or 0.0)

            self.last_codex_ts = float(data.get('last_codex_ts') or 0.0)
            self.last_codex_automation = bool(data.get('last_codex_automation') or False)
            self.last_codex_profile = str(data.get('last_codex_profile') or '')
            self.last_codex_mode = _normalize_codex_mode(data.get('last_codex_mode'))
            self.last_codex_model = str(data.get('last_codex_model') or '').strip()
            self.last_codex_reasoning = _normalize_codex_reasoning(data.get('last_codex_reasoning'))

            lct = data.get('last_codex_ts_by_chat') or {}
            if isinstance(lct, dict):
                cleaned_lct: dict[str, float] = {}
                for k, v in lct.items():
                    try:
                        chat_key = str(int(k))
                        cleaned_lct[chat_key] = float(v)
                    except Exception:
                        continue
                self.last_codex_ts_by_chat = cleaned_lct

            lca = data.get('last_codex_automation_by_chat') or {}
            if isinstance(lca, dict):
                cleaned_lca: dict[str, bool] = {}
                for k, v in lca.items():
                    try:
                        chat_key = str(int(k))
                        cleaned_lca[chat_key] = bool(v)
                    except Exception:
                        continue
                self.last_codex_automation_by_chat = cleaned_lca

            lcp = data.get('last_codex_profile_by_chat') or {}
            if isinstance(lcp, dict):
                cleaned_lcp: dict[str, str] = {}
                for k, v in lcp.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    name = str(v or '').strip()
                    if not name:
                        continue
                    cleaned_lcp[chat_key] = name
                self.last_codex_profile_by_chat = cleaned_lcp

            lcm = data.get('last_codex_mode_by_chat') or {}
            if isinstance(lcm, dict):
                cleaned_lcm: dict[str, str] = {}
                for k, v in lcm.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    mode = _normalize_codex_mode(v)
                    if not mode:
                        continue
                    cleaned_lcm[chat_key] = mode
                self.last_codex_mode_by_chat = cleaned_lcm

            lmc = data.get('last_codex_model_by_chat') or {}
            if isinstance(lmc, dict):
                cleaned_lmc: dict[str, str] = {}
                for k, v in lmc.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    model = str(v or '').strip()
                    if not model:
                        continue
                    cleaned_lmc[chat_key] = model
                self.last_codex_model_by_chat = cleaned_lmc

            lcr = data.get('last_codex_reasoning_by_chat') or {}
            if isinstance(lcr, dict):
                cleaned_lcr: dict[str, str] = {}
                for k, v in lcr.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    reasoning = _normalize_codex_reasoning(v)
                    if not reasoning:
                        continue
                    cleaned_lcr[chat_key] = reasoning
                self.last_codex_reasoning_by_chat = cleaned_lcr

            # Best-effort migration from legacy single-chat fields.
            if self.last_chat_id and self.last_codex_ts and not self.last_codex_ts_by_chat:
                self.last_codex_ts_by_chat[str(int(self.last_chat_id))] = float(self.last_codex_ts)
            if self.last_chat_id and not self.last_codex_automation_by_chat:
                self.last_codex_automation_by_chat[str(int(self.last_chat_id))] = bool(self.last_codex_automation)
            if not self.last_codex_profile:
                self.last_codex_profile = 'auto' if self.last_codex_automation else 'chat'
            if self.last_chat_id and self.last_codex_profile:
                key = str(int(self.last_chat_id))
                if key not in self.last_codex_profile_by_chat:
                    self.last_codex_profile_by_chat[key] = str(self.last_codex_profile)

            # Backfill per-chat profile names if missing.
            for chat_key, is_auto in self.last_codex_automation_by_chat.items():
                if chat_key in self.last_codex_profile_by_chat:
                    continue
                self.last_codex_profile_by_chat[chat_key] = 'auto' if bool(is_auto) else 'chat'

            scope_ts = data.get('last_codex_ts_by_scope') or {}
            if isinstance(scope_ts, dict):
                cleaned_scope_ts: dict[str, float] = {}
                for k, v in scope_ts.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    try:
                        ts = float(v or 0.0)
                    except Exception:
                        ts = 0.0
                    if ts > 0:
                        cleaned_scope_ts[sk] = float(ts)
                self.last_codex_ts_by_scope = cleaned_scope_ts

            scope_auto = data.get('last_codex_automation_by_scope') or {}
            if isinstance(scope_auto, dict):
                cleaned_scope_auto: dict[str, bool] = {}
                for k, v in scope_auto.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    cleaned_scope_auto[sk] = bool(v)
                self.last_codex_automation_by_scope = cleaned_scope_auto

            scope_profile = data.get('last_codex_profile_by_scope') or {}
            if isinstance(scope_profile, dict):
                cleaned_scope_profile: dict[str, str] = {}
                for k, v in scope_profile.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    name = str(v or '').strip()
                    if not name:
                        continue
                    cleaned_scope_profile[sk] = name
                self.last_codex_profile_by_scope = cleaned_scope_profile

            scope_mode = data.get('last_codex_mode_by_scope') or {}
            if isinstance(scope_mode, dict):
                cleaned_scope_mode: dict[str, str] = {}
                for k, v in scope_mode.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    mode = _normalize_codex_mode(v)
                    if not mode:
                        continue
                    cleaned_scope_mode[sk] = mode
                self.last_codex_mode_by_scope = cleaned_scope_mode

            scope_model = data.get('last_codex_model_by_scope') or {}
            if isinstance(scope_model, dict):
                cleaned_scope_model: dict[str, str] = {}
                for k, v in scope_model.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    model = str(v or '').strip()
                    if not model:
                        continue
                    cleaned_scope_model[sk] = model
                self.last_codex_model_by_scope = cleaned_scope_model

            scope_reasoning = data.get('last_codex_reasoning_by_scope') or {}
            if isinstance(scope_reasoning, dict):
                cleaned_scope_reasoning: dict[str, str] = {}
                for k, v in scope_reasoning.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    reasoning = _normalize_codex_reasoning(v)
                    if not reasoning:
                        continue
                    cleaned_scope_reasoning[sk] = reasoning
                self.last_codex_reasoning_by_scope = cleaned_scope_reasoning

            # Best-effort migration: if scope-level maps are missing, treat chat-level maps as "<chat_id>:0".
            if not self.last_codex_ts_by_scope and self.last_codex_ts_by_chat:
                self.last_codex_ts_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): float(ts)
                    for chat_key, ts in self.last_codex_ts_by_chat.items()
                }
            if not self.last_codex_automation_by_scope and self.last_codex_automation_by_chat:
                self.last_codex_automation_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): bool(v)
                    for chat_key, v in self.last_codex_automation_by_chat.items()
                }
            if not self.last_codex_profile_by_scope and self.last_codex_profile_by_chat:
                self.last_codex_profile_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): str(name)
                    for chat_key, name in self.last_codex_profile_by_chat.items()
                    if str(name or '').strip()
                }
            if not self.last_codex_mode_by_scope and self.last_codex_mode_by_chat:
                self.last_codex_mode_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): str(mode)
                    for chat_key, mode in self.last_codex_mode_by_chat.items()
                }
            if not self.last_codex_model_by_scope and self.last_codex_model_by_chat:
                self.last_codex_model_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): str(model)
                    for chat_key, model in self.last_codex_model_by_chat.items()
                }
            if not self.last_codex_reasoning_by_scope and self.last_codex_reasoning_by_chat:
                self.last_codex_reasoning_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): str(reasoning)
                    for chat_key, reasoning in self.last_codex_reasoning_by_chat.items()
                }

            if not self.last_codex_mode and self.last_codex_profile:
                self.last_codex_mode = 'write' if str(self.last_codex_profile).strip().casefold() in {'auto', 'danger'} else 'read'
            if not self.last_codex_mode and self.last_codex_automation:
                self.last_codex_mode = 'write'
            if not self.last_codex_mode:
                self.last_codex_mode = 'read'

        self.restart_pending = bool(data.get('restart_pending') or False)
        self.restart_requested_ts = float(data.get('restart_requested_ts') or 0.0)
        self.restart_shutting_down_ts = float(data.get('restart_shutting_down_ts') or 0.0)
        self.restart_requested_chat_id = int(data.get('restart_requested_chat_id') or 0)
        self.restart_requested_message_thread_id = int(data.get('restart_requested_message_thread_id') or 0)
        self.restart_requested_user_id = int(data.get('restart_requested_user_id') or 0)
        self.restart_requested_message_id = int(data.get('restart_requested_message_id') or 0)
        self.restart_requested_ack_message_id = int(data.get('restart_requested_ack_message_id') or 0)

        pa = data.get('pending_attachments_by_chat') or {}
        if isinstance(pa, dict):
            cleaned_pa: dict[str, list[dict[str, Any]]] = {}
            for k, v in pa.items():
                try:
                    chat_key = str(int(k))
                except Exception:
                    continue
                if not isinstance(v, list):
                    continue
                cleaned_pa_items: list[dict[str, Any]] = []
                for item in v:
                    if not isinstance(item, dict):
                        continue
                    path_raw = item.get('path')
                    name_raw = item.get('name')
                    if not isinstance(path_raw, str) or not path_raw.strip():
                        continue
                    path_s = path_raw.strip()
                    name_s = name_raw.strip() if isinstance(name_raw, str) and name_raw.strip() else Path(path_s).name
                    kind = item.get('kind')
                    kind_s = str(kind or '').strip()[:32]
                    try:
                        size_bytes = int(item.get('size_bytes') or 0)
                    except Exception:
                        size_bytes = 0
                    try:
                        ts = float(item.get('ts') or 0.0)
                    except Exception:
                        ts = 0.0
                    cleaned_pa_items.append(
                        {
                            'path': path_s,
                            'name': name_s.strip(),
                            'kind': kind_s,
                            'size_bytes': size_bytes,
                            'ts': ts,
                        }
                    )
                if cleaned_pa_items:
                    cleaned_pa[chat_key] = cleaned_pa_items
            self.pending_attachments_by_chat = cleaned_pa

            pas = data.get('pending_attachments_by_scope') or {}
            if isinstance(pas, dict):
                cleaned_pas: dict[str, list[dict[str, Any]]] = {}
                for k, v in pas.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, list):
                        continue
                    cleaned_pas_items: list[dict[str, Any]] = []
                    for item in v:
                        if not isinstance(item, dict):
                            continue
                        path_raw = item.get('path')
                        name_raw = item.get('name')
                        if not isinstance(path_raw, str) or not path_raw.strip():
                            continue
                        path_s = path_raw.strip()
                        name_s = (
                            name_raw.strip() if isinstance(name_raw, str) and name_raw.strip() else Path(path_s).name
                        )
                        kind = item.get('kind')
                        kind_s = str(kind or '').strip()[:32]
                        try:
                            size_bytes = int(item.get('size_bytes') or 0)
                        except Exception:
                            size_bytes = 0
                        try:
                            ts = float(item.get('ts') or 0.0)
                        except Exception:
                            ts = 0.0
                        cleaned_pas_items.append(
                            {
                                'path': path_s,
                                'name': name_s.strip(),
                                'kind': kind_s,
                                'size_bytes': size_bytes,
                                'ts': ts,
                            }
                        )
                    if cleaned_pas_items:
                        cleaned_pas[sk] = cleaned_pas_items
                self.pending_attachments_by_scope = cleaned_pas

            if not self.pending_attachments_by_scope and self.pending_attachments_by_chat:
                self.pending_attachments_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): list(items)
                    for chat_key, items in self.pending_attachments_by_chat.items()
                }

            ai = data.get('attachments_index_by_chat') or {}
            if isinstance(ai, dict):
                cleaned_ai: dict[str, list[dict[str, Any]]] = {}
                for k, v in ai.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    if not isinstance(v, list):
                        continue
                    cleaned_items: list[dict[str, Any]] = []
                    for item in v:
                        if not isinstance(item, dict):
                            continue
                        try:
                            mid = int(item.get('message_id') or 0)
                        except Exception:
                            mid = 0
                        if mid <= 0:
                            continue
                        attachments_raw = item.get('attachments') or []
                        if not isinstance(attachments_raw, list):
                            continue
                        cleaned_atts: list[dict[str, Any]] = []
                        for a in attachments_raw:
                            if not isinstance(a, dict):
                                continue
                            path_raw = a.get('path')
                            name_raw = a.get('name')
                            if not isinstance(path_raw, str) or not path_raw.strip():
                                continue
                            path_s = path_raw.strip()
                            name_s = (
                                name_raw.strip()
                                if isinstance(name_raw, str) and name_raw.strip()
                                else Path(path_s).name
                            )
                            kind = a.get('kind')
                            kind_s = str(kind or '').strip()[:32]
                            try:
                                size_bytes = int(a.get('size_bytes') or 0)
                            except Exception:
                                size_bytes = 0
                            cleaned_atts.append(
                                {
                                    'path': path_s,
                                    'name': name_s.strip(),
                                    'kind': kind_s,
                                    'size_bytes': size_bytes,
                                }
                            )
                        if not cleaned_atts:
                            continue
                        try:
                            ts = float(item.get('ts') or 0.0)
                        except Exception:
                            ts = 0.0
                        cleaned_items.append({'message_id': mid, 'attachments': cleaned_atts, 'ts': ts})
                    if cleaned_items:
                        cleaned_ai[chat_key] = cleaned_items
                self.attachments_index_by_chat = cleaned_ai

            pr = data.get('pending_reply_to_by_chat') or {}
            if isinstance(pr, dict):
                cleaned_pr: dict[str, dict[str, Any]] = {}
                for k, v in pr.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_reply: dict[str, Any] = {}
                    for kk in [
                        'message_id',
                        'sent_ts',
                        'from_is_bot',
                        'from_user_id',
                        'from_name',
                        'text',
                        'attachments',
                    ]:
                        if kk in v:
                            cleaned_reply[kk] = v.get(kk)
                    quote_raw = v.get('quote')
                    if isinstance(quote_raw, dict):
                        quote_cleaned_chat: dict[str, Any] = {}
                        q_text = quote_raw.get('text')
                        if isinstance(q_text, str) and q_text.strip():
                            quote_cleaned_chat['text'] = q_text.strip()
                        q_pos = quote_raw.get('position')
                        if isinstance(q_pos, (int, float)):
                            quote_cleaned_chat['position'] = int(q_pos)
                        q_is_manual = quote_raw.get('is_manual')
                        if isinstance(q_is_manual, bool):
                            quote_cleaned_chat['is_manual'] = bool(q_is_manual)
                        if quote_cleaned_chat:
                            cleaned_reply['quote'] = quote_cleaned_chat
                    if cleaned_reply:
                        cleaned_pr[chat_key] = cleaned_reply
                self.pending_reply_to_by_chat = cleaned_pr

            prs = data.get('pending_reply_to_by_scope') or {}
            if isinstance(prs, dict):
                cleaned_prs: dict[str, dict[str, Any]] = {}
                for k, v in prs.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_reply_scope: dict[str, Any] = {}
                    for kk in [
                        'message_id',
                        'sent_ts',
                        'from_is_bot',
                        'from_user_id',
                        'from_name',
                        'text',
                        'attachments',
                    ]:
                        if kk in v:
                            cleaned_reply_scope[kk] = v.get(kk)
                    quote_raw = v.get('quote')
                    if isinstance(quote_raw, dict):
                        quote_cleaned_scope: dict[str, Any] = {}
                        q_text = quote_raw.get('text')
                        if isinstance(q_text, str) and q_text.strip():
                            quote_cleaned_scope['text'] = q_text.strip()
                        q_pos = quote_raw.get('position')
                        if isinstance(q_pos, (int, float)):
                            quote_cleaned_scope['position'] = int(q_pos)
                        q_is_manual = quote_raw.get('is_manual')
                        if isinstance(q_is_manual, bool):
                            quote_cleaned_scope['is_manual'] = bool(q_is_manual)
                        if quote_cleaned_scope:
                            cleaned_reply_scope['quote'] = quote_cleaned_scope
                    if cleaned_reply_scope:
                        cleaned_prs[sk] = cleaned_reply_scope
                self.pending_reply_to_by_scope = cleaned_prs

            if not self.pending_reply_to_by_scope and self.pending_reply_to_by_chat:
                self.pending_reply_to_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): dict(v)
                    for chat_key, v in self.pending_reply_to_by_chat.items()
                    if isinstance(v, dict)
                }

            ob = data.get('tg_outbox') or []
            if isinstance(ob, list):
                cleaned_ob: list[dict[str, Any]] = []
                for item in ob:
                    if not isinstance(item, dict):
                        continue
                    op = item.get('op')
                    params = item.get('params')
                    if not isinstance(op, str) or not op.strip():
                        continue
                    if not isinstance(params, dict):
                        params = {}
                    try:
                        chat_id = int(item.get('chat_id') or 0)
                    except Exception:
                        chat_id = 0
                    if chat_id == 0:
                        continue
                    iid = item.get('id')
                    item_id = str(iid or '').strip()
                    if not item_id:
                        continue
                    try:
                        created_ts = float(item.get('created_ts') or 0.0)
                    except Exception:
                        created_ts = 0.0
                    try:
                        attempts = int(item.get('attempts') or 0)
                    except Exception:
                        attempts = 0
                    try:
                        next_attempt_ts = float(item.get('next_attempt_ts') or 0.0)
                    except Exception:
                        next_attempt_ts = 0.0
                    last_error = item.get('last_error')
                    last_error_s = str(last_error or '')[:400] if last_error is not None else ''
                    coalesce_key = item.get('coalesce_key')
                    coalesce_s = str(coalesce_key or '').strip()[:64] if coalesce_key is not None else ''
                    meta_raw = item.get('meta')
                    meta_cleaned: dict[str, Any] | None = None
                    if isinstance(meta_raw, dict) and meta_raw:
                        cleaned_meta: dict[str, Any] = {}
                        for mk, mv in meta_raw.items():
                            if not isinstance(mk, str) or not mk.strip():
                                continue
                            key = mk.strip()[:64]
                            if isinstance(mv, (str, int, float)) and not isinstance(mv, bool):
                                cleaned_meta[key] = mv
                            elif isinstance(mv, bool):
                                cleaned_meta[key] = bool(mv)
                            elif isinstance(mv, list):
                                # Keep small lists of primitives (e.g., reminder ids).
                                cleaned_meta_list: list[Any] = []
                                for x in mv[:50]:
                                    if isinstance(x, (str, int, float)) and not isinstance(x, bool):
                                        cleaned_meta_list.append(x)
                                    elif isinstance(x, bool):
                                        cleaned_meta_list.append(bool(x))
                                if cleaned_meta_list:
                                    cleaned_meta[key] = cleaned_meta_list
                        if cleaned_meta:
                            meta_cleaned = cleaned_meta

                    cleaned_item: dict[str, Any] = {
                        'id': item_id,
                        'op': op.strip()[:32],
                        'chat_id': int(chat_id),
                        'params': dict(params),
                        'created_ts': float(created_ts),
                        'attempts': int(attempts),
                        'next_attempt_ts': float(next_attempt_ts),
                        'last_error': last_error_s,
                    }
                    if coalesce_s:
                        cleaned_item['coalesce_key'] = coalesce_s
                    if meta_cleaned is not None:
                        cleaned_item['meta'] = meta_cleaned
                    cleaned_ob.append(cleaned_item)
                # Keep last N for safety.
                self.tg_outbox = cleaned_ob[-500:]

            midmap = data.get('tg_message_id_by_coalesce_key_by_chat') or {}
            if isinstance(midmap, dict):
                cleaned_midmap: dict[str, dict[str, int]] = {}
                for k, v in midmap.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_inner: dict[str, int] = {}
                    for kk, vv in v.items():
                        if not isinstance(kk, str) or not kk.strip():
                            continue
                        ck = kk.strip()[:64]
                        try:
                            mid = int(vv or 0)
                        except Exception:
                            mid = 0
                        if mid > 0:
                            cleaned_inner[ck] = mid
                    if cleaned_inner:
                        # Keep only the most recent N by insertion order.
                        cleaned_midmap[chat_key] = dict(list(cleaned_inner.items())[-200:])
                self.tg_message_id_by_coalesce_key_by_chat = cleaned_midmap

            off = data.get('tg_offline_since_by_chat') or {}
            if isinstance(off, dict):
                cleaned_off: dict[str, float] = {}
                for k, v in off.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    try:
                        ts = float(v or 0.0)
                    except Exception:
                        ts = 0.0
                    if ts > 0:
                        cleaned_off[chat_key] = float(ts)
                self.tg_offline_since_by_chat = cleaned_off

            ons = data.get('tg_offline_notice_sent_ts_by_chat') or {}
            if isinstance(ons, dict):
                cleaned_ons: dict[str, float] = {}
                for k, v in ons.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    try:
                        ts = float(v or 0.0)
                    except Exception:
                        ts = 0.0
                    if ts > 0:
                        cleaned_ons[chat_key] = float(ts)
                self.tg_offline_notice_sent_ts_by_chat = cleaned_ons

            pcj = data.get('pending_codex_jobs_by_chat') or {}
            if isinstance(pcj, dict):
                cleaned_pcj: dict[str, dict[str, Any]] = {}
                for k, v in pcj.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_pcj_item: dict[str, Any] = {}
                    # Keep only known fields (best-effort); add more anytime.
                    for kk in (
                        'payload',
                        'attachments',
                        'reply_to',
                        'sent_ts',
                        'automation',
                        'dangerous',
                        'profile_name',
                        'exec_mode',
                        'reason',
                        'model',
                        'message_id',
                        'user_id',
                        'created_ts',
                        'attempts',
                        'next_attempt_ts',
                        'last_error',
                    ):
                        if kk in v:
                            cleaned_pcj_item[kk] = v.get(kk)
                    if cleaned_pcj_item:
                        cleaned_pcj[chat_key] = cleaned_pcj_item
                self.pending_codex_jobs_by_chat = cleaned_pcj

            pcjs = data.get('pending_codex_jobs_by_scope') or {}
            if isinstance(pcjs, dict):
                cleaned_pcjs: dict[str, dict[str, Any]] = {}
                for k, v in pcjs.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_pcjs_item: dict[str, Any] = {}
                    for kk in (
                        'payload',
                        'attachments',
                        'reply_to',
                        'sent_ts',
                        'automation',
                        'dangerous',
                        'profile_name',
                        'exec_mode',
                        'reason',
                        'model',
                        'reasoning_effort',
                        'defer_reason',
                        'message_id',
                        'ack_message_id',
                        'message_thread_id',
                        'user_id',
                        'created_ts',
                        'attempts',
                        'next_attempt_ts',
                        'last_error',
                    ):
                        if kk in v:
                            cleaned_pcjs_item[kk] = v.get(kk)
                    if cleaned_pcjs_item:
                        cleaned_pcjs[sk] = cleaned_pcjs_item
                self.pending_codex_jobs_by_scope = cleaned_pcjs

            if not self.pending_codex_jobs_by_scope and self.pending_codex_jobs_by_chat:
                self.pending_codex_jobs_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): dict(v)
                    for chat_key, v in self.pending_codex_jobs_by_chat.items()
                    if isinstance(v, dict)
                }

            pdc = data.get('pending_dangerous_confirmations_by_chat') or {}
            if isinstance(pdc, dict):
                cleaned_pdc: dict[str, dict[str, dict[str, Any]]] = {}
                now = _now_ts()
                for k, v in pdc.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_pdc_chat: dict[str, dict[str, Any]] = {}
                    for rid, job_raw in v.items():
                        if not isinstance(rid, str) or not rid.strip():
                            continue
                        rid_s = rid.strip()[:32]
                        if not isinstance(job_raw, dict):
                            continue
                        # Keep only known fields (best-effort).
                        cleaned_pdc_job: dict[str, Any] = {}
                        payload = job_raw.get('payload')
                        if isinstance(payload, str) and payload.strip():
                            cleaned_pdc_job['payload'] = _clamp_text(payload, 12000)
                        attachments_raw = job_raw.get('attachments') or []
                        if isinstance(attachments_raw, list) and attachments_raw:
                            pdc_atts: list[dict[str, Any]] = []
                            for a in attachments_raw:
                                if isinstance(a, dict):
                                    pdc_atts.append(dict(a))
                            if pdc_atts:
                                cleaned_pdc_job['attachments'] = pdc_atts[:20]
                        reply_to = job_raw.get('reply_to')
                        if isinstance(reply_to, dict) and reply_to:
                            cleaned_pdc_job['reply_to'] = dict(reply_to)
                        try:
                            sent_ts = float(job_raw.get('sent_ts') or 0.0)
                        except Exception:
                            sent_ts = 0.0
                        if sent_ts > 0:
                            cleaned_pdc_job['sent_ts'] = float(sent_ts)
                        try:
                            user_id = int(job_raw.get('user_id') or 0)
                        except Exception:
                            user_id = 0
                        if user_id > 0:
                            cleaned_pdc_job['user_id'] = int(user_id)
                        try:
                            message_id = int(job_raw.get('message_id') or 0)
                        except Exception:
                            message_id = 0
                        if message_id > 0:
                            cleaned_pdc_job['message_id'] = int(message_id)
                        try:
                            created_ts = float(job_raw.get('created_ts') or 0.0)
                        except Exception:
                            created_ts = 0.0
                        if created_ts > 0:
                            cleaned_pdc_job['created_ts'] = float(created_ts)
                        try:
                            expires_ts = float(job_raw.get('expires_ts') or 0.0)
                        except Exception:
                            expires_ts = 0.0
                        if expires_ts > 0:
                            cleaned_pdc_job['expires_ts'] = float(expires_ts)
                        # Drop expired items on load.
                        if expires_ts > 0 and expires_ts <= now:
                            continue
                        if cleaned_pdc_job:
                            cleaned_pdc_chat[rid_s] = cleaned_pdc_job
                    if cleaned_pdc_chat:
                        cleaned_pdc[chat_key] = cleaned_pdc_chat
                self.pending_dangerous_confirmations_by_chat = cleaned_pdc

            pdcs = data.get('pending_dangerous_confirmations_by_scope') or {}
            if isinstance(pdcs, dict):
                cleaned_pdcs: dict[str, dict[str, dict[str, Any]]] = {}
                now = _now_ts()
                for k, v in pdcs.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict):
                        continue
                    cleaned_pdcs_scope: dict[str, dict[str, Any]] = {}
                    for rid, job_raw in v.items():
                        if not isinstance(rid, str) or not rid.strip():
                            continue
                        rid_s = rid.strip()[:32]
                        if not isinstance(job_raw, dict):
                            continue
                        cleaned_pdcs_job: dict[str, Any] = {}
                        payload = job_raw.get('payload')
                        if isinstance(payload, str) and payload.strip():
                            cleaned_pdcs_job['payload'] = _clamp_text(payload, 12000)
                        attachments_raw = job_raw.get('attachments') or []
                        if isinstance(attachments_raw, list) and attachments_raw:
                            pdcs_atts: list[dict[str, Any]] = []
                            for a in attachments_raw:
                                if isinstance(a, dict):
                                    pdcs_atts.append(dict(a))
                            if pdcs_atts:
                                cleaned_pdcs_job['attachments'] = pdcs_atts
                        reply_to_raw = job_raw.get('reply_to')
                        if isinstance(reply_to_raw, dict) and reply_to_raw:
                            cleaned_pdcs_job['reply_to'] = dict(reply_to_raw)
                        tg_chat_raw = job_raw.get('tg_chat')
                        if isinstance(tg_chat_raw, dict) and tg_chat_raw:
                            cleaned_pdcs_job['tg_chat'] = dict(tg_chat_raw)
                        tg_user_raw = job_raw.get('tg_user')
                        if isinstance(tg_user_raw, dict) and tg_user_raw:
                            cleaned_pdcs_job['tg_user'] = dict(tg_user_raw)
                        for kk in ('sent_ts', 'user_id', 'message_id', 'created_ts', 'expires_ts'):
                            if kk in job_raw:
                                cleaned_pdcs_job[kk] = job_raw.get(kk)
                        try:
                            expires_ts = float(job_raw.get('expires_ts') or 0.0)
                        except Exception:
                            expires_ts = 0.0
                        if expires_ts > 0:
                            cleaned_pdcs_job['expires_ts'] = float(expires_ts)
                        if expires_ts > 0 and expires_ts <= now:
                            continue
                        if cleaned_pdcs_job:
                            cleaned_pdcs_scope[rid_s] = cleaned_pdcs_job
                    if cleaned_pdcs_scope:
                        cleaned_pdcs[sk] = cleaned_pdcs_scope
                self.pending_dangerous_confirmations_by_scope = cleaned_pdcs

            if not self.pending_dangerous_confirmations_by_scope and self.pending_dangerous_confirmations_by_chat:
                self.pending_dangerous_confirmations_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): dict(per_chat)
                    for chat_key, per_chat in self.pending_dangerous_confirmations_by_chat.items()
                    if isinstance(per_chat, dict) and per_chat
                }

            pvr = data.get('pending_voice_routes_by_chat') or {}
            if isinstance(pvr, dict):
                cleaned_pvr: dict[str, dict[str, dict[str, Any]]] = {}
                now = _now_ts()
                for k, v in pvr.items():
                    try:
                        chat_key = str(int(k))
                    except Exception:
                        continue
                    if not isinstance(v, dict) or not v:
                        continue
                    cleaned_pvr_chat: dict[str, dict[str, Any]] = {}
                    for mid_raw, entry_raw in v.items():
                        try:
                            mid = int(mid_raw)
                        except Exception:
                            continue
                        if mid <= 0 or not isinstance(entry_raw, dict):
                            continue
                        choice_raw = entry_raw.get('choice')
                        choice = str(choice_raw or '').strip().lower()[:16]
                        if choice and choice not in {'read', 'write', 'danger', 'none'}:
                            choice = ''
                        try:
                            created_ts = float(entry_raw.get('created_ts') or 0.0)
                        except Exception:
                            created_ts = 0.0
                        try:
                            selected_ts = float(entry_raw.get('selected_ts') or 0.0)
                        except Exception:
                            selected_ts = 0.0
                        try:
                            expires_ts = float(entry_raw.get('expires_ts') or 0.0)
                        except Exception:
                            expires_ts = 0.0
                        # Best-effort TTL if older versions didn't store expiry.
                        if expires_ts <= 0 and created_ts > 0:
                            expires_ts = created_ts + 24 * 60 * 60
                        if expires_ts > 0 and expires_ts <= now:
                            continue
                        cleaned_pvr_entry: dict[str, Any] = {}
                        if choice:
                            cleaned_pvr_entry['choice'] = choice
                        if created_ts > 0:
                            cleaned_pvr_entry['created_ts'] = float(created_ts)
                        if selected_ts > 0:
                            cleaned_pvr_entry['selected_ts'] = float(selected_ts)
                        if expires_ts > 0:
                            cleaned_pvr_entry['expires_ts'] = float(expires_ts)
                        if cleaned_pvr_entry:
                            cleaned_pvr_chat[str(mid)] = cleaned_pvr_entry
                    if cleaned_pvr_chat:
                        cleaned_pvr[chat_key] = cleaned_pvr_chat
                self.pending_voice_routes_by_chat = cleaned_pvr

            pvrs = data.get('pending_voice_routes_by_scope') or {}
            if isinstance(pvrs, dict):
                cleaned_pvrs: dict[str, dict[str, dict[str, Any]]] = {}
                now = _now_ts()
                for k, v in pvrs.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict) or not v:
                        continue
                    cleaned_pvrs_scope: dict[str, dict[str, Any]] = {}
                    for mid_raw, entry_raw in v.items():
                        try:
                            mid = int(mid_raw)
                        except Exception:
                            continue
                        if mid <= 0 or not isinstance(entry_raw, dict):
                            continue
                        choice_raw = entry_raw.get('choice')
                        choice = str(choice_raw or '').strip().lower()[:16]
                        if choice and choice not in {'read', 'write', 'danger', 'none'}:
                            choice = ''
                        try:
                            created_ts = float(entry_raw.get('created_ts') or 0.0)
                        except Exception:
                            created_ts = 0.0
                        try:
                            selected_ts = float(entry_raw.get('selected_ts') or 0.0)
                        except Exception:
                            selected_ts = 0.0
                        try:
                            expires_ts = float(entry_raw.get('expires_ts') or 0.0)
                        except Exception:
                            expires_ts = 0.0
                        if expires_ts <= 0 and created_ts > 0:
                            expires_ts = created_ts + 24 * 60 * 60
                        if expires_ts > 0 and expires_ts <= now:
                            continue
                        cleaned_pvrs_entry: dict[str, Any] = {}
                        if choice:
                            cleaned_pvrs_entry['choice'] = choice
                        if created_ts > 0:
                            cleaned_pvrs_entry['created_ts'] = float(created_ts)
                        if selected_ts > 0:
                            cleaned_pvrs_entry['selected_ts'] = float(selected_ts)
                        if expires_ts > 0:
                            cleaned_pvrs_entry['expires_ts'] = float(expires_ts)
                        if cleaned_pvrs_entry:
                            cleaned_pvrs_scope[str(mid)] = cleaned_pvrs_entry
                    if cleaned_pvrs_scope:
                        cleaned_pvrs[sk] = cleaned_pvrs_scope
                self.pending_voice_routes_by_scope = cleaned_pvrs

            if not self.pending_voice_routes_by_scope and self.pending_voice_routes_by_chat:
                self.pending_voice_routes_by_scope = {
                    _scope_key(chat_id=int(chat_key), message_thread_id=0): dict(per_chat)
                    for chat_key, per_chat in self.pending_voice_routes_by_chat.items()
                    if isinstance(per_chat, dict) and per_chat
                }

            pfu = data.get('pending_followups_by_scope') or {}
            if isinstance(pfu, dict):
                cleaned_pfu: dict[str, list[dict[str, Any]]] = {}
                for k, v in pfu.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, list) or not v:
                        continue
                    cleaned_followups: list[dict[str, Any]] = []
                    for item in v:
                        if not isinstance(item, dict):
                            continue
                        try:
                            mid = int(item.get('message_id') or 0)
                        except Exception:
                            mid = 0
                        if mid <= 0:
                            continue
                        try:
                            user_id = int(item.get('user_id') or 0)
                        except Exception:
                            user_id = 0
                        if user_id <= 0:
                            continue
                        try:
                            received_ts = float(item.get('received_ts') or 0.0)
                        except Exception:
                            received_ts = 0.0
                        text = item.get('text')
                        if not isinstance(text, str) or not text.strip():
                            continue
                        cleaned_followup: dict[str, Any] = {
                            'message_id': int(mid),
                            'user_id': int(user_id),
                            'received_ts': float(received_ts),
                            'text': _clamp_text(text, 12000),
                        }
                        attachments = item.get('attachments') or []
                        if isinstance(attachments, list) and attachments:
                            cleaned_attachments: list[dict[str, Any]] = []
                            for a in attachments:
                                if isinstance(a, dict) and a:
                                    cleaned_attachments.append(dict(a))
                            if cleaned_attachments:
                                cleaned_followup['attachments'] = cleaned_attachments[:20]
                        reply_to = item.get('reply_to')
                        if isinstance(reply_to, dict) and reply_to:
                            cleaned_followup['reply_to'] = dict(reply_to)
                        cleaned_followups.append(cleaned_followup)
                    if cleaned_followups:
                        cleaned_pfu[sk] = cleaned_followups[-200:]
                self.pending_followups_by_scope = cleaned_pfu

            ca = data.get('collect_active') or {}
            if isinstance(ca, dict):
                cleaned_ca: dict[str, dict[str, Any]] = {}
                for k, v in ca.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict) or not v:
                        continue
                    cleaned_ca[sk] = dict(v)
                self.collect_active = cleaned_ca

            cp = data.get('collect_pending') or {}
            if isinstance(cp, dict):
                cleaned_cp: dict[str, list[dict[str, Any]]] = {}
                for k, v in cp.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, list):
                        continue
                    cleaned_cp_items: list[dict[str, Any]] = []
                    for item in v:
                        if not isinstance(item, dict):
                            continue
                        cleaned_cp_items.append(dict(item))
                    if cleaned_cp_items:
                        cleaned_cp[sk] = cleaned_cp_items[-200:]
                self.collect_pending = cleaned_cp

            cd = data.get('collect_deferred') or {}
            if isinstance(cd, dict):
                cleaned_cd: dict[str, list[dict[str, Any]]] = {}
                for k, v in cd.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, list):
                        continue
                    cleaned_cd_items: list[dict[str, Any]] = []
                    for item in v:
                        if not isinstance(item, dict):
                            continue
                        cleaned_cd_items.append(dict(item))
                    if cleaned_cd_items:
                        cleaned_cd[sk] = cleaned_cd_items[-200:]
                self.collect_deferred = cleaned_cd

            cpd = data.get('collect_packet_decisions_by_scope') or {}
            if isinstance(cpd, dict):
                cleaned_cpd: dict[str, dict[str, dict[str, Any]]] = {}
                for k, v in cpd.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict):
                        continue
                    per_scope: dict[str, dict[str, Any]] = {}
                    for packet_id_raw, decision_raw in v.items():
                        pid = str(packet_id_raw or '').strip()
                        if not pid:
                            continue
                        if not isinstance(decision_raw, dict):
                            continue
                        status = str(decision_raw.get('status') or '').strip().lower()
                        if status not in {'pending', 'forced'}:
                            continue
                        reasons_raw = decision_raw.get('reasons')
                        if not isinstance(reasons_raw, list):
                            reasons: list[str] = []
                        else:
                            reasons = [str(x).strip() for x in reasons_raw if str(x).strip()]
                        try:
                            created_ts = float(decision_raw.get('created_ts') or 0.0)
                        except (TypeError, ValueError):
                            created_ts = 0.0
                        cleaned_decision: dict[str, Any] = {
                            'status': status,
                            'created_ts': created_ts,
                            'reasons': reasons,
                            'forced': status == 'forced',
                        }
                        report = decision_raw.get('report')
                        if isinstance(report, dict):
                            cleaned_decision['report'] = dict(report)
                        per_scope[pid] = cleaned_decision
                    if per_scope:
                        cleaned_cpd[sk] = per_scope
                self.collect_packet_decisions_by_scope = cleaned_cpd

            wfu = data.get('waiting_for_user_by_scope') or {}
            if isinstance(wfu, dict):
                cleaned_wfu: dict[str, dict[str, Any]] = {}
                for k, v in wfu.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    if not isinstance(v, dict) or not v:
                        continue
                    q = v.get('question')
                    if not isinstance(q, str) or not q.strip():
                        q = v.get('text')
                    if not isinstance(q, str) or not q.strip():
                        continue
                    try:
                        asked_ts = float(v.get('asked_ts') or v.get('created_ts') or 0.0)
                    except Exception:
                        asked_ts = 0.0
                    if asked_ts <= 0:
                        continue
                    try:
                        ping_count = int(v.get('ping_count') or 0)
                    except Exception:
                        ping_count = 0
                    ping_count = max(0, min(3, ping_count))
                    try:
                        last_ping_ts = float(v.get('last_ping_ts') or 0.0)
                    except Exception:
                        last_ping_ts = 0.0
                    default = v.get('default')
                    if not isinstance(default, str):
                        default = ''
                    mode = v.get('mode')
                    mode_s = str(mode or '').strip().lower()
                    if mode_s not in {'read', 'write', 'danger'}:
                        mode_s = ''
                    cleaned: dict[str, Any] = {
                        'asked_ts': float(asked_ts),
                        'question': _clamp_text(q, 4000),
                        'ping_count': int(ping_count),
                        'last_ping_ts': float(last_ping_ts),
                    }
                    if default.strip():
                        cleaned['default'] = _clamp_text(default, 2000)
                    if mode_s:
                        cleaned['mode'] = mode_s
                    options_raw = v.get('options') or v.get('choices') or v.get('variants') or []
                    if isinstance(options_raw, list):
                        cleaned_opts: list[str] = []
                        for opt in options_raw[:5]:
                            if isinstance(opt, str) and opt.strip():
                                cleaned_opts.append(_clamp_text(opt.strip(), 200))
                        if cleaned_opts:
                            cleaned['options'] = cleaned_opts
                    for src_key in ('origin_message_id', 'origin_ack_message_id', 'origin_user_id'):
                        try:
                            n = int(v.get(src_key) or 0)
                        except Exception:
                            n = 0
                        if n > 0:
                            cleaned[src_key] = int(n)
                    cleaned_wfu[sk] = cleaned
                self.waiting_for_user_by_scope = cleaned_wfu

            lcts = data.get('live_chatter_last_sent_ts_by_scope') or {}
            if isinstance(lcts, dict):
                cleaned_lcts: dict[str, float] = {}
                for k, v in lcts.items():
                    sk = _normalize_scope_key(k)
                    if not sk:
                        continue
                    try:
                        ts = float(v or 0.0)
                    except Exception:
                        ts = 0.0
                    if ts > 0:
                        cleaned_lcts[sk] = float(ts)
                self.live_chatter_last_sent_ts_by_scope = cleaned_lcts

            hist = data.get('history')
            if isinstance(hist, list):
                cleaned_hist: list[dict[str, Any]] = []
                for item in hist:
                    if not isinstance(item, dict):
                        continue
                    # minimal validation
                    ts_raw = item.get('ts')
                    role = item.get('role')
                    kind = item.get('kind')
                    text = item.get('text')
                    if not isinstance(ts_raw, (int, float)):
                        continue
                    if not isinstance(role, str) or not isinstance(kind, str) or not isinstance(text, str):
                        continue
                    meta = item.get('meta')
                    if meta is not None and not isinstance(meta, dict):
                        meta = {'_': str(meta)}
                    cleaned_hist.append(
                        {'ts': float(ts_raw), 'role': role, 'kind': kind, 'text': text, 'meta': meta or {}}
                    )
                # keep as-is; pruning happens on append
                self.history = cleaned_hist

            metrics = data.get('metrics') or {}
            if isinstance(metrics, dict):
                cleaned_metrics: dict[str, int | float] = {}
                for k, v in metrics.items():
                    if not isinstance(k, str):
                        continue
                    key = k.strip()
                    if not key:
                        continue
                    if isinstance(v, bool):
                        # Avoid subtle bool/int mixing in counters.
                        cleaned_metrics[key] = int(v)
                    elif isinstance(v, int):
                        cleaned_metrics[key] = int(v)
                    elif isinstance(v, float):
                        cleaned_metrics[key] = float(v)
                self.metrics = cleaned_metrics

            def _load_bool_map(raw: object) -> dict[str, bool]:
                if not isinstance(raw, dict):
                    return {}
                out: dict[str, bool] = {}
                for k, v in raw.items():
                    try:
                        key = str(int(k))
                    except Exception:
                        continue
                    if isinstance(v, bool):
                        out[key] = bool(v)
                    elif isinstance(v, (int, float)):
                        out[key] = bool(int(v))
                    elif isinstance(v, str):
                        s = v.strip().lower()
                        if s in {'1', 'true', 'yes', 'on'}:
                            out[key] = True
                        elif s in {'0', 'false', 'no', 'off'}:
                            out[key] = False
                return out

            def _load_int_map(raw: object, *, min_value: int, max_value: int) -> dict[str, int]:
                if not isinstance(raw, dict):
                    return {}
                out: dict[str, int] = {}
                for k, v in raw.items():
                    try:
                        key = str(int(k))
                    except Exception:
                        continue
                    try:
                        n = int(v)
                    except Exception:
                        continue
                    n = max(int(min_value), min(int(max_value), n))
                    out[key] = int(n)
                return out

            self.ux_prefer_edit_delivery_by_chat = _load_bool_map(data.get('ux_prefer_edit_delivery_by_chat'))
            self.ux_done_notice_enabled_by_chat = _load_bool_map(data.get('ux_done_notice_enabled_by_chat'))
            self.ux_done_notice_delete_seconds_by_chat = _load_int_map(
                data.get('ux_done_notice_delete_seconds_by_chat'),
                min_value=0,
                max_value=24 * 60 * 60,
            )
            self.ux_bot_initiatives_enabled_by_chat = _load_bool_map(data.get('ux_bot_initiatives_enabled_by_chat'))
            self.ux_live_chatter_enabled_by_chat = _load_bool_map(data.get('ux_live_chatter_enabled_by_chat'))
            self.ux_mcp_live_enabled_by_chat = _load_bool_map(data.get('ux_mcp_live_enabled_by_chat'))
            self.ux_user_in_loop_enabled_by_chat = _load_bool_map(data.get('ux_user_in_loop_enabled_by_chat'))

    def save(self) -> None:
        with self.lock:
            data: dict[str, Any] = {
                'tg_offset': self.tg_offset,
                'last_chat_id': self.last_chat_id,
                'last_user_id': self.last_user_id,
                'watch_chat_id': self.watch_chat_id,
                'reminders_chat_id': self.reminders_chat_id,
                'reminders_message_thread_id': self.reminders_message_thread_id,
                'last_user_msg_ts': self.last_user_msg_ts,
                'last_user_msg_ts_by_chat': self.last_user_msg_ts_by_chat,
                'snooze_until_ts': self.snooze_until_ts,
                'snooze_kind': self.snooze_kind,
                'sleep_until_by_scope': self.sleep_until_by_scope,
                'gentle_until_ts': self.gentle_until_ts,
                'gentle_reason': self.gentle_reason,
                'mute_events_ts': self.mute_events_ts,
                'last_ping_ts': self.last_ping_ts,
                'last_ping_stage': self.last_ping_stage,
                'touch_ts_at_ping': self.touch_ts_at_ping,
                'reminders_sent': self.reminders_sent,
                'reminders_pending': self.reminders_pending,
                'mm_sent_up_to_ts_by_channel': self.mm_sent_up_to_ts_by_channel,
                'mm_pending_up_to_ts_by_channel': self.mm_pending_up_to_ts_by_channel,
                'mm_mfa_token': self.mm_mfa_token,
                'mm_mfa_token_set_ts': self.mm_mfa_token_set_ts,
                'mm_mfa_prompt_ts': self.mm_mfa_prompt_ts,
                'mm_mfa_required_ts': self.mm_mfa_required_ts,
                'mm_session_token': self.mm_session_token,
                'mm_session_token_set_ts': self.mm_session_token_set_ts,
                'last_codex_ts': self.last_codex_ts,
                'last_codex_automation': self.last_codex_automation,
                'last_codex_profile': self.last_codex_profile,
                'last_codex_mode': self.last_codex_mode,
                'last_codex_model': self.last_codex_model,
                'last_codex_reasoning': self.last_codex_reasoning,
                'last_codex_ts_by_chat': self.last_codex_ts_by_chat,
                'last_codex_automation_by_chat': self.last_codex_automation_by_chat,
                'last_codex_profile_by_chat': self.last_codex_profile_by_chat,
                'last_codex_mode_by_chat': self.last_codex_mode_by_chat,
                'last_codex_model_by_chat': self.last_codex_model_by_chat,
                'last_codex_reasoning_by_chat': self.last_codex_reasoning_by_chat,
                'last_codex_ts_by_scope': self.last_codex_ts_by_scope,
                'last_codex_automation_by_scope': self.last_codex_automation_by_scope,
                'last_codex_profile_by_scope': self.last_codex_profile_by_scope,
                'last_codex_mode_by_scope': self.last_codex_mode_by_scope,
                'last_codex_model_by_scope': self.last_codex_model_by_scope,
                'last_codex_reasoning_by_scope': self.last_codex_reasoning_by_scope,
                'restart_pending': self.restart_pending,
                'restart_requested_ts': self.restart_requested_ts,
                'restart_shutting_down_ts': self.restart_shutting_down_ts,
                'restart_requested_chat_id': self.restart_requested_chat_id,
                'restart_requested_message_thread_id': self.restart_requested_message_thread_id,
                'restart_requested_user_id': self.restart_requested_user_id,
                'restart_requested_message_id': self.restart_requested_message_id,
                'restart_requested_ack_message_id': self.restart_requested_ack_message_id,
                'pending_attachments_by_chat': self.pending_attachments_by_chat,
                'pending_reply_to_by_chat': self.pending_reply_to_by_chat,
                'pending_attachments_by_scope': self.pending_attachments_by_scope,
                'pending_reply_to_by_scope': self.pending_reply_to_by_scope,
                'tg_outbox': self.tg_outbox,
                'tg_offline_since_by_chat': self.tg_offline_since_by_chat,
                'tg_offline_notice_sent_ts_by_chat': self.tg_offline_notice_sent_ts_by_chat,
                'tg_message_id_by_coalesce_key_by_chat': self.tg_message_id_by_coalesce_key_by_chat,
                'attachments_index_by_chat': self.attachments_index_by_chat,
                'pending_codex_jobs_by_chat': self.pending_codex_jobs_by_chat,
                'pending_codex_jobs_by_scope': self.pending_codex_jobs_by_scope,
                'pending_dangerous_confirmations_by_chat': self.pending_dangerous_confirmations_by_chat,
                'pending_dangerous_confirmations_by_scope': self.pending_dangerous_confirmations_by_scope,
                'pending_voice_routes_by_chat': self.pending_voice_routes_by_chat,
                'pending_voice_routes_by_scope': self.pending_voice_routes_by_scope,
                'pending_followups_by_scope': self.pending_followups_by_scope,
                'collect_active': self.collect_active,
                'collect_pending': self.collect_pending,
                'collect_deferred': self.collect_deferred,
                'collect_packet_decisions_by_scope': self.collect_packet_decisions_by_scope,
                'waiting_for_user_by_scope': self.waiting_for_user_by_scope,
                'live_chatter_last_sent_ts_by_scope': self.live_chatter_last_sent_ts_by_scope,
                'ux_prefer_edit_delivery_by_chat': self.ux_prefer_edit_delivery_by_chat,
                'ux_done_notice_enabled_by_chat': self.ux_done_notice_enabled_by_chat,
                'ux_done_notice_delete_seconds_by_chat': self.ux_done_notice_delete_seconds_by_chat,
                'ux_bot_initiatives_enabled_by_chat': self.ux_bot_initiatives_enabled_by_chat,
                'ux_live_chatter_enabled_by_chat': self.ux_live_chatter_enabled_by_chat,
                'ux_mcp_live_enabled_by_chat': self.ux_mcp_live_enabled_by_chat,
                'ux_user_in_loop_enabled_by_chat': self.ux_user_in_loop_enabled_by_chat,
                'history': self.history,
                'metrics': self.metrics,
            }
            _atomic_write(self.path, json.dumps(data, ensure_ascii=False, indent=2))

    def metrics_snapshot(self) -> dict[str, int | float]:
        with self.lock:
            return dict(self.metrics)

    def metric_inc(self, name: str, *, delta: int = 1) -> None:
        key = str(name or '').strip()
        if not key:
            return
        d = int(delta)
        if d == 0:
            return
        with self.lock:
            cur = self.metrics.get(key) or 0
            try:
                cur_i = int(cur)
            except Exception:
                cur_i = 0
            self.metrics[key] = int(cur_i + d)

    def metric_set(self, name: str, value: int | float) -> None:
        key = str(name or '').strip()
        if not key:
            return
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return
        with self.lock:
            self.metrics[key] = int(value) if isinstance(value, int) else float(value)

    def metric_observe_ms(self, name: str, ms: float) -> None:
        base = str(name or '').strip()
        if not base:
            return
        try:
            ms_f = float(ms)
        except Exception:
            return
        if ms_f < 0:
            ms_f = 0.0

        n_key = f'{base}.n'
        sum_key = f'{base}.sum_ms'
        max_key = f'{base}.max_ms'
        last_key = f'{base}.last_ms'

        with self.lock:
            n0 = self.metrics.get(n_key) or 0
            try:
                n = int(n0)
            except Exception:
                n = 0
            self.metrics[n_key] = int(n + 1)

            s0 = self.metrics.get(sum_key) or 0.0
            try:
                s = float(s0)
            except Exception:
                s = 0.0
            self.metrics[sum_key] = float(s + ms_f)

            m0 = self.metrics.get(max_key) or 0.0
            try:
                m = float(m0)
            except Exception:
                m = 0.0
            if ms_f > m:
                self.metrics[max_key] = float(ms_f)

            self.metrics[last_key] = float(ms_f)

    def add_pending_attachments(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        attachments: list[dict[str, Any]],
        max_per_scope: int = 12,
    ) -> int:
        if not attachments:
            return 0
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        now = _now_ts()
        cleaned: list[dict[str, Any]] = []
        for a in attachments:
            if not isinstance(a, dict):
                continue
            path = a.get('path')
            if not isinstance(path, str) or not path.strip():
                continue
            name = a.get('name')
            if not isinstance(name, str) or not name.strip():
                name = Path(path).name
            kind = a.get('kind')
            kind_s = str(kind or '').strip()[:32]
            try:
                size_bytes = int(a.get('size_bytes') or 0)
            except Exception:
                size_bytes = 0
            cleaned.append(
                {
                    'path': path.strip(),
                    'name': str(name).strip(),
                    'kind': kind_s,
                    'size_bytes': size_bytes,
                    'ts': float(a.get('ts') or now),
                }
            )
        if not cleaned:
            return 0
        with self.lock:
            existing = list(self.pending_attachments_by_scope.get(key) or [])
            existing.extend(cleaned)
            if max_per_scope > 0 and len(existing) > max_per_scope:
                existing = existing[-max_per_scope:]
            self.pending_attachments_by_scope[key] = existing
            n = len(existing)
        self.save()
        return n

    def take_pending_attachments(self, *, chat_id: int, message_thread_id: int = 0) -> list[dict[str, Any]]:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            out = list(self.pending_attachments_by_scope.get(key) or [])
            if key in self.pending_attachments_by_scope:
                self.pending_attachments_by_scope.pop(key, None)
        if out:
            self.save()
        return out

    def pending_attachments_count(self, *, chat_id: int, message_thread_id: int = 0) -> int:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            return len(self.pending_attachments_by_scope.get(key) or [])

    def set_pending_reply_to(
        self, *, chat_id: int, message_thread_id: int = 0, reply_to: dict[str, Any] | None
    ) -> None:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            if not reply_to:
                self.pending_reply_to_by_scope.pop(key, None)
            else:
                self.pending_reply_to_by_scope[key] = dict(reply_to)
        self.save()

    def take_pending_reply_to(self, *, chat_id: int, message_thread_id: int = 0) -> dict[str, Any] | None:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            out = self.pending_reply_to_by_scope.pop(key, None)
        if out is not None:
            self.save()
        return dict(out) if isinstance(out, dict) else None

    def tg_mark_offline(self, *, chat_id: int, ts: float | None = None) -> float:
        key = str(int(chat_id))
        now = float(ts or _now_ts())
        changed = False
        with self.lock:
            cur = float(self.tg_offline_since_by_chat.get(key) or 0.0)
            if cur <= 0:
                self.tg_offline_since_by_chat[key] = float(now)
                # New offline epoch: allow a fresh restore notice later.
                self.tg_offline_notice_sent_ts_by_chat.pop(key, None)
                cur = float(now)
                changed = True
        if changed:
            self.save()
        return float(cur)

    def tg_clear_offline(self, *, chat_id: int) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            if key in self.tg_offline_since_by_chat:
                self.tg_offline_since_by_chat.pop(key, None)
                changed = True
            if key in self.tg_offline_notice_sent_ts_by_chat:
                self.tg_offline_notice_sent_ts_by_chat.pop(key, None)
                changed = True
        if changed:
            self.save()

    def tg_offline_since(self, *, chat_id: int) -> float:
        key = str(int(chat_id))
        with self.lock:
            return float(self.tg_offline_since_by_chat.get(key) or 0.0)

    def tg_offline_notice_sent_ts(self, *, chat_id: int) -> float:
        key = str(int(chat_id))
        with self.lock:
            return float(self.tg_offline_notice_sent_ts_by_chat.get(key) or 0.0)

    def tg_mark_offline_notice_sent(self, *, chat_id: int, ts: float | None = None) -> None:
        key = str(int(chat_id))
        now = float(ts or _now_ts())
        changed = False
        with self.lock:
            cur = float(self.tg_offline_notice_sent_ts_by_chat.get(key) or 0.0)
            if cur <= 0:
                self.tg_offline_notice_sent_ts_by_chat[key] = float(now)
                changed = True
        if changed:
            self.save()

    def tg_offline_chat_ids_snapshot(self) -> list[int]:
        with self.lock:
            out: list[int] = []
            for k, v in self.tg_offline_since_by_chat.items():
                try:
                    cid = int(k)
                except Exception:
                    continue
                try:
                    ts = float(v or 0.0)
                except Exception:
                    ts = 0.0
                if ts > 0 and cid != 0:
                    out.append(cid)
            return out

    def tg_outbox_snapshot(self) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(x) for x in self.tg_outbox if isinstance(x, dict)]

    def tg_outbox_replace(self, *, items: list[dict[str, Any]]) -> None:
        cleaned: list[dict[str, Any]] = []
        for it in items:
            if isinstance(it, dict):
                cleaned.append(dict(it))
        with self.lock:
            self.tg_outbox = cleaned
        self.save()

    def tg_outbox_enqueue(self, *, item: dict[str, Any], max_items: int = 500) -> None:
        if not isinstance(item, dict):
            return
        try:
            chat_id = int(item.get('chat_id') or 0)
        except Exception:
            chat_id = 0
        if chat_id == 0:
            return
        item_id = str(item.get('id') or '').strip()
        op = str(item.get('op') or '').strip()
        if not item_id or not op:
            return
        params = item.get('params')
        if not isinstance(params, dict):
            params = {}
            item['params'] = params

        coalesce_key = item.get('coalesce_key')
        coalesce_s = str(coalesce_key or '').strip()
        key_chat = str(int(chat_id))
        with self.lock:
            if coalesce_s:
                self.tg_outbox = [
                    x
                    for x in self.tg_outbox
                    if not (
                        isinstance(x, dict)
                        and str(x.get('coalesce_key') or '').strip() == coalesce_s
                        and str(int(x.get('chat_id') or 0)) == key_chat
                    )
                ]
            self.tg_outbox.append(dict(item))
            if max_items > 0 and len(self.tg_outbox) > max_items:
                self.tg_outbox = self.tg_outbox[-max_items:]
        self.save()

    def tg_message_id_for_coalesce_key(self, *, chat_id: int, coalesce_key: str) -> int:
        try:
            cid = int(chat_id)
        except Exception:
            return 0
        if cid == 0:
            return 0
        ck = str(coalesce_key or '').strip()[:64]
        if not ck:
            return 0
        chat_key = str(int(cid))
        with self.lock:
            mapping = self.tg_message_id_by_coalesce_key_by_chat.get(chat_key)
            if not isinstance(mapping, dict):
                return 0
            try:
                return int(mapping.get(ck) or 0)
            except Exception:
                return 0

    def tg_coalesce_key_for_message_id(self, *, chat_id: int, message_id: int) -> str:
        """Best-effort reverse lookup: message_id -> coalesce_key.

        Used to detect stale/mismatched ack_message_id values (e.g. when a new task accidentally reuses
        an old ack message_id and would overwrite an unrelated message via edit).
        """
        try:
            cid = int(chat_id)
        except Exception:
            return ''
        if cid == 0:
            return ''
        try:
            mid = int(message_id)
        except Exception:
            return ''
        if mid <= 0:
            return ''
        chat_key = str(int(cid))
        with self.lock:
            mapping = self.tg_message_id_by_coalesce_key_by_chat.get(chat_key)
            if not isinstance(mapping, dict) or not mapping:
                return ''
            for ck, mapped_mid in mapping.items():
                try:
                    if int(mapped_mid or 0) == mid:
                        return str(ck or '').strip()[:64]
                except Exception:
                    continue
        return ''

    def tg_bind_message_id_for_coalesce_key(
        self, *, chat_id: int, coalesce_key: str, message_id: int, max_keys_per_chat: int = 200
    ) -> None:
        try:
            cid = int(chat_id)
        except Exception:
            return
        if cid == 0:
            return
        ck = str(coalesce_key or '').strip()[:64]
        if not ck:
            return
        try:
            mid = int(message_id)
        except Exception:
            mid = 0
        if mid <= 0:
            return
        chat_key = str(int(cid))
        max_keys = int(max_keys_per_chat or 0)

        with self.lock:
            mapping = self.tg_message_id_by_coalesce_key_by_chat.get(chat_key)
            if not isinstance(mapping, dict):
                mapping = {}
                self.tg_message_id_by_coalesce_key_by_chat[chat_key] = mapping
            # Refresh insertion order.
            try:
                mapping.pop(ck, None)
            except Exception:
                pass
            mapping[ck] = int(mid)
            if max_keys > 0:
                while len(mapping) > max_keys:
                    try:
                        oldest = next(iter(mapping))
                    except StopIteration:
                        break
                    mapping.pop(oldest, None)
        self.save()

    def set_pending_codex_job(self, *, chat_id: int, message_thread_id: int = 0, job: dict[str, Any] | None) -> None:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            if not job:
                self.pending_codex_jobs_by_scope.pop(key, None)
            else:
                self.pending_codex_jobs_by_scope[key] = dict(job)
        self.save()

    def pending_codex_job(self, *, chat_id: int, message_thread_id: int = 0) -> dict[str, Any] | None:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            job = self.pending_codex_jobs_by_scope.get(key)
        return dict(job) if isinstance(job, dict) else None

    def pending_codex_jobs_snapshot(self) -> dict[str, dict[str, Any]]:
        with self.lock:
            out: dict[str, dict[str, Any]] = {}
            for k, v in self.pending_codex_jobs_by_scope.items():
                if isinstance(k, str) and isinstance(v, dict):
                    out[k] = dict(v)
            return out

    def pending_dangerous_confirmation(
        self, *, chat_id: int, message_thread_id: int = 0, request_id: str
    ) -> dict[str, Any] | None:
        scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        rid = (request_id or '').strip()[:32]
        if not rid:
            return None
        now = _now_ts()
        with self.lock:
            job = (self.pending_dangerous_confirmations_by_scope.get(scope_key) or {}).get(rid)
        if not isinstance(job, dict) or not job:
            return None
        try:
            expires_ts = float(job.get('expires_ts') or 0.0)
        except Exception:
            expires_ts = 0.0
        if expires_ts > 0 and expires_ts <= now:
            return None
        return dict(job)

    def has_active_dangerous_confirmations(self) -> bool:
        """Return True if there is at least one non-expired dangerous confirmation pending.

        Also prunes expired/invalid entries (best-effort) so callers can use this as a queue barrier signal.
        """
        now = _now_ts()
        changed = False
        active = False
        cleaned_all: dict[str, dict[str, dict[str, Any]]] = {}

        with self.lock:
            for scope_key, per_scope in (self.pending_dangerous_confirmations_by_scope or {}).items():
                if not isinstance(scope_key, str) or not scope_key.strip():
                    changed = True
                    continue
                if not isinstance(per_scope, dict) or not per_scope:
                    changed = True
                    continue
                kept: dict[str, dict[str, Any]] = {}
                for rid, job in per_scope.items():
                    if not isinstance(rid, str) or not rid.strip():
                        changed = True
                        continue
                    if not isinstance(job, dict) or not job:
                        changed = True
                        continue
                    try:
                        exp = float(job.get('expires_ts') or 0.0)
                    except Exception:
                        exp = 0.0
                    if exp > 0 and exp <= now:
                        changed = True
                        continue
                    kept[rid.strip()[:32]] = dict(job)

                if kept:
                    cleaned_all[scope_key.strip()] = kept
                    active = True
                else:
                    changed = True

            if changed:
                self.pending_dangerous_confirmations_by_scope = cleaned_all

        if changed:
            self.save()
        return active

    def set_pending_dangerous_confirmation(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        request_id: str,
        job: dict[str, Any] | None,
        max_per_chat: int = 20,
    ) -> None:
        scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        rid = (request_id or '').strip()[:32]
        if not rid:
            return

        now = _now_ts()
        with self.lock:
            per_chat = dict(self.pending_dangerous_confirmations_by_scope.get(scope_key) or {})
            if not job:
                per_chat.pop(rid, None)
            else:
                per_chat[rid] = dict(job)

            # Prune expired and keep last N by created_ts (best-effort).
            pruned: dict[str, dict[str, Any]] = {}
            items: list[tuple[float, str, dict[str, Any]]] = []
            for k, v in per_chat.items():
                if not isinstance(k, str) or not k.strip():
                    continue
                if not isinstance(v, dict) or not v:
                    continue
                try:
                    exp = float(v.get('expires_ts') or 0.0)
                except Exception:
                    exp = 0.0
                if exp > 0 and exp <= now:
                    continue
                try:
                    created = float(v.get('created_ts') or 0.0)
                except Exception:
                    created = 0.0
                items.append((created, k.strip()[:32], dict(v)))
            items.sort(key=lambda t: t[0], reverse=True)
            for _, k, v in items[: max(0, int(max_per_chat or 0)) or 20]:
                pruned[k] = v

            if pruned:
                self.pending_dangerous_confirmations_by_scope[scope_key] = pruned
            else:
                self.pending_dangerous_confirmations_by_scope.pop(scope_key, None)
        self.save()

    def pop_pending_dangerous_confirmation(
        self, *, chat_id: int, message_thread_id: int = 0, request_id: str
    ) -> dict[str, Any] | None:
        scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        rid = (request_id or '').strip()[:32]
        if not rid:
            return None
        now = _now_ts()
        with self.lock:
            per_chat = dict(self.pending_dangerous_confirmations_by_scope.get(scope_key) or {})
            job = per_chat.pop(rid, None)
            # Always prune expired on pop.
            kept: dict[str, dict[str, Any]] = {}
            for k, v in per_chat.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    continue
                try:
                    exp = float(v.get('expires_ts') or 0.0)
                except Exception:
                    exp = 0.0
                if exp > 0 and exp <= now:
                    continue
                kept[k.strip()[:32]] = dict(v)
            if kept:
                self.pending_dangerous_confirmations_by_scope[scope_key] = kept
            else:
                self.pending_dangerous_confirmations_by_scope.pop(scope_key, None)
        self.save()
        return dict(job) if isinstance(job, dict) else None

    def init_pending_voice_route(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        voice_message_id: int,
        ttl_seconds: int = 24 * 60 * 60,
        max_per_chat: int = 50,
    ) -> None:
        """Create a pending voice-route selection window for a voice message (best-effort)."""
        try:
            scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
            mid = int(voice_message_id)
        except Exception:
            return
        if mid <= 0:
            return
        ttl = max(60, min(int(ttl_seconds or 0), 7 * 24 * 60 * 60))
        now = _now_ts()
        entry_key = str(int(mid))

        with self.lock:
            per_chat = dict(self.pending_voice_routes_by_scope.get(scope_key) or {})
            entry = dict(per_chat.get(entry_key) or {})
            if not entry:
                entry = {'created_ts': float(now)}
            exp = float(entry.get('expires_ts') or 0.0)
            entry['expires_ts'] = float(max(exp, float(now + ttl)) if exp > 0 else float(now + ttl))
            per_chat[entry_key] = entry

            # Prune expired and keep last N by created_ts.
            kept: dict[str, dict[str, Any]] = {}
            items: list[tuple[float, str, dict[str, Any]]] = []
            for k, v in per_chat.items():
                if not isinstance(k, str) or not k.strip() or not isinstance(v, dict):
                    continue
                try:
                    exp0 = float(v.get('expires_ts') or 0.0)
                except Exception:
                    exp0 = 0.0
                if exp0 > 0 and exp0 <= now:
                    continue
                try:
                    created0 = float(v.get('created_ts') or 0.0)
                except Exception:
                    created0 = 0.0
                items.append((created0, k.strip(), dict(v)))
            items.sort(key=lambda t: t[0], reverse=True)
            limit = max(1, int(max_per_chat or 0))
            for _, k, v in items[:limit]:
                kept[k] = v
            if kept:
                self.pending_voice_routes_by_scope[scope_key] = kept
            else:
                self.pending_voice_routes_by_scope.pop(scope_key, None)
        self.save()

    def set_voice_route_choice(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        voice_message_id: int,
        choice: str,
        ttl_seconds: int = 24 * 60 * 60,
        max_per_chat: int = 50,
    ) -> None:
        """Persist the user's routing choice for a voice message."""
        ch = str(choice or '').strip().lower()
        if ch not in {'read', 'write', 'danger', 'none'}:
            return
        try:
            scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
            mid = int(voice_message_id)
        except Exception:
            return
        if mid <= 0:
            return
        ttl = max(60, min(int(ttl_seconds or 0), 7 * 24 * 60 * 60))
        now = _now_ts()
        entry_key = str(int(mid))

        with self.lock:
            per_chat = dict(self.pending_voice_routes_by_scope.get(scope_key) or {})
            entry = dict(per_chat.get(entry_key) or {})
            if not entry:
                entry = {'created_ts': float(now)}
            entry['choice'] = ch
            entry['selected_ts'] = float(now)
            entry['expires_ts'] = float(now + ttl)
            per_chat[entry_key] = entry

            # Prune expired and keep last N by created_ts.
            kept: dict[str, dict[str, Any]] = {}
            items: list[tuple[float, str, dict[str, Any]]] = []
            for k, v in per_chat.items():
                if not isinstance(k, str) or not k.strip() or not isinstance(v, dict):
                    continue
                try:
                    exp0 = float(v.get('expires_ts') or 0.0)
                except Exception:
                    exp0 = 0.0
                if exp0 > 0 and exp0 <= now:
                    continue
                try:
                    created0 = float(v.get('created_ts') or 0.0)
                except Exception:
                    created0 = 0.0
                items.append((created0, k.strip(), dict(v)))
            items.sort(key=lambda t: t[0], reverse=True)
            limit = max(1, int(max_per_chat or 0))
            for _, k, v in items[:limit]:
                kept[k] = v
            if kept:
                self.pending_voice_routes_by_scope[scope_key] = kept
            else:
                self.pending_voice_routes_by_scope.pop(scope_key, None)
        self.save()

    def pending_voice_route(
        self, *, chat_id: int, message_thread_id: int = 0, voice_message_id: int
    ) -> dict[str, Any] | None:
        """Return pending voice-route entry (if not expired)."""
        try:
            scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
            mid = int(voice_message_id)
        except Exception:
            return None
        if mid <= 0:
            return None
        entry_key = str(int(mid))
        now = _now_ts()
        with self.lock:
            entry = (self.pending_voice_routes_by_scope.get(scope_key) or {}).get(entry_key)
        if not isinstance(entry, dict) or not entry:
            return None
        try:
            exp = float(entry.get('expires_ts') or 0.0)
        except Exception:
            exp = 0.0
        if exp > 0 and exp <= now:
            # Best-effort prune.
            self.pop_pending_voice_route(chat_id=chat_id, message_thread_id=message_thread_id, voice_message_id=mid)
            return None
        return dict(entry)

    def pending_voice_route_choice(
        self, *, chat_id: int, message_thread_id: int = 0, voice_message_id: int
    ) -> str | None:
        """Return the selected choice if present (read/write/danger/none)."""
        entry = self.pending_voice_route(
            chat_id=chat_id, message_thread_id=message_thread_id, voice_message_id=voice_message_id
        )
        if not entry:
            return None
        ch = entry.get('choice')
        if isinstance(ch, str) and ch.strip():
            out = ch.strip().lower()
            return out if out in {'read', 'write', 'danger', 'none'} else None
        return None

    def pop_pending_voice_route(
        self, *, chat_id: int, message_thread_id: int = 0, voice_message_id: int
    ) -> dict[str, Any] | None:
        """Remove pending voice-route entry and return it (best-effort)."""
        try:
            scope_key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
            mid = int(voice_message_id)
        except Exception:
            return None
        if mid <= 0:
            return None
        entry_key = str(int(mid))
        now = _now_ts()
        with self.lock:
            per_chat = dict(self.pending_voice_routes_by_scope.get(scope_key) or {})
            entry = per_chat.pop(entry_key, None)
            # Prune expired even when popping.
            kept: dict[str, dict[str, Any]] = {}
            for k, v in per_chat.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    continue
                try:
                    exp0 = float(v.get('expires_ts') or 0.0)
                except Exception:
                    exp0 = 0.0
                if exp0 > 0 and exp0 <= now:
                    continue
                kept[k.strip()] = dict(v)
            if kept:
                self.pending_voice_routes_by_scope[scope_key] = kept
            else:
                self.pending_voice_routes_by_scope.pop(scope_key, None)
        self.save()
        return dict(entry) if isinstance(entry, dict) else None

    def remember_message_attachments(
        self, *, chat_id: int, message_id: int, attachments: list[dict[str, Any]], max_entries_per_chat: int = 200
    ) -> None:
        if message_id <= 0 or not attachments:
            return
        key = str(int(chat_id))
        now = _now_ts()
        cleaned: list[dict[str, Any]] = []
        for a in attachments:
            if not isinstance(a, dict):
                continue
            path = a.get('path')
            if not isinstance(path, str) or not path.strip():
                continue
            name = a.get('name')
            if not isinstance(name, str) or not name.strip():
                name = Path(path).name
            kind = a.get('kind')
            kind_s = str(kind or '').strip()[:32]
            try:
                size_bytes = int(a.get('size_bytes') or 0)
            except Exception:
                size_bytes = 0
            cleaned.append(
                {
                    'path': path.strip(),
                    'name': str(name).strip(),
                    'kind': kind_s,
                    'size_bytes': size_bytes,
                }
            )
        if not cleaned:
            return
        with self.lock:
            items = list(self.attachments_index_by_chat.get(key) or [])
            items = [x for x in items if int(x.get('message_id') or 0) != int(message_id)]
            items.append({'message_id': int(message_id), 'attachments': cleaned, 'ts': float(now)})
            if max_entries_per_chat > 0 and len(items) > max_entries_per_chat:
                items = items[-max_entries_per_chat:]
            self.attachments_index_by_chat[key] = items
        self.save()

    def get_message_attachments(self, *, chat_id: int, message_id: int) -> list[dict[str, Any]]:
        if message_id <= 0:
            return []
        key = str(int(chat_id))
        with self.lock:
            items = list(self.attachments_index_by_chat.get(key) or [])
        for item in reversed(items):
            try:
                mid = int(item.get('message_id') or 0)
            except Exception:
                continue
            if mid != int(message_id):
                continue
            atts = item.get('attachments') or []
            if not isinstance(atts, list):
                return []
            out: list[dict[str, Any]] = []
            for a in atts:
                if isinstance(a, dict):
                    out.append(dict(a))
            return out
        return []

    def is_restart_pending(self) -> bool:
        with self.lock:
            return bool(self.restart_pending)

    def request_restart(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        user_id: int,
        message_id: int = 0,
        ack_message_id: int = 0,
    ) -> None:
        now = _now_ts()
        with self.lock:
            self.restart_pending = True
            self.restart_requested_ts = float(now)
            self.restart_shutting_down_ts = 0.0
            self.restart_requested_chat_id = int(chat_id)
            self.restart_requested_message_thread_id = int(message_thread_id or 0)
            self.restart_requested_user_id = int(user_id)
            self.restart_requested_message_id = int(message_id)
            self.restart_requested_ack_message_id = int(ack_message_id or 0)
        self.save()

    def clear_restart_pending(self, *, preserve_request: bool = False) -> None:
        with self.lock:
            self.restart_pending = False
            if not preserve_request:
                self.restart_requested_ts = 0.0
                self.restart_shutting_down_ts = 0.0
                self.restart_requested_chat_id = 0
                self.restart_requested_message_thread_id = 0
                self.restart_requested_user_id = 0
                self.restart_requested_message_id = 0
                self.restart_requested_ack_message_id = 0
        self.save()

    def restart_target(self) -> tuple[int, int, int, int]:
        with self.lock:
            return (
                int(self.restart_requested_chat_id),
                int(self.restart_requested_message_thread_id),
                int(self.restart_requested_message_id),
                int(self.restart_requested_ack_message_id),
            )

    def restart_requested_at(self) -> float:
        with self.lock:
            return float(self.restart_requested_ts)

    def restart_shutting_down_at(self) -> float:
        with self.lock:
            return float(self.restart_shutting_down_ts)

    def mark_restart_shutting_down(self, *, status_message_id: int = 0) -> None:
        now = _now_ts()
        with self.lock:
            self.restart_shutting_down_ts = float(now)
            if int(status_message_id) > 0:
                self.restart_requested_ack_message_id = int(status_message_id)
        self.save()

    def set_tg_offset(self, offset: int) -> None:
        with self.lock:
            self.tg_offset = int(offset)
        self.save()

    def mark_user_activity(self, *, chat_id: int, user_id: int, counts_for_watch: bool = True) -> None:
        """Record user activity.

        `counts_for_watch` controls whether this activity should affect the global watcher state
        (idle pings, reminders). For multi-chat setups we typically want only the owner chat to
        drive those signals.
        """
        now_ts = _now_ts()
        key = str(int(chat_id))
        with self.lock:
            self.last_chat_id = int(chat_id)
            self.last_user_id = int(user_id)
            self.last_user_msg_ts_by_chat[key] = float(now_ts)
            if counts_for_watch:
                if int(chat_id) > 0:
                    self.watch_chat_id = int(chat_id)
                self.last_user_msg_ts = float(now_ts)
                # activity clears pending ping escalation
                self.last_ping_stage = 0
                self.last_ping_ts = 0.0
                self.touch_ts_at_ping = 0.0
        self.save()

    def set_reminders_target(self, *, chat_id: int, message_thread_id: int = 0) -> None:
        with self.lock:
            self.reminders_chat_id = int(chat_id or 0)
            self.reminders_message_thread_id = int(message_thread_id or 0)
        self.save()

    def reminders_target(self) -> tuple[int, int]:
        with self.lock:
            return (int(self.reminders_chat_id or 0), int(self.reminders_message_thread_id or 0))

    def last_user_msg_ts_for_chat(self, *, chat_id: int) -> float:
        key = str(int(chat_id))
        with self.lock:
            return float(self.last_user_msg_ts_by_chat.get(key) or 0.0)

    def clear_ping_state(self) -> None:
        """Clear only ping/escalation state (does not mark user as active)."""
        with self.lock:
            self.last_ping_stage = 0
            self.last_ping_ts = 0.0
            self.touch_ts_at_ping = 0.0
        self.save()

    def set_snooze(self, seconds: int, *, kind: str = 'mute') -> None:
        with self.lock:
            self.snooze_until_ts = max(self.snooze_until_ts, _now_ts() + float(seconds))
            self.snooze_kind = (kind or 'mute').strip()[:32]
        self.save()

    def sleep_until(self, *, chat_id: int, message_thread_id: int = 0) -> float:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        now = _now_ts()
        changed = False
        ts = 0.0
        with self.lock:
            ts = float(self.sleep_until_by_scope.get(key) or 0.0)
            if ts <= 0:
                return 0.0
            if ts <= now:
                self.sleep_until_by_scope.pop(key, None)
                ts = 0.0
                changed = True
        if changed:
            self.save()
        return ts

    def set_sleep_until(self, *, chat_id: int, message_thread_id: int = 0, until_ts: float = 0.0) -> None:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        try:
            ts = float(until_ts)
        except Exception:
            ts = 0.0
        if ts < 0:
            ts = 0.0
        changed = False
        with self.lock:
            if ts <= 0:
                if key in self.sleep_until_by_scope:
                    self.sleep_until_by_scope.pop(key, None)
                    changed = True
            elif self.sleep_until_by_scope.get(key) != ts:
                self.sleep_until_by_scope[key] = ts
                changed = True
        if changed:
            self.save()

    def clear_sleep(self, *, chat_id: int, message_thread_id: int = 0) -> None:
        key = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        changed = False
        with self.lock:
            if key in self.sleep_until_by_scope:
                self.sleep_until_by_scope.pop(key, None)
                changed = True
        if changed:
            self.save()

    def is_sleeping(self, *, chat_id: int, message_thread_id: int = 0) -> bool:
        return self.sleep_until(chat_id=chat_id, message_thread_id=message_thread_id) > 0.0

    def clear_snooze(self) -> None:
        with self.lock:
            self.snooze_until_ts = 0.0
            self.snooze_kind = ''
        self.save()

    def is_snoozed(self) -> bool:
        with self.lock:
            return _now_ts() < self.snooze_until_ts

    def is_gentle_active(self) -> bool:
        with self.lock:
            return _now_ts() < self.gentle_until_ts

    def enable_gentle(self, *, seconds: int, reason: str = '', extend: bool = True) -> None:
        now = _now_ts()
        until = now + float(max(0, int(seconds)))
        with self.lock:
            if extend:
                self.gentle_until_ts = max(self.gentle_until_ts, until)
            else:
                self.gentle_until_ts = until
            if reason:
                self.gentle_reason = _clamp_text(reason, 160)
        self.save()

    def disable_gentle(self) -> None:
        with self.lock:
            self.gentle_until_ts = 0.0
            self.gentle_reason = ''
        self.save()

    # -----------------------------
    # UX settings (per chat)
    # -----------------------------
    def ux_prefer_edit_delivery(self, *, chat_id: int) -> bool:
        """Prefer delivering the final answer via edit_message_text when possible."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_prefer_edit_delivery_by_chat.get(key)
        return True if v is None else bool(v)

    def ux_set_prefer_edit_delivery(self, *, chat_id: int, value: bool) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            cur = self.ux_prefer_edit_delivery_by_chat.get(key)
            new = bool(value)
            if cur != new:
                self.ux_prefer_edit_delivery_by_chat[key] = new
                changed = True
        if changed:
            self.save()

    def ux_done_notice_enabled(self, *, chat_id: int) -> bool:
        """Whether to send a short '✅ Готово' notice when final answer was delivered via edit."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_done_notice_enabled_by_chat.get(key)
        return True if v is None else bool(v)

    def ux_set_done_notice_enabled(self, *, chat_id: int, value: bool) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            cur = self.ux_done_notice_enabled_by_chat.get(key)
            new = bool(value)
            if cur != new:
                self.ux_done_notice_enabled_by_chat[key] = new
                changed = True
        if changed:
            self.save()

    def ux_done_notice_delete_seconds(self, *, chat_id: int) -> int:
        """Auto-delete timeout for the '✅ Готово' notice (0 = keep)."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_done_notice_delete_seconds_by_chat.get(key)
        if v is None:
            return 300
        try:
            n = int(v)
        except Exception:
            return 300
        return max(0, min(24 * 60 * 60, n))

    def ux_set_done_notice_delete_seconds(self, *, chat_id: int, seconds: int) -> None:
        key = str(int(chat_id))
        try:
            n = int(seconds)
        except Exception:
            return
        n = max(0, min(24 * 60 * 60, n))
        changed = False
        with self.lock:
            cur = self.ux_done_notice_delete_seconds_by_chat.get(key)
            if cur != n:
                self.ux_done_notice_delete_seconds_by_chat[key] = int(n)
                changed = True
        if changed:
            self.save()

    def ux_bot_initiatives_enabled(self, *, chat_id: int) -> bool:
        """Whether to allow proactive bot initiatives (watcher pings, auto gentle, etc.)."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_bot_initiatives_enabled_by_chat.get(key)
        return True if v is None else bool(v)

    def ux_set_bot_initiatives_enabled(self, *, chat_id: int, value: bool) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            cur = self.ux_bot_initiatives_enabled_by_chat.get(key)
            new = bool(value)
            if cur != new:
                self.ux_bot_initiatives_enabled_by_chat[key] = new
                changed = True
        if changed:
            self.save()

    def ux_live_chatter_enabled(self, *, chat_id: int) -> bool:
        """Whether to allow proactive 'live chatter' messages during long-running tasks."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_live_chatter_enabled_by_chat.get(key)
        return False if v is None else bool(v)

    def ux_set_live_chatter_enabled(self, *, chat_id: int, value: bool) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            cur = self.ux_live_chatter_enabled_by_chat.get(key)
            new = bool(value)
            if cur != new:
                self.ux_live_chatter_enabled_by_chat[key] = new
                changed = True
        if changed:
            self.save()

    def ux_mcp_live_enabled(self, *, chat_id: int) -> bool:
        """Whether to allow MCP follow-ups tools (get/wait/ack) for this chat."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_mcp_live_enabled_by_chat.get(key)
        return True if v is None else bool(v)

    def ux_set_mcp_live_enabled(self, *, chat_id: int, value: bool) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            cur = self.ux_mcp_live_enabled_by_chat.get(key)
            new = bool(value)
            if cur != new:
                self.ux_mcp_live_enabled_by_chat[key] = new
                changed = True
        if changed:
            self.save()

    def ux_user_in_loop_enabled(self, *, chat_id: int) -> bool:
        """Whether to allow bot-side user-in-the-loop via `ask_user` control blocks."""
        key = str(int(chat_id))
        with self.lock:
            v = self.ux_user_in_loop_enabled_by_chat.get(key)
        return True if v is None else bool(v)

    def ux_set_user_in_loop_enabled(self, *, chat_id: int, value: bool) -> None:
        key = str(int(chat_id))
        changed = False
        with self.lock:
            cur = self.ux_user_in_loop_enabled_by_chat.get(key)
            new = bool(value)
            if cur != new:
                self.ux_user_in_loop_enabled_by_chat[key] = new
                changed = True
        if changed:
            self.save()

    def record_mute_event(self, *, window_seconds: int) -> int:
        """Record a mute request and return count of mute events within the given window."""
        now = _now_ts()
        with self.lock:
            self.mute_events_ts.append(now)
            if window_seconds > 0:
                cutoff = now - float(window_seconds)
                self.mute_events_ts = [t for t in self.mute_events_ts if t >= cutoff]
            return len(self.mute_events_ts)

    def last_codex_ts_for(self, chat_id: int, message_thread_id: int = 0) -> float:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            v = self.last_codex_ts_by_scope.get(sk)
            if v is None and int(message_thread_id or 0) == 0:
                v = self.last_codex_ts_by_chat.get(str(int(chat_id)))
            return float(v or 0.0)

    def last_codex_automation_for(self, chat_id: int, message_thread_id: int = 0) -> bool:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            v = self.last_codex_automation_by_scope.get(sk)
            if v is None and int(message_thread_id or 0) == 0:
                v = self.last_codex_automation_by_chat.get(str(int(chat_id)))
            return bool(v or False)

    def last_codex_profile_for(self, chat_id: int, message_thread_id: int = 0) -> str:
        key = str(int(chat_id))
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            name = str(self.last_codex_profile_by_scope.get(sk) or '').strip()
            if not name and int(message_thread_id or 0) == 0:
                name = str(self.last_codex_profile_by_chat.get(key) or '').strip()
            if name:
                return name
            # Fallback: infer from automation flag.
            auto = bool(self.last_codex_automation_by_scope.get(sk) or False)
            if int(message_thread_id or 0) == 0:
                auto = bool(self.last_codex_automation_by_chat.get(key) or auto)
            return 'auto' if auto else 'chat'

    def last_codex_mode_for(self, chat_id: int, message_thread_id: int = 0) -> str:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        key = str(int(chat_id))
        mode = ''
        with self.lock:
            mode = _normalize_codex_mode(self.last_codex_mode_by_scope.get(sk))
            if not mode and int(message_thread_id or 0) == 0:
                mode = _normalize_codex_mode(self.last_codex_mode_by_chat.get(key))
            if not mode:
                mode = _normalize_codex_mode(self.last_codex_mode)
            if mode:
                return mode
        profile = str(self.last_codex_profile_for(chat_id=chat_id, message_thread_id=message_thread_id)).strip().casefold()
        return 'write' if profile in {'auto', 'danger'} else 'read'

    def _resolved_codex_model_for(self, *, chat_id: int, message_thread_id: int = 0) -> str:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        root_sk = _scope_key(chat_id=int(chat_id), message_thread_id=0)
        key = str(int(chat_id))
        model = str(self.last_codex_model_by_scope.get(sk) or '').strip()
        if not model and sk != root_sk:
            model = str(self.last_codex_model_by_scope.get(root_sk) or '').strip()
        if not model:
            model = str(self.last_codex_model_by_chat.get(key) or '').strip()
        if not model:
            model = str(self.last_codex_model or '').strip()
        return model

    def last_codex_model_for(self, chat_id: int, message_thread_id: int = 0) -> str:
        with self.lock:
            return self._resolved_codex_model_for(chat_id=chat_id, message_thread_id=message_thread_id)

    def last_codex_reasoning_for(self, chat_id: int, message_thread_id: int = 0) -> str:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        key = str(int(chat_id))
        with self.lock:
            reasoning = _normalize_codex_reasoning(self.last_codex_reasoning_by_scope.get(sk))
            if (not reasoning) and int(message_thread_id or 0) == 0:
                reasoning = _normalize_codex_reasoning(self.last_codex_reasoning_by_chat.get(key))
            if not reasoning:
                reasoning = _normalize_codex_reasoning(self.last_codex_reasoning)
            return reasoning or 'medium'

    def last_codex_profile_state_for(
        self, chat_id: int, message_thread_id: int = 0
    ) -> tuple[str | None, str | None, str | None]:
        """Return explicit per-scope codex profile state without synthetic fallbacks."""
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        key = str(int(chat_id))
        with self.lock:
            mode = _normalize_codex_mode(self.last_codex_mode_by_scope.get(sk))
            if (not mode) and int(message_thread_id or 0) == 0:
                mode = _normalize_codex_mode(self.last_codex_mode_by_chat.get(key))

            model = self._resolved_codex_model_for(chat_id=chat_id, message_thread_id=message_thread_id)

            reasoning = _normalize_codex_reasoning(self.last_codex_reasoning_by_scope.get(sk))
            if (not reasoning) and int(message_thread_id or 0) == 0:
                reasoning = _normalize_codex_reasoning(self.last_codex_reasoning_by_chat.get(key))

        return (mode or None, model or None, reasoning or None)

    def set_last_codex_run(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        automation: bool,
        profile_name: str | None = None,
        mode: str | None = None,
        model: str | None = None,
        reasoning: str | None = None,
    ) -> None:
        norm_mode = _normalize_codex_mode(mode)
        norm_reasoning = _normalize_codex_reasoning(reasoning)
        norm_model = str(model or '').strip()

        profile = (profile_name or '').strip()
        if not profile:
            if norm_mode == 'write':
                profile = 'auto'
            elif norm_mode == 'read':
                profile = 'chat'
            else:
                profile = 'auto' if automation else 'chat'

        now = _now_ts()
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        key = str(int(chat_id))
        with self.lock:
            # Legacy fields (kept for backward compatibility / debugging)
            self.last_codex_ts = float(now)
            self.last_codex_automation = bool(automation)
            self.last_codex_profile = (profile or self.last_codex_profile).strip() or ('auto' if automation else 'chat')

            if norm_mode:
                self.last_codex_mode = norm_mode
                self.last_codex_mode_by_scope[sk] = norm_mode
                if int(message_thread_id or 0) == 0:
                    self.last_codex_mode_by_chat[key] = norm_mode
            if model is not None:
                if norm_model:
                    self.last_codex_model_by_scope[sk] = norm_model
                    if int(message_thread_id or 0) == 0:
                        self.last_codex_model = norm_model
                        self.last_codex_model_by_chat[key] = norm_model
                else:
                    self.last_codex_model_by_scope.pop(sk, None)
                    if int(message_thread_id or 0) == 0:
                        self.last_codex_model = ''
                        self.last_codex_model_by_chat.pop(key, None)
            if reasoning is not None:
                self.last_codex_reasoning = norm_reasoning
                self.last_codex_reasoning_by_scope[sk] = norm_reasoning
                if int(message_thread_id or 0) == 0:
                    self.last_codex_reasoning_by_chat[key] = norm_reasoning

            # Per-scope fields
            self.last_codex_ts_by_scope[sk] = float(now)
            self.last_codex_automation_by_scope[sk] = bool(automation)
            self.last_codex_profile_by_scope[sk] = self.last_codex_profile
            # Per-chat fields (legacy): maintain only for thread_id=0.
            if int(message_thread_id or 0) == 0:
                self.last_codex_ts_by_chat[key] = float(now)
                self.last_codex_automation_by_chat[key] = bool(automation)
                self.last_codex_profile_by_chat[key] = self.last_codex_profile
        self.save()

    def set_last_codex_profile_state(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        mode: str | None = None,
        model: str | None = None,
        reasoning: str | None = None,
        profile_name: str | None = None,
    ) -> None:
        mode_norm = _normalize_codex_mode(mode)
        reasoning_norm = _normalize_codex_reasoning(reasoning)
        model_norm = str(model or '').strip()
        profile = (profile_name or '').strip()
        if not profile and mode_norm:
            profile = 'auto' if mode_norm == 'write' else 'chat'
        self.set_last_codex_run(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            automation=(mode_norm == 'write'),
            profile_name=(profile or None),
            mode=mode_norm,
            model=(model_norm if model is not None else None),
            reasoning=(reasoning_norm if reasoning is not None else None),
        )

    def append_history(
        self,
        *,
        role: str,
        kind: str,
        text: str,
        meta: dict[str, Any] | None = None,
        chat_id: int | None = None,
        message_thread_id: int | None = None,
        max_events: int = 80,
        max_chars: int = 500,
    ) -> None:
        now = _now_ts()
        meta_out = dict(meta or {})
        if chat_id is not None and 'chat_id' not in meta_out:
            meta_out['chat_id'] = int(chat_id)
        if (
            message_thread_id is not None
            and 'tg_message_thread_id' not in meta_out
            and 'message_thread_id' not in meta_out
        ):
            meta_out['tg_message_thread_id'] = int(message_thread_id or 0)
        item = {
            'ts': float(now),
            'role': _clamp_text(role, 24),
            'kind': _clamp_text(kind, 32),
            'text': _clamp_text(text, max_chars),
            'meta': meta_out,
        }
        with self.lock:
            self.history.append(item)
            if max_events > 0 and len(self.history) > max_events:
                self.history = self.history[-max_events:]
        self.save()

    def record_pending_followup(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        message_id: int,
        user_id: int,
        received_ts: float,
        text: str,
        attachments: list[dict[str, Any]] | None = None,
        reply_to: dict[str, Any] | None = None,
        max_items_per_scope: int = 200,
    ) -> None:
        mid = int(message_id or 0)
        uid = int(user_id or 0)
        if mid <= 0 or uid <= 0:
            return
        txt = (text or '').strip()
        if not txt:
            return
        item: dict[str, Any] = {
            'message_id': int(mid),
            'user_id': int(uid),
            'received_ts': float(received_ts or 0.0),
            'text': _clamp_text(txt, 12000),
        }
        if attachments:
            item['attachments'] = [dict(a) for a in attachments if isinstance(a, dict)][:20]
        if reply_to:
            item['reply_to'] = dict(reply_to)

        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        max_n = max(0, int(max_items_per_scope or 0))
        with self.lock:
            items = self.pending_followups_by_scope.get(sk)
            if not isinstance(items, list):
                items = []
            items.append(item)
            if max_n > 0 and len(items) > max_n:
                items = items[-max_n:]
            self.pending_followups_by_scope[sk] = items
        self.save()

    def collect_status(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
    ) -> str:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            if isinstance(self.collect_active.get(sk), dict):
                return 'active'
            if (self.collect_pending.get(sk) or []):
                return 'pending'
            if (self.collect_deferred.get(sk) or []):
                return 'deferred'
        return 'idle'

    def collect_append(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        item: dict[str, Any] | None,
        max_items_per_scope: int = 200,
    ) -> None:
        if not isinstance(item, dict):
            return
        payload = dict(item)
        if not payload:
            return
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        max_n = max(0, int(max_items_per_scope or 0))
        changed = False
        with self.lock:
            items = self.collect_pending.get(sk)
            if not isinstance(items, list):
                items = []
            items.append(payload)
            if max_n > 0 and len(items) > max_n:
                items = items[-max_n:]
            self.collect_pending[sk] = items
            changed = True
        if changed:
            self.save()

    def collect_start(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
    ) -> dict[str, Any] | None:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        started: dict[str, Any] | None = None
        changed = False
        with self.lock:
            active = self.collect_active.get(sk)
            if isinstance(active, dict) and active:
                return dict(active)

            if sk in self.collect_active:
                self.collect_active.pop(sk, None)
                changed = True

            pending = self.collect_pending.get(sk) or []
            if not isinstance(pending, list):
                pending = []
            while pending and not isinstance(pending[0], dict):
                pending = pending[1:]
            if not pending:
                if sk in self.collect_pending:
                    self.collect_pending.pop(sk, None)
                    changed = True
                started = None
            else:
                started = dict(pending.pop(0))
                if pending:
                    self.collect_pending[sk] = pending
                else:
                    self.collect_pending.pop(sk, None)
                if started:
                    self.collect_active[sk] = started
                else:
                    self.collect_active.pop(sk, None)
                changed = True

        if changed:
            self.save()
        return dict(started) if isinstance(started, dict) else None

    def collect_complete(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        max_deferred_per_scope: int = 200,
    ) -> dict[str, Any] | None:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        max_n = max(0, int(max_deferred_per_scope or 0))
        completed: dict[str, Any] | None = None
        changed = False
        with self.lock:
            active = self.collect_active.pop(sk, None)
            if not isinstance(active, dict) or not active:
                return None if sk not in self.collect_active else None
            deferred = self.collect_deferred.get(sk)
            if not isinstance(deferred, list):
                deferred = []
            deferred.append(dict(active))
            if max_n > 0 and len(deferred) > max_n:
                deferred = deferred[-max_n:]
            self.collect_deferred[sk] = deferred
            completed = dict(active)
            changed = True
        if changed:
            self.save()
        return completed

    def collect_cancel(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
    ) -> dict[str, Any] | None:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        canceled: dict[str, Any] | None = None
        changed = False
        with self.lock:
            active = self.collect_active.get(sk)
            if isinstance(active, dict):
                canceled = dict(active)
            if sk in self.collect_active:
                self.collect_active.pop(sk, None)
                changed = True
        if changed:
            self.save()
        return canceled

    def collect_packet_decision(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        packet_id: str,
    ) -> dict[str, Any] | None:
        """Return stored decision for a collect packet in this scope."""
        pid = str(packet_id or '').strip()
        if not pid:
            return None
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            decision = self.collect_packet_decisions_by_scope.get(sk, {}).get(pid)
        if isinstance(decision, dict) and decision:
            return dict(decision)
        return None

    def set_collect_packet_decision(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
        packet_id: str,
        status: str | None,
        reasons: list[str] | None = None,
        report: dict[str, Any] | None = None,
        created_ts: float | None = None,
    ) -> None:
        """Persist collect packet decision for retry/force handling."""
        pid = str(packet_id or '').strip()
        if not pid:
            return

        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        status_norm = str(status or '').strip().lower()

        changed = False
        with self.lock:
            per_scope = dict(self.collect_packet_decisions_by_scope.get(sk) or {})

            if status_norm not in {'pending', 'forced'}:
                if sk in self.collect_packet_decisions_by_scope:
                    existing = self.collect_packet_decisions_by_scope.get(sk) or {}
                    if isinstance(existing, dict) and pid in existing:
                        existing = dict(existing)
                        existing.pop(pid, None)
                        if existing:
                            self.collect_packet_decisions_by_scope[sk] = existing
                        else:
                            self.collect_packet_decisions_by_scope.pop(sk, None)
                        changed = True
            else:
                try:
                    created_ts_f = float(created_ts) if created_ts is not None else 0.0
                except (TypeError, ValueError):
                    created_ts_f = 0.0
                entry: dict[str, Any] = {
                    'status': status_norm,
                    'created_ts': created_ts_f,
                    'forced': status_norm == 'forced',
                    'reasons': [str(x).strip() for x in (reasons or []) if str(x).strip()],
                }
                if report is not None and isinstance(report, dict):
                    entry['report'] = dict(report)

                per_scope[pid] = entry
                self.collect_packet_decisions_by_scope[sk] = per_scope
                changed = True

        if changed:
            self.save()

    def status(self, *, chat_id: int, message_thread_id: int = 0) -> str:
        return self.collect_status(chat_id=chat_id, message_thread_id=message_thread_id)

    def append(self, *, chat_id: int, message_thread_id: int = 0, item: dict[str, Any] | None) -> None:
        self.collect_append(chat_id=chat_id, message_thread_id=message_thread_id, item=item)

    def start(self, *, chat_id: int, message_thread_id: int = 0) -> dict[str, Any] | None:
        return self.collect_start(chat_id=chat_id, message_thread_id=message_thread_id)

    def complete(
        self,
        *,
        chat_id: int,
        message_thread_id: int = 0,
    ) -> dict[str, Any] | None:
        return self.collect_complete(chat_id=chat_id, message_thread_id=message_thread_id)

    def cancel(self, *, chat_id: int, message_thread_id: int = 0) -> dict[str, Any] | None:
        return self.collect_cancel(chat_id=chat_id, message_thread_id=message_thread_id)

    def waiting_for_user(self, *, chat_id: int, message_thread_id: int = 0) -> dict[str, Any] | None:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            job = self.waiting_for_user_by_scope.get(sk)
        if not isinstance(job, dict) or not job:
            return None
        return dict(job)

    def waiting_for_user_snapshot(self) -> dict[str, dict[str, Any]]:
        with self.lock:
            out: dict[str, dict[str, Any]] = {}
            for k, v in (self.waiting_for_user_by_scope or {}).items():
                if not isinstance(k, str) or not k.strip():
                    continue
                if not isinstance(v, dict) or not v:
                    continue
                out[k.strip()] = dict(v)
            return out

    def is_waiting_for_user(self, *, chat_id: int, message_thread_id: int = 0) -> bool:
        return self.waiting_for_user(chat_id=chat_id, message_thread_id=message_thread_id) is not None

    def set_waiting_for_user(self, *, chat_id: int, message_thread_id: int = 0, job: dict[str, Any] | None) -> None:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        changed = False
        with self.lock:
            if not job:
                if sk in self.waiting_for_user_by_scope:
                    self.waiting_for_user_by_scope.pop(sk, None)
                    changed = True
            else:
                self.waiting_for_user_by_scope[sk] = dict(job)
                changed = True
        if changed:
            self.save()

    def bump_waiting_for_user_ping(self, *, chat_id: int, message_thread_id: int = 0, now_ts: float) -> int:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        changed = False
        ping_count = 0
        with self.lock:
            job = self.waiting_for_user_by_scope.get(sk)
            if isinstance(job, dict) and job:
                try:
                    ping_count = int(job.get('ping_count') or 0)
                except Exception:
                    ping_count = 0
                ping_count = max(0, min(3, ping_count + 1))
                job['ping_count'] = int(ping_count)
                job['last_ping_ts'] = float(now_ts or 0.0)
                self.waiting_for_user_by_scope[sk] = dict(job)
                changed = True
        if changed:
            self.save()
        return int(ping_count)

    def live_chatter_last_sent_ts(self, *, chat_id: int, message_thread_id: int = 0) -> float:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        with self.lock:
            v = self.live_chatter_last_sent_ts_by_scope.get(sk)
        try:
            ts = float(v or 0.0)
        except Exception:
            ts = 0.0
        return float(ts) if ts > 0 else 0.0

    def set_live_chatter_last_sent_ts(self, *, chat_id: int, message_thread_id: int = 0, ts: float) -> None:
        sk = _scope_key(chat_id=int(chat_id), message_thread_id=int(message_thread_id or 0))
        try:
            ts_f = float(ts or 0.0)
        except Exception:
            ts_f = 0.0
        changed = False
        with self.lock:
            if ts_f <= 0:
                if sk in self.live_chatter_last_sent_ts_by_scope:
                    self.live_chatter_last_sent_ts_by_scope.pop(sk, None)
                    changed = True
            else:
                cur = self.live_chatter_last_sent_ts_by_scope.get(sk)
                try:
                    cur_f = float(cur or 0.0)
                except Exception:
                    cur_f = 0.0
                if cur_f != ts_f:
                    self.live_chatter_last_sent_ts_by_scope[sk] = float(ts_f)
                    changed = True
        if changed:
            self.save()

    def recent_history_since(
        self,
        *,
        since_ts: float,
        limit: int = 30,
        chat_id: int | None = None,
        message_thread_id: int | None = None,
    ) -> list[dict[str, Any]]:
        with self.lock:
            if chat_id is None:
                scoped = list(self.history)
            else:
                cid = int(chat_id)
                scoped = []
                for x in self.history:
                    meta = x.get('meta') or {}
                    try:
                        meta_chat_id = meta.get('chat_id')
                    except Exception:
                        continue
                    if isinstance(meta_chat_id, bool):
                        continue
                    if isinstance(meta_chat_id, (int, float)):
                        if int(meta_chat_id) == cid:
                            scoped.append(x)
                        continue
                    if isinstance(meta_chat_id, str):
                        try:
                            if int(meta_chat_id.strip()) == cid:
                                scoped.append(x)
                        except Exception:
                            continue

            if message_thread_id is not None:
                want_tid = int(message_thread_id or 0)

                def _meta_tid(item: dict[str, Any]) -> int:
                    meta = item.get('meta') or {}
                    if not isinstance(meta, dict):
                        return 0
                    for k in ('tg_message_thread_id', 'message_thread_id', 'tg_thread_id'):
                        if k in meta:
                            try:
                                return int(meta.get(k) or 0)
                            except Exception:
                                return 0
                    return 0

                if want_tid <= 0:
                    # "No topic": keep only items without a topic id.
                    scoped = [x for x in scoped if _meta_tid(x) <= 0]
                else:
                    scoped = [x for x in scoped if _meta_tid(x) == want_tid]

            items = [x for x in scoped if float(x.get('ts') or 0.0) > float(since_ts)]
            if not items:
                # fallback to the last N items
                items = list(scoped)[-max(0, int(limit)) :]
            if limit > 0 and len(items) > limit:
                items = items[-limit:]
            return [dict(x) for x in items]

    def reminders_mark_sent(self, date_key: str, reminder_id: str) -> None:
        self.reminders_mark_sent_many(date_key, [reminder_id])

    def reminders_mark_sent_many(self, date_key: str, reminder_ids: list[str]) -> None:
        key = str(date_key or '').strip()
        if not key:
            return
        changed = False
        with self.lock:
            ids = self.reminders_sent.get(key) or []
            seen = set(ids)
            for rid in reminder_ids:
                r = str(rid or '').strip()
                if not r or r in seen:
                    continue
                ids.append(r)
                seen.add(r)
                changed = True
            if changed:
                self.reminders_sent[key] = ids
        if changed:
            self.save()

    def reminders_was_sent(self, date_key: str, reminder_id: str) -> bool:
        key = str(date_key or '').strip()
        rid = str(reminder_id or '').strip()
        if not key or not rid:
            return False
        with self.lock:
            return rid in (self.reminders_sent.get(key) or [])

    def reminders_mark_pending_many(self, date_key: str, reminder_ids: list[str]) -> None:
        key = str(date_key or '').strip()
        if not key:
            return
        changed = False
        with self.lock:
            ids = self.reminders_pending.get(key) or []
            seen = set(ids)
            for rid in reminder_ids:
                r = str(rid or '').strip()
                if not r or r in seen:
                    continue
                ids.append(r)
                seen.add(r)
                changed = True
            if changed:
                self.reminders_pending[key] = ids
        if changed:
            self.save()

    def reminders_clear_pending_many(self, date_key: str, reminder_ids: list[str]) -> None:
        key = str(date_key or '').strip()
        if not key:
            return
        want = {str(rid or '').strip() for rid in reminder_ids}
        want.discard('')
        if not want:
            return
        changed = False
        with self.lock:
            cur = self.reminders_pending.get(key) or []
            kept = [x for x in cur if str(x) not in want]
            if len(kept) != len(cur):
                changed = True
                if kept:
                    self.reminders_pending[key] = kept
                else:
                    self.reminders_pending.pop(key, None)
        if changed:
            self.save()

    def reminders_was_pending(self, date_key: str, reminder_id: str) -> bool:
        key = str(date_key or '').strip()
        rid = str(reminder_id or '').strip()
        if not key or not rid:
            return False
        with self.lock:
            return rid in (self.reminders_pending.get(key) or [])

    def reminders_prune_pending(self, date_key: str, *, keep_ids: set[str]) -> None:
        key = str(date_key or '').strip()
        if not key:
            return
        keep = {str(x or '').strip() for x in keep_ids}
        keep.discard('')
        changed = False
        with self.lock:
            cur = self.reminders_pending.get(key) or []
            if not cur:
                return
            kept = [str(x) for x in cur if str(x) in keep]
            if len(kept) != len(cur):
                changed = True
                if kept:
                    self.reminders_pending[key] = kept
                else:
                    self.reminders_pending.pop(key, None)
        if changed:
            self.save()

    # -----------------------------
    # Mattermost de-duplication
    # -----------------------------
    def mm_sent_up_to_ts(self, channel_id: str) -> int:
        cid = str(channel_id or '').strip()
        if not cid:
            return 0
        with self.lock:
            try:
                return int(self.mm_sent_up_to_ts_by_channel.get(cid) or 0)
            except Exception:
                return 0

    def mm_pending_up_to_ts(self, channel_id: str) -> int:
        cid = str(channel_id or '').strip()
        if not cid:
            return 0
        with self.lock:
            try:
                return int(self.mm_pending_up_to_ts_by_channel.get(cid) or 0)
            except Exception:
                return 0

    def mm_effective_cutoff_ts(self, channel_id: str) -> int:
        """Return a monotonic cutoff used to avoid duplicate forwarding.

        Uses max(sent, pending).
        """
        cid = str(channel_id or '').strip()
        if not cid:
            return 0
        with self.lock:
            try:
                s = int(self.mm_sent_up_to_ts_by_channel.get(cid) or 0)
            except Exception:
                s = 0
            try:
                p = int(self.mm_pending_up_to_ts_by_channel.get(cid) or 0)
            except Exception:
                p = 0
            return int(max(s, p))

    def mm_mark_pending(self, *, channel_id: str, up_to_ts: int) -> None:
        cid = str(channel_id or '').strip()
        if not cid:
            return
        try:
            ts = int(up_to_ts)
        except Exception:
            return
        if ts <= 0:
            return
        changed = False
        with self.lock:
            cur = int(self.mm_pending_up_to_ts_by_channel.get(cid) or 0)
            if ts != cur:
                self.mm_pending_up_to_ts_by_channel[cid] = int(ts)
                changed = True
        if changed:
            self.save()

    def mm_mark_sent(self, *, channel_id: str, up_to_ts: int) -> None:
        cid = str(channel_id or '').strip()
        if not cid:
            return
        try:
            ts = int(up_to_ts)
        except Exception:
            return
        if ts <= 0:
            return
        changed = False
        with self.lock:
            cur = int(self.mm_sent_up_to_ts_by_channel.get(cid) or 0)
            if ts > cur:
                self.mm_sent_up_to_ts_by_channel[cid] = int(ts)
                changed = True
            pending = int(self.mm_pending_up_to_ts_by_channel.get(cid) or 0)
            if pending > 0 and pending <= ts:
                self.mm_pending_up_to_ts_by_channel.pop(cid, None)
                changed = True
        if changed:
            self.save()

    def mm_prune_pending(self, *, keep: dict[str, int]) -> None:
        """Sync pending map with outbox state (prevents 'stuck pending' if outbox was dropped)."""
        if not isinstance(keep, dict):
            keep = {}
        cleaned: dict[str, int] = {}
        for k, v in keep.items():
            cid = str(k or '').strip()
            if not cid:
                continue
            try:
                ts = int(v)
            except Exception:
                continue
            if ts > 0:
                cleaned[cid] = int(ts)
        changed = False
        with self.lock:
            if self.mm_pending_up_to_ts_by_channel != cleaned:
                self.mm_pending_up_to_ts_by_channel = cleaned
                changed = True
        if changed:
            self.save()

    # -----------------------------
    # Mattermost MFA helpers
    # -----------------------------
    def mm_set_mfa_token(self, token: str) -> None:
        code = str(token or '').strip()
        if not code:
            return
        # Keep it short: typical OTP is 6 digits.
        code = code[:16]
        now = _now_ts()
        with self.lock:
            self.mm_mfa_token = code
            self.mm_mfa_token_set_ts = float(now)
            # Reset prompt throttle so the next failure can prompt again.
            self.mm_mfa_prompt_ts = 0.0
        self.save()

    def mm_has_mfa_token(self, *, max_age_seconds: int = 120) -> bool:
        now = _now_ts()
        max_age = float(max(1, int(max_age_seconds)))
        with self.lock:
            code = str(self.mm_mfa_token or '').strip()
            ts = float(self.mm_mfa_token_set_ts or 0.0)
            if not code or ts <= 0 or (now - ts) > max_age:
                return False
            return True

    def mm_consume_mfa_token(self, *, max_age_seconds: int = 120) -> str:
        """Return MFA token once and clear it (best-effort)."""
        now = _now_ts()
        max_age = float(max(1, int(max_age_seconds)))
        with self.lock:
            code = str(self.mm_mfa_token or '').strip()
            ts = float(self.mm_mfa_token_set_ts or 0.0)
            if not code or ts <= 0 or (now - ts) > max_age:
                self.mm_mfa_token = ''
                self.mm_mfa_token_set_ts = 0.0
                return ''
            # Consume once.
            self.mm_mfa_token = ''
            self.mm_mfa_token_set_ts = 0.0
            return code

    def mm_should_prompt_mfa(self, *, min_interval_seconds: int = 300) -> bool:
        now = _now_ts()
        min_interval = float(max(5, int(min_interval_seconds)))
        with self.lock:
            last = float(self.mm_mfa_prompt_ts or 0.0)
            if last > 0 and (now - last) < min_interval:
                return False
            self.mm_mfa_prompt_ts = float(now)
            return True

    def mm_mark_mfa_prompted(self) -> None:
        now = _now_ts()
        with self.lock:
            self.mm_mfa_prompt_ts = float(now)
        self.save()

    def mm_is_mfa_required(self) -> bool:
        with self.lock:
            return float(self.mm_mfa_required_ts or 0.0) > 0.0

    def mm_mark_mfa_required(self) -> None:
        now = _now_ts()
        changed = False
        with self.lock:
            if float(self.mm_mfa_required_ts or 0.0) <= 0.0:
                self.mm_mfa_required_ts = float(now)
                changed = True
        if changed:
            self.save()

    def mm_clear_mfa_required(self) -> None:
        changed = False
        with self.lock:
            if float(self.mm_mfa_required_ts or 0.0) != 0.0:
                self.mm_mfa_required_ts = 0.0
                changed = True
        if changed:
            self.save()

    def mm_get_session_token(self) -> str:
        with self.lock:
            return str(self.mm_session_token or '').strip()

    def mm_set_session_token(self, token: str) -> None:
        t = str(token or '').strip()
        if not t:
            return
        t = t[:4096]
        now = _now_ts()
        changed = False
        with self.lock:
            if t != str(self.mm_session_token or '') or float(self.mm_session_token_set_ts or 0.0) <= 0.0:
                self.mm_session_token = t
                self.mm_session_token_set_ts = float(now)
                changed = True
        if changed:
            self.save()

    def mm_clear_session_token(self) -> None:
        changed = False
        with self.lock:
            if str(self.mm_session_token or '').strip() or float(self.mm_session_token_set_ts or 0.0) != 0.0:
                self.mm_session_token = ''
                self.mm_session_token_set_ts = 0.0
                changed = True
        if changed:
            self.save()

    def mm_reset_state(self) -> None:
        """Drop all Mattermost-related state (safe to call any time)."""
        changed = False
        with self.lock:
            if self.mm_sent_up_to_ts_by_channel:
                self.mm_sent_up_to_ts_by_channel = {}
                changed = True
            if self.mm_pending_up_to_ts_by_channel:
                self.mm_pending_up_to_ts_by_channel = {}
                changed = True
            if str(self.mm_mfa_token or '').strip() or float(self.mm_mfa_token_set_ts or 0.0) != 0.0:
                self.mm_mfa_token = ''
                self.mm_mfa_token_set_ts = 0.0
                changed = True
            if float(self.mm_mfa_prompt_ts or 0.0) != 0.0:
                self.mm_mfa_prompt_ts = 0.0
                changed = True
            if float(self.mm_mfa_required_ts or 0.0) != 0.0:
                self.mm_mfa_required_ts = 0.0
                changed = True
            if str(self.mm_session_token or '').strip() or float(self.mm_session_token_set_ts or 0.0) != 0.0:
                self.mm_session_token = ''
                self.mm_session_token_set_ts = 0.0
                changed = True
        if changed:
            self.save()
