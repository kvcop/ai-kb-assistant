from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import re
import time
from collections.abc import Iterable
from pathlib import Path

from .keyboards import idle_stage, lunch_expired
from .state import BotState
from .telegram_api import TelegramDeliveryAPI


@dataclasses.dataclass(frozen=True)
class ReminderEntry:
    rule: str
    text: str


@dataclasses.dataclass(frozen=True)
class ReminderRule:
    kind: str
    label: str | None
    date: dt.date | None = None
    start: dt.date | None = None
    end: dt.date | None = None
    weekdays: tuple[int, ...] = ()
    targets: tuple[str, ...] = ()


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode('utf-8')).hexdigest()


def _parse_ymd_date(raw: str) -> dt.date | None:
    raw = (raw or '').strip()
    m = re.fullmatch(r'(\d{4})-(\d{2})-(\d{2})', raw)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _parse_weekdays(raw: str) -> tuple[int, ...] | None:
    """Parse weekday spec like 'tue,fri' into a tuple of dt.weekday() ints (Mon=0..Sun=6)."""
    raw = (raw or '').strip()
    if not raw:
        return None
    tokens = [t for t in re.split(r'[,\s]+', raw) if t]
    if not tokens:
        return None

    alias: dict[str, int] = {
        '0': 0,
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        'mon': 0,
        'monday': 0,
        'пн': 0,
        'tue': 1,
        'tues': 1,
        'tuesday': 1,
        'вт': 1,
        'wed': 2,
        'wednesday': 2,
        'ср': 2,
        'thu': 3,
        'thur': 3,
        'thurs': 3,
        'thursday': 3,
        'чт': 3,
        'fri': 4,
        'friday': 4,
        'пт': 4,
        'sat': 5,
        'saturday': 5,
        'сб': 5,
        'sun': 6,
        'sunday': 6,
        'вс': 6,
    }

    days: set[int] = set()
    for tok in tokens:
        key = tok.strip().casefold()
        if not key:
            continue
        d = alias.get(key)
        if d is None:
            return None
        if 0 <= d <= 6:
            days.add(int(d))
    if not days:
        return None
    return tuple(sorted(days))


def _try_parse_hhmm(label: str | None) -> int | None:
    if not label:
        return None
    m = re.fullmatch(r'(\d{1,2}):(\d{2})', label.strip())
    if not m:
        return None
    hour, minute = int(m.group(1)), int(m.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour * 60 + minute


def _load_reminders_db(path: Path) -> list[ReminderEntry]:
    try:
        if not path.exists():
            return []
        content = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return []

    entries: list[ReminderEntry] = []
    for raw_line in content.splitlines():
        line = raw_line.rstrip('\n')
        if not line.strip():
            continue
        if line.lstrip().startswith('#'):
            continue
        if '\t' not in line:
            continue
        rule, text = line.split('\t', 1)
        rule = rule.strip()
        text = text.strip()
        if not rule or not text:
            continue
        entries.append(ReminderEntry(rule=rule, text=text))
    return entries


def _parse_reminder_rule(rule: str) -> ReminderRule | None:
    raw = rule.strip()
    if not raw:
        return None
    parts = [p.strip() for p in raw.split('|') if p.strip()]
    if not parts:
        return None
    head = parts[0]
    opts = parts[1:]

    base_raw, sep, label_raw = head.partition('@')
    label = label_raw.strip() if sep and label_raw.strip() else None
    base = base_raw.strip().casefold()

    targets: list[str] = []
    for opt in opts:
        key_raw, eq, value_raw = opt.partition('=')
        if not eq:
            continue
        key = key_raw.strip().casefold()
        if key not in {'to', 'target', 'targets', 'chat', 'chats'}:
            continue
        for token in (value_raw or '').split(','):
            t = token.strip()
            if t:
                targets.append(t)

    if base == 'daily':
        return ReminderRule(kind='daily', label=label, targets=tuple(targets))
    if base.startswith('weekly:'):
        value = base_raw[len('weekly:') :].strip()
        weekdays = _parse_weekdays(value)
        if not weekdays:
            return None
        return ReminderRule(kind='weekly', label=label, weekdays=weekdays, targets=tuple(targets))
    if base.startswith('date:'):
        date = _parse_ymd_date(base_raw[len('date:') :].strip())
        if not date:
            return None
        return ReminderRule(kind='date', label=label, date=date, targets=tuple(targets))
    if base.startswith('range:'):
        value = base_raw[len('range:') :].strip()
        if '..' not in value:
            return None
        start_raw, end_raw = value.split('..', 1)
        start = _parse_ymd_date(start_raw.strip())
        end = _parse_ymd_date(end_raw.strip())
        if not start or not end or end < start:
            return None
        return ReminderRule(kind='range', label=label, start=start, end=end, targets=tuple(targets))
    return None


def _reminder_matches_date(rule: ReminderRule, target_date: dt.date) -> bool:
    if rule.kind == 'daily':
        return True
    if rule.kind == 'weekly':
        return bool(rule.weekdays) and target_date.weekday() in rule.weekdays
    if rule.kind == 'date':
        return rule.date == target_date
    if rule.kind == 'range':
        if rule.start is None or rule.end is None:
            return False
        return rule.start <= target_date <= rule.end
    return False


def _work_hours_to_minutes(spec: str) -> tuple[int, int] | None:
    raw = (spec or '').strip()
    m = re.fullmatch(r'(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})', raw)
    if not m:
        return None
    h1, m1, h2, m2 = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    if not (0 <= h1 <= 23 and 0 <= h2 <= 23 and 0 <= m1 <= 59 and 0 <= m2 <= 59):
        return None
    return (h1 * 60 + m1, h2 * 60 + m2)


def _within_work_hours(now: dt.datetime, spec: str) -> bool:
    rng = _work_hours_to_minutes(spec)
    if not rng:
        return True  # fail-open
    start_m, end_m = rng
    cur_m = now.hour * 60 + now.minute
    if start_m <= end_m:
        return start_m <= cur_m <= end_m
    # overnight window
    return cur_m >= start_m or cur_m <= end_m


def _work_window_start_ts(now: dt.datetime, spec: str) -> float | None:
    """Return the epoch seconds for the start of the current work-hours window.

    Used to avoid treating out-of-hours (e.g. sleep/overnight) time as "idle"
    for watcher escalation logic.
    """
    rng = _work_hours_to_minutes(spec)
    if not rng:
        return None
    start_m, end_m = rng
    start_h, start_min = divmod(int(start_m), 60)

    cur_m = now.hour * 60 + now.minute
    if start_m <= end_m:
        start_dt = now.replace(hour=start_h, minute=start_min, second=0, microsecond=0)
        return float(start_dt.timestamp())

    # Overnight window. Example: 22:00-06:00.
    # If we're in the late-evening part, start is today; otherwise it's yesterday.
    start_day = now.date() if cur_m >= start_m else (now.date() - dt.timedelta(days=1))
    start_dt = dt.datetime(start_day.year, start_day.month, start_day.day, start_h, start_min)
    return float(start_dt.timestamp())


def _iter_candidate_paths(repo_root: Path) -> Iterable[Path]:
    # These are heuristics: adjust anytime without touching KB format.
    yield repo_root / 'notes' / 'work' / 'daily-brief.md'
    yield repo_root / 'notes' / 'work' / 'end-of-day.md'
    yield repo_root / 'notes' / 'work' / 'open-questions.md'
    yield repo_root / 'notes' / 'work' / 'typos.md'
    yield repo_root / 'notes' / 'work' / 'reminders.md'

    daily_logs = repo_root / 'notes' / 'daily-logs'
    if daily_logs.exists():
        for p in sorted(daily_logs.glob('*.md'))[-20:]:
            yield p

    meetings = repo_root / 'notes' / 'meetings'
    if meetings.exists():
        for p in sorted(meetings.glob('*.md'))[-20:]:
            yield p

    technical = repo_root / 'notes' / 'technical'
    if technical.exists():
        for p in sorted(technical.glob('*.md'))[-20:]:
            yield p

    jira_snaps = repo_root / 'logs' / 'jira-snapshots'
    if jira_snaps.exists():
        for p in sorted(jira_snaps.glob('*'))[-50:]:
            yield p


def _latest_mtime(paths: Iterable[Path]) -> float:
    best = 0.0
    for p in paths:
        try:
            if p.exists():
                best = max(best, p.stat().st_mtime)
        except OSError:
            continue
    return best


class Watcher:
    def __init__(
        self,
        *,
        repo_root: Path,
        reminders_file: Path,
        owner_chat_id: int = 0,
        reminder_broadcast_chat_ids: list[int] | None = None,
        reminders_include_weekends: bool = False,
        work_hours: str,
        include_weekends: bool,
        idle_minutes: int,
        ack_minutes: int,
        idle_stage_minutes: list[int],
        grace_minutes: int,
        gentle_default_minutes: int,
        gentle_auto_idle_minutes: int,
        gentle_ping_cooldown_minutes: int,
        gentle_stage_cap: int,
        history_max_events: int = 120,
        history_entry_max_chars: int = 500,
    ) -> None:
        self.repo_root = repo_root
        self.reminders_file = reminders_file
        self.owner_chat_id = int(owner_chat_id or 0)
        self.reminder_broadcast_chat_ids = [int(x) for x in (reminder_broadcast_chat_ids or []) if int(x or 0) != 0]
        self.reminders_include_weekends = bool(reminders_include_weekends)
        self.work_hours = work_hours
        self.include_weekends = include_weekends
        self.idle_minutes = idle_minutes
        self.ack_minutes = ack_minutes
        self.idle_stage_minutes = list(idle_stage_minutes)
        self.grace_minutes = grace_minutes

        # gentle mode config
        self.gentle_default_minutes = int(gentle_default_minutes)
        self.gentle_auto_idle_minutes = int(gentle_auto_idle_minutes)
        self.gentle_ping_cooldown_minutes = int(gentle_ping_cooldown_minutes)
        self.gentle_stage_cap = int(gentle_stage_cap)

        # history config
        self.history_max_events = int(history_max_events)
        self.history_entry_max_chars = int(history_entry_max_chars)

    def _target_chat_id(self, state: BotState) -> int:
        if int(self.owner_chat_id or 0) != 0:
            return int(self.owner_chat_id)
        watch_chat_id = int(getattr(state, 'watch_chat_id', 0) or 0)
        if watch_chat_id > 0:
            return watch_chat_id
        return int(state.last_chat_id or 0)

    def _watch_delivery_target(self, *, state: BotState, default_chat_id: int) -> tuple[int, int | None]:
        reminders_chat_id, reminders_message_thread_id = state.reminders_target()
        if int(reminders_chat_id or 0) != 0:
            tid = int(reminders_message_thread_id or 0)
            return (int(reminders_chat_id), int(tid) if tid != 0 else None)
        return (int(default_chat_id), None)

    def _today_key(self, now: dt.datetime) -> str:
        return now.date().isoformat()

    def _kb_touch_ts(self) -> float:
        return _latest_mtime(_iter_candidate_paths(self.repo_root))

    def _format_age(self, seconds: float) -> str:
        if seconds < 60:
            return f'{int(seconds)}с'
        minutes = seconds / 60.0
        if minutes < 60:
            return f'{int(minutes)}м'
        hours = minutes / 60.0
        if hours < 48:
            return f'{hours:.1f}ч'
        days = hours / 24.0
        return f'{days:.1f}д'

    def _should_run_now(self, now: dt.datetime) -> bool:
        if not self.include_weekends and now.weekday() >= 5:
            return False
        return _within_work_hours(now, self.work_hours)

    def build_status_text(self, now: dt.datetime, state: BotState) -> str:
        kb_ts = self._kb_touch_ts()
        age_kb = self._format_age(max(0.0, time.time() - kb_ts)) if kb_ts else 'нет данных'
        age_tg = (
            self._format_age(max(0.0, time.time() - state.last_user_msg_ts)) if state.last_user_msg_ts else 'нет данных'
        )
        snooze = ''
        if state.is_snoozed():
            left = state.snooze_until_ts - time.time()
            kind = f' ({state.snooze_kind})' if getattr(state, 'snooze_kind', '') else ''
            snooze = f'\nПауза{kind}: ещё {self._format_age(max(0.0, left))}'
        return f'KB активность: {age_kb}\nTelegram активность: {age_tg}{snooze}'

    def tick(self, *, api: TelegramDeliveryAPI, state: BotState) -> None:
        now = dt.datetime.now()
        chat_id = self._target_chat_id(state)
        notify_chat_id, notify_thread_id = self._watch_delivery_target(state=state, default_chat_id=chat_id)
        initiatives_enabled = state.ux_bot_initiatives_enabled(chat_id=chat_id)

        # If a snooze has expired, clear it. For lunch-type snooze we also send a gentle check-in.
        now_ts = time.time()
        if state.snooze_until_ts > 0 and now_ts >= state.snooze_until_ts:
            kind = (state.snooze_kind or '').strip().lower()
            kb_ts = self._kb_touch_ts()
            touch_ts = max(kb_ts, state.last_user_msg_ts)
            idle_seconds = now_ts - touch_ts if touch_ts > 0 else now_ts

            state.clear_snooze()

            # Only ping for lunch, and only if there wasn't any recent activity.
            if (
                initiatives_enabled
                and kind == 'lunch'
                and chat_id > 0
                and self._should_run_now(now)
                and idle_seconds >= 5 * 60
            ):
                state.append_history(
                    role='bot',
                    kind='lunch_expired',
                    text='⏰ Обед закончился? Если ты тут — нажми ‘Вернулся’. Если нужно ещё — выбери паузу.',
                    meta={'kind': 'lunch_expired'},
                    chat_id=notify_chat_id,
                    message_thread_id=notify_thread_id,
                    max_events=self.history_max_events,
                    max_chars=self.history_entry_max_chars,
                )
                api.send_message(
                    chat_id=notify_chat_id,
                    message_thread_id=notify_thread_id,
                    text='⏰ Обед закончился? Если ты тут — нажми ‘Вернулся’. Если нужно ещё — выбери паузу.',
                    reply_markup=lunch_expired(gentle_active=state.is_gentle_active()),
                )

        # 1) Send due reminders.
        self._tick_reminders(now=now, api=api, state=state, default_chat_id=chat_id)

        # Everything below is about idle pings (private-only).
        if chat_id <= 0:
            return
        if not self._should_run_now(now):
            return

        # User may disable proactive bot initiatives while keeping reminders enabled.
        if not initiatives_enabled:
            return

        # Snooze: skip idle pings.
        if state.is_snoozed():
            return

        # 2) Inactivity pings (no tg msg AND no kb updates)
        self._tick_idle(now=now, api=api, state=state, chat_id=chat_id)

    def _resolve_reminder_targets(
        self,
        rule: ReminderRule,
        *,
        default_chat_id: int,
        owner_chat_id_override: int | None = None,
    ) -> list[int]:
        if not rule.targets:
            cid = int(default_chat_id or 0)
            return [cid] if cid != 0 else []

        out: list[int] = []
        seen: set[int] = set()

        for raw in rule.targets:
            token = (raw or '').strip()
            if not token:
                continue
            token_cf = token.casefold()

            if token_cf in {'owner', 'me', 'private'}:
                cid = int(owner_chat_id_override or 0) or int(self.owner_chat_id or 0) or int(default_chat_id or 0)
                if cid != 0 and cid not in seen:
                    out.append(cid)
                    seen.add(cid)
                continue

            if token_cf in {'broadcast', 'public', 'group', 'groups'}:
                for cid in self.reminder_broadcast_chat_ids:
                    if cid != 0 and cid not in seen:
                        out.append(cid)
                        seen.add(cid)
                continue

            try:
                cid = int(token)
            except ValueError:
                continue
            if cid != 0 and cid not in seen:
                out.append(cid)
                seen.add(cid)

        return out

    def _tick_reminders(
        self, *, now: dt.datetime, api: TelegramDeliveryAPI, state: BotState, default_chat_id: int
    ) -> None:
        entries = _load_reminders_db(self.reminders_file)
        if not entries:
            return

        reminders_chat_id, reminders_message_thread_id = state.reminders_target()
        default_chat_id = int(reminders_chat_id) if int(reminders_chat_id or 0) != 0 else int(default_chat_id)
        owner_chat_id_override = int(reminders_chat_id) if int(reminders_chat_id or 0) != 0 else None

        date_key = self._today_key(now)
        now_min = now.hour * 60 + now.minute
        grace = self.grace_minutes

        # If a deferred reminder was dropped from the outbox (e.g. due to max queue size),
        # clear its pending flag so we can retry within the grace window.
        try:
            outbox = state.tg_outbox_snapshot()
            keep_ids: set[str] = set()
            for it in outbox:
                if not isinstance(it, dict):
                    continue
                meta = it.get('meta')
                if not isinstance(meta, dict):
                    continue
                if str(meta.get('kind') or '').strip() != 'reminders':
                    continue
                if str(meta.get('date_key') or '').strip() != date_key:
                    continue
                ids = meta.get('reminder_ids')
                if not isinstance(ids, list):
                    continue
                for rid in ids:
                    s = str(rid or '').strip()
                    if s:
                        keep_ids.add(s)
            state.reminders_prune_pending(date_key, keep_ids=keep_ids)
        except Exception:
            pass

        due_by_chat: dict[int, list[tuple[str, str]]] = {}
        for e in entries:
            pr = _parse_reminder_rule(e.rule)
            if not pr:
                continue
            if (not self.reminders_include_weekends) and now.weekday() >= 5 and pr.kind == 'daily':
                continue
            if not _reminder_matches_date(pr, now.date()):
                continue

            due_min = _try_parse_hhmm(pr.label)
            if due_min is None:
                # If no explicit HH:MM label, don't spam — send only once at start of workday window.
                # (You can change policy later.)
                work_minutes = _work_hours_to_minutes(self.work_hours)
                due_min = work_minutes[0] if work_minutes else 9 * 60 + 30

            # due if passed and within grace window
            if now_min < due_min:
                continue
            if now_min > due_min + grace:
                continue

            text = e.text if pr.label is None else f'{pr.label}: {e.text}'
            targets = self._resolve_reminder_targets(
                pr,
                default_chat_id=default_chat_id,
                owner_chat_id_override=owner_chat_id_override,
            )
            if not targets:
                continue
            for target_chat_id in targets:
                rid = _sha1(f'{e.rule}\t{e.text}\t{int(target_chat_id)}')
                if state.reminders_was_sent(date_key, rid):
                    continue
                if state.reminders_was_pending(date_key, rid):
                    continue
                due_by_chat.setdefault(int(target_chat_id), []).append((rid, text))

        for target_chat_id, due in due_by_chat.items():
            if not due:
                continue

            text = '\n'.join([f'⏰ {t}' for _, t in due])
            reminder_ids = [rid for rid, _ in due]
            meta = {'kind': 'reminders', 'date_key': date_key, 'reminder_ids': reminder_ids}
            coalesce_key = _sha1(f'reminders:{date_key}:{int(target_chat_id)}:{"|".join(sorted(reminder_ids))}')
            thread_id: int | None = None
            if int(reminders_chat_id or 0) != 0 and int(target_chat_id) == int(reminders_chat_id):
                if int(reminders_message_thread_id or 0) != 0:
                    thread_id = int(reminders_message_thread_id)
            try:
                res = api.send_chunks(
                    chat_id=int(target_chat_id),
                    message_thread_id=thread_id,
                    text=text,
                    coalesce_key=coalesce_key,
                    meta=meta,
                )
            except Exception:
                continue

            deferred = bool(res.get('deferred')) if isinstance(res, dict) else False
            if deferred:
                state.reminders_mark_pending_many(date_key, reminder_ids)
            else:
                state.reminders_mark_sent_many(date_key, reminder_ids)

    def _tick_idle(self, *, now: dt.datetime, api: TelegramDeliveryAPI, state: BotState, chat_id: int) -> None:
        kb_ts = self._kb_touch_ts()
        touch_ts = max(kb_ts, state.last_user_msg_ts)

        if touch_ts <= 0:
            return

        notify_chat_id, notify_thread_id = self._watch_delivery_target(state=state, default_chat_id=chat_id)

        now_ts = time.time()

        # New day/new window: reset ping escalation so stages can restart.
        # Otherwise (because we clamp "idle" to the current work window start) the stage would drop
        # and we'd never send pings again until it exceeds the previous-day stage.
        window_start_ts = _work_window_start_ts(now, self.work_hours)
        if window_start_ts is not None and state.last_ping_stage > 0:
            if state.last_ping_ts <= 0 or state.last_ping_ts < window_start_ts:
                state.clear_ping_state()

        effective_touch_ts = touch_ts
        if window_start_ts is not None:
            effective_touch_ts = max(effective_touch_ts, window_start_ts)

        idle_seconds = max(0.0, now_ts - effective_touch_ts)

        # Determine current stage based on minutes since last touch,
        # but clamped to the current work-hours window start (so overnight doesn't escalate).
        stage = 0
        for i, minutes in enumerate(self.idle_stage_minutes, start=1):
            if idle_seconds >= float(minutes) * 60.0:
                stage = i

        if stage <= 0:
            return

        # Gentle mode: auto-enable after long silence.
        gentle_active = state.is_gentle_active()
        just_enabled_gentle = False
        if (not gentle_active) and self.gentle_auto_idle_minutes > 0:
            if idle_seconds >= float(self.gentle_auto_idle_minutes) * 60.0:
                state.enable_gentle(
                    seconds=int(self.gentle_default_minutes) * 60,
                    reason='auto: idle too long',
                    extend=True,
                )
                state.append_history(
                    role='bot',
                    kind='gentle_auto',
                    text=f'🫶 Включил щадящий режим автоматически (тишина {self._format_age(idle_seconds)}).',
                    meta={'why': 'idle', 'idle_seconds': int(idle_seconds)},
                    chat_id=notify_chat_id,
                    message_thread_id=notify_thread_id,
                    max_events=self.history_max_events,
                    max_chars=self.history_entry_max_chars,
                )
                gentle_active = True
                just_enabled_gentle = True

        # Apply stage cap in gentle mode.
        effective_stage = stage
        if gentle_active and self.gentle_stage_cap > 0:
            effective_stage = min(stage, int(self.gentle_stage_cap))

        # If user did something (telegram OR KB changed) after last ping, clear ping state.
        if state.last_ping_ts > 0 and touch_ts > state.touch_ts_at_ping:
            state.clear_ping_state()
            return

        # Don't repeat the same (or lower) stage for the same idle period.
        # In gentle mode we allow repeating after a cooldown (so it's not spammy, but still reaches you).
        now_ts = time.time()
        if effective_stage <= state.last_ping_stage:
            if not gentle_active:
                return
            cooldown = float(max(1, self.gentle_ping_cooldown_minutes)) * 60.0
            if state.last_ping_ts > 0 and (now_ts - state.last_ping_ts) < cooldown:
                return

        # Emit stage message.
        state.last_ping_stage = effective_stage
        state.last_ping_ts = now_ts
        state.touch_ts_at_ping = touch_ts
        state.save()

        msg = self._idle_message(
            stage=effective_stage,
            idle_seconds=idle_seconds,
            gentle_active=gentle_active,
            just_enabled_gentle=just_enabled_gentle,
        )
        state.append_history(
            role='bot',
            kind='watch_ping',
            text=msg,
            meta={'stage': int(effective_stage), 'gentle': bool(gentle_active)},
            chat_id=notify_chat_id,
            message_thread_id=notify_thread_id,
            max_events=self.history_max_events,
            max_chars=self.history_entry_max_chars,
        )
        api.send_message(
            chat_id=notify_chat_id,
            message_thread_id=notify_thread_id,
            text=msg,
            reply_markup=idle_stage(effective_stage, gentle_active=gentle_active),
        )

    def _idle_message(
        self, *, stage: int, idle_seconds: float, gentle_active: bool = False, just_enabled_gentle: bool = False
    ) -> str:
        age = self._format_age(idle_seconds)

        if gentle_active:
            header = '🫶 Щадящий режим включён.'
            if just_enabled_gentle:
                header = '🫶 Включил щадящий режим автоматически.'
            # Make every gentle message self-contained.
            if stage <= 1:
                return (
                    f'{header}\n'
                    f'Тишина {age}.\n'
                    'Можно просто нажать кнопку: ✅ Я здесь / 🍽️ Обед / 🔕 Пауза.\n'
                    'Если хочешь мягко вернуться — 🧠 Сводка или ✍️ Статус-шаблон.'
                )
            if stage == 2:
                return (
                    f'{header}\n'
                    f'Тишина {age}.\n'
                    'Самый маленький шаг: 1 строка ‘сделал → дальше → блокер’.\n'
                    'Или нажми 🧠 Сводка, или поставь 🔕 Пауза.'
                )
            # stage 3+
            return (
                f'{header}\n'
                f'Тишина {age}.\n'
                'Если нужно защитить вечер/выходные — 🔚 Закончить день.\n'
                'Если сейчас не до этого — поставь 🔕 Пауза.'
            )

        # Normal mode (non-gentle). Each stage message should be self-contained.
        if stage <= 1:
            return (
                f'👋 Давно тишина ({age}).\n'
                'Если ты на паузе — /lunch или /mute 30m.\n'
                'Если ты здесь — просто ответь любым сообщением или /back.'
            )

        if stage == 2:
            return (
                f'🧭 Уже {age} без активности (Telegram или KB).\n'
                'Чтобы вернуться без боли, можно сделать микро-шаг на 2 минуты:\n'
                '• 1 строка статуса: что сделал / что дальше / что блокирует\n'
                'Если сейчас не время — /mute 1h.'
            )

        if stage == 3:
            return (
                f'🧩 Всё ещё тихо уже {age}.\n'
                'Хочешь — я помогу быстро восстановить контекст: нажми 🧠 Сводка или /status.\n'
                'Если ты просто отдыхаешь/встреча — /mute 2h.'
            )

        if stage == 4:
            return (
                f'🚨 Похоже ты выпал примерно на {age}.\n'
                'Я могу помочь закрыть день или обновить статусы автоматически (через Codex/skills).\n'
                "Напиши: 'давай закончим день'.\n"
                'Если не актуально — /mute 1d.'
            )

        # stage 5+
        return (
            f'🧾 Напоминание: {age} без активности.\n'
            "Если хочется защитить выходные — можем закрыть день за минуту: 'давай закончим день'.\n"
            'Или поставь паузу: /mute 1d.'
        )
