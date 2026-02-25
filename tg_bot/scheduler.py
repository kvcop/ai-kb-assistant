from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import Condition, Lock
from typing import Generic, Protocol, TypeVar


class SchedulableEvent(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def chat_id(self) -> int: ...

    @property
    def message_thread_id(self) -> int: ...

    @property
    def received_ts(self) -> float: ...

    @property
    def text(self) -> str: ...


T = TypeVar('T', bound=SchedulableEvent)
Scope = tuple[int, int]


@dataclass(frozen=True)
class QueuedItem(Generic[T]):
    seq: int
    ts: float
    event: T

    @property
    def scope(self) -> Scope:
        try:
            cid = int(self.event.chat_id or 0)
        except Exception:
            cid = 0
        try:
            tid = int(self.event.message_thread_id or 0)
        except Exception:
            tid = 0
        return (cid, tid)


@dataclass(frozen=True)
class RunningItem(Generic[T]):
    item: QueuedItem[T]
    started_ts: float


class ParallelScheduler(Generic[T]):
    """Thread-safe scheduler: per-scope FIFO + global parallel slots.

    Design goals:
    - never run two jobs for the same scope (chat_id, message_thread_id) concurrently
    - respect a global `max_parallel_jobs`
    - allow "priority" events (e.g. confirmations) to jump ahead of normal backlog
    - support a "pause barrier" (defers older non-callback events to a paused backlog)
    - keep enough state for /queue snapshot + lightweight queue edit
    """

    def __init__(
        self,
        *,
        max_parallel_jobs: int,
        summarize: Callable[[T], str],
    ) -> None:
        self.max_parallel_jobs = max(1, int(max_parallel_jobs))
        self._summarize = summarize
        self._lock = Lock()
        self._cv = Condition(self._lock)
        self._seq = 0

        self._main: list[QueuedItem[T]] = []
        self._prio: list[QueuedItem[T]] = []
        self._paused: deque[QueuedItem[T]] = deque()
        self._running: dict[Scope, RunningItem[T]] = {}

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _ts_for(self, ev: T) -> float:
        try:
            ts = float(ev.received_ts or 0.0)
        except Exception:
            ts = 0.0
        if ts <= 0:
            ts = time.time()
        return ts

    def _is_callback(self, ev: T) -> bool:
        return str(getattr(ev, 'kind', '') or '').strip() == 'callback'

    def _should_pause(
        self,
        item: QueuedItem[T],
        *,
        pause_active: bool,
        pause_ts: float,
        scope_sleeping: Callable[[int, int], bool] | None = None,
    ) -> bool:
        if scope_sleeping is not None:
            try:
                if bool(scope_sleeping(*item.scope)):
                    return True
            except Exception:
                pass

        if not pause_active:
            return False

        if self._is_callback(item.event):
            return False

        if float(item.ts or 0.0) <= 0:
            return False
        return float(item.ts) < float(pause_ts or 0.0)

    def enqueue(self, ev: T, *, priority: bool = False) -> None:
        with self._cv:
            item = QueuedItem(seq=self._next_seq(), ts=self._ts_for(ev), event=ev)
            if priority:
                self._prio.append(item)
            else:
                self._main.append(item)
            self._cv.notify_all()

    def mark_done(self, *, chat_id: int, message_thread_id: int = 0) -> None:
        scope = (int(chat_id), int(message_thread_id or 0))
        with self._cv:
            self._running.pop(scope, None)
            self._cv.notify_all()

    def running_scopes_snapshot(self) -> set[Scope]:
        with self._lock:
            return set(self._running.keys())

    def running_count(self) -> int:
        with self._lock:
            return len(self._running)

    def scope_queue_len(self, *, chat_id: int, message_thread_id: int = 0) -> int:
        scope = (int(chat_id), int(message_thread_id or 0))
        with self._lock:
            n = 0
            if scope in self._running:
                n += 1
            for item in self._prio:
                if item.scope == scope:
                    n += 1
            for item in self._main:
                if item.scope == scope:
                    n += 1
            for item in self._paused:
                if item.scope == scope:
                    n += 1
            return n

    def _apply_pause_barrier(
        self,
        *,
        pause_active: bool,
        pause_ts: float,
        scope_sleeping: Callable[[int, int], bool] | None = None,
    ) -> None:
        if not pause_active:
            return
        if not self._main:
            return
        to_pause: list[QueuedItem[T]] = []
        kept: list[QueuedItem[T]] = []
        for item in self._main:
            if not self._is_callback(item.event) and self._should_pause(
                item, pause_active=pause_active, pause_ts=pause_ts, scope_sleeping=scope_sleeping
            ):
                to_pause.append(item)
            else:
                kept.append(item)
        if not to_pause:
            return
        self._main = kept
        self._paused.extend(to_pause)

    def _pick_best_eligible(
        self,
        items: list[QueuedItem[T]],
        *,
        pause_active: bool,
        pause_ts: float,
        scope_sleeping: Callable[[int, int], bool] | None = None,
    ) -> int | None:
        best_idx: int | None = None
        best_key: tuple[float, int] | None = None
        for idx, item in enumerate(items):
            scope = item.scope
            if scope in self._running:
                continue
            if self._should_pause(item, pause_active=pause_active, pause_ts=pause_ts, scope_sleeping=scope_sleeping):
                continue
            key = (float(item.ts or 0.0), int(item.seq or 0))
            if best_key is None or key < best_key:
                best_key = key
                best_idx = idx
        return best_idx

    def _pick_best_eligible_from_deque(
        self,
        items: deque[QueuedItem[T]],
        *,
        pause_active: bool,
        pause_ts: float,
        scope_sleeping: Callable[[int, int], bool] | None = None,
    ) -> int | None:
        best_idx: int | None = None
        best_key: tuple[float, int] | None = None
        for idx, item in enumerate(items):
            scope = item.scope
            if scope in self._running:
                continue
            if self._should_pause(item, pause_active=pause_active, pause_ts=pause_ts, scope_sleeping=scope_sleeping):
                continue
            key = (float(item.ts or 0.0), int(item.seq or 0))
            if best_key is None or key < best_key:
                best_key = key
                best_idx = idx
        return best_idx

    def try_dispatch_next(
        self,
        *,
        pause_active: bool,
        pause_ts: float,
        scope_sleeping: Callable[[int, int], bool] | None = None,
    ) -> T | None:
        """Pick one runnable event and mark its scope as running.

        Returns the event (caller should process it) or None if nothing is runnable or slots are full.
        """
        with self._cv:
            if len(self._running) >= int(self.max_parallel_jobs):
                return None

            # Enforce pause barrier lazily.
            self._apply_pause_barrier(pause_active=pause_active, pause_ts=pause_ts, scope_sleeping=scope_sleeping)

            # Priority first (e.g. dangerous confirms).
            idx = self._pick_best_eligible(
                self._prio,
                pause_active=pause_active,
                pause_ts=pause_ts,
                scope_sleeping=scope_sleeping,
            )
            if idx is not None:
                item = self._prio.pop(idx)
                self._running[item.scope] = RunningItem(item=item, started_ts=time.time())
                return item.event

            # When pause is lifted, resume backlog first (before "main").
            if not pause_active and self._paused:
                didx = self._pick_best_eligible_from_deque(
                    self._paused,
                    pause_active=pause_active,
                    pause_ts=pause_ts,
                    scope_sleeping=scope_sleeping,
                )
                if didx is not None:
                    item = self._paused[didx]
                    try:
                        del self._paused[didx]
                    except Exception:
                        # Fallback (should not happen).
                        item = self._paused.popleft()
                    self._running[item.scope] = RunningItem(item=item, started_ts=time.time())
                    return item.event
                # Paused backlog exists but all scopes are busy: do not dispatch newer items.
                return None

            idx = self._pick_best_eligible(
                self._main,
                pause_active=pause_active,
                pause_ts=pause_ts,
                scope_sleeping=scope_sleeping,
            )
            if idx is None:
                return None
            item = self._main.pop(idx)
            self._running[item.scope] = RunningItem(item=item, started_ts=time.time())
            return item.event

    def wait(self, *, timeout_seconds: float) -> None:
        with self._cv:
            self._cv.wait(timeout=max(0.05, float(timeout_seconds)))

    def snapshot(self, *, max_items: int) -> dict[str, object]:
        lim = max(0, int(max_items))
        with self._lock:
            main_n = len(self._main)
            prio_n = len(self._prio)
            paused_n = len(self._paused)

            main_head = [self._summarize(x.event) for x in self._main[:lim]]
            prio_head = [self._summarize(x.event) for x in self._prio[:lim]]
            paused_head = [self._summarize(x.event) for x in list(self._paused)[:lim]]

            running_items = list(self._running.values())
            running_items.sort(key=lambda r: (float(r.item.ts or 0.0), int(r.item.seq or 0)))
            running_summaries = [self._summarize(r.item.event) for r in running_items[: max(0, min(3, lim or 3))]]
            running_total = len(running_items)

        in_flight = ''
        if running_total == 1 and running_summaries:
            in_flight = running_summaries[0]
        elif running_total > 1 and running_summaries:
            in_flight = running_summaries[0] + f' (+{running_total - 1})'
        elif running_total > 0:
            in_flight = f'{running_total} running'

        return {
            'in_flight': in_flight,
            'main_n': int(main_n),
            'prio_n': int(prio_n),
            'paused_n': int(paused_n),
            'main_head': main_head,
            'prio_head': prio_head,
            'paused_head': paused_head,
        }

    def drop_all(self) -> dict[str, int]:
        with self._cv:
            n_main = len(self._main)
            n_prio = len(self._prio)
            n_paused = len(self._paused)
            self._main.clear()
            self._prio.clear()
            self._paused.clear()
            self._cv.notify_all()
        return {'main': int(n_main), 'prio': int(n_prio), 'paused': int(n_paused)}

    def mutate_main(self, *, action: str, index: int) -> dict[str, object]:
        a = str(action or '').strip().lower()
        try:
            i = int(index)
        except Exception:
            return {'ok': False, 'error': 'bad_index'}
        with self._cv:
            n = len(self._main)
            if i < 0 or i >= n:
                return {'ok': False, 'error': 'out_of_range', 'n': int(n)}

            changed = False
            if a in {'del', 'delete', 'rm'}:
                del self._main[i]
                changed = True
            elif a == 'up':
                if i > 0:
                    self._main[i - 1], self._main[i] = self._main[i], self._main[i - 1]
                    changed = True
            elif a in {'down', 'dn'}:
                if i + 1 < n:
                    self._main[i + 1], self._main[i] = self._main[i], self._main[i + 1]
                    changed = True
            else:
                return {'ok': False, 'error': 'bad_action'}

            if changed:
                self._cv.notify_all()

            return {'ok': True, 'changed': bool(changed), 'n': len(self._main)}
