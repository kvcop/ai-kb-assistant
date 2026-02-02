import unittest
from dataclasses import dataclass

from tg_bot.scheduler import ParallelScheduler


@dataclass(frozen=True)
class _Ev:
    kind: str
    chat_id: int
    message_thread_id: int
    received_ts: float
    text: str


def _summ(ev: _Ev) -> str:
    return f'{ev.kind} chat={ev.chat_id} tid={ev.message_thread_id}: {ev.text}'


class TestParallelScheduler(unittest.TestCase):
    def test_per_scope_serialization_and_slots(self) -> None:
        s: ParallelScheduler[_Ev] = ParallelScheduler(max_parallel_jobs=2, summarize=_summ)
        ev1 = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=10.0, text='a')
        ev2 = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=11.0, text='b')
        ev3 = _Ev(kind='text', chat_id=2, message_thread_id=0, received_ts=12.0, text='c')
        s.enqueue(ev1)
        s.enqueue(ev2)
        s.enqueue(ev3)

        d1 = s.try_dispatch_next(pause_active=False, pause_ts=0.0)
        d2 = s.try_dispatch_next(pause_active=False, pause_ts=0.0)
        self.assertEqual({d1, d2}, {ev1, ev3})
        self.assertIsNone(s.try_dispatch_next(pause_active=False, pause_ts=0.0))

        s.mark_done(chat_id=2, message_thread_id=0)
        self.assertIsNone(s.try_dispatch_next(pause_active=False, pause_ts=0.0))

        s.mark_done(chat_id=1, message_thread_id=0)
        d3 = s.try_dispatch_next(pause_active=False, pause_ts=0.0)
        self.assertEqual(d3, ev2)

    def test_priority_beats_main(self) -> None:
        s: ParallelScheduler[_Ev] = ParallelScheduler(max_parallel_jobs=1, summarize=_summ)
        main = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=10.0, text='main')
        prio = _Ev(kind='callback', chat_id=1, message_thread_id=0, received_ts=20.0, text='prio')
        s.enqueue(main)
        s.enqueue(prio, priority=True)
        d = s.try_dispatch_next(pause_active=False, pause_ts=0.0)
        self.assertEqual(d, prio)

    def test_pause_barrier_defers_old_events(self) -> None:
        s: ParallelScheduler[_Ev] = ParallelScheduler(max_parallel_jobs=1, summarize=_summ)
        old = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=5.0, text='old')
        new = _Ev(kind='text', chat_id=1, message_thread_id=1, received_ts=15.0, text='new')
        s.enqueue(old)
        s.enqueue(new)

        d1 = s.try_dispatch_next(pause_active=True, pause_ts=10.0)
        self.assertEqual(d1, new)
        snap = s.snapshot(max_items=10)
        self.assertEqual(snap['paused_n'], 1)

        s.mark_done(chat_id=1, message_thread_id=1)
        d2 = s.try_dispatch_next(pause_active=False, pause_ts=10.0)
        self.assertEqual(d2, old)

    def test_mutate_main_up_down_delete(self) -> None:
        s: ParallelScheduler[_Ev] = ParallelScheduler(max_parallel_jobs=1, summarize=_summ)
        a = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=1.0, text='a')
        b = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=2.0, text='b')
        c = _Ev(kind='text', chat_id=1, message_thread_id=0, received_ts=3.0, text='c')
        s.enqueue(a)
        s.enqueue(b)
        s.enqueue(c)

        snap0 = s.snapshot(max_items=10)
        self.assertEqual([x.split(': ', 1)[-1] for x in snap0['main_head']], ['a', 'b', 'c'])

        self.assertTrue(s.mutate_main(action='up', index=2)['ok'])
        snap1 = s.snapshot(max_items=10)
        self.assertEqual([x.split(': ', 1)[-1] for x in snap1['main_head']], ['a', 'c', 'b'])

        self.assertTrue(s.mutate_main(action='down', index=0)['ok'])
        snap2 = s.snapshot(max_items=10)
        self.assertEqual([x.split(': ', 1)[-1] for x in snap2['main_head']], ['c', 'a', 'b'])

        self.assertTrue(s.mutate_main(action='del', index=1)['ok'])
        snap3 = s.snapshot(max_items=10)
        self.assertEqual([x.split(': ', 1)[-1] for x in snap3['main_head']], ['c', 'b'])
