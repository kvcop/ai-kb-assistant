import unittest

from tg_bot.app import Event, _restart_queue_drained, _should_spool_during_restart
from tg_bot.scheduler import ParallelScheduler


class TestShouldSpoolDuringRestart(unittest.TestCase):
    def test_spools_regular_text(self) -> None:
        ev = Event(kind='text', chat_id=1, chat_type='private', user_id=1, text='hello')
        self.assertTrue(_should_spool_during_restart(ev))

    def test_does_not_spool_restart_command(self) -> None:
        ev = Event(kind='text', chat_id=1, chat_type='private', user_id=1, text='/restart')
        self.assertFalse(_should_spool_during_restart(ev))


class TestRestartQueueDrained(unittest.TestCase):
    def test_restart_queue_drained_ignores_running_jobs(self) -> None:
        scheduler = ParallelScheduler(max_parallel_jobs=1, summarize=lambda ev: str(getattr(ev, 'text', '')))
        ev = Event(kind='text', chat_id=1, chat_type='private', user_id=1, text='hello', message_id=1)
        scheduler.enqueue(ev)

        self.assertFalse(_restart_queue_drained(scheduler=scheduler))

        dispatched = scheduler.try_dispatch_next(pause_active=False, pause_ts=0.0)
        self.assertIsNotNone(dispatched)
        self.assertEqual(scheduler.running_count(), 1)
        self.assertTrue(_restart_queue_drained(scheduler=scheduler))
