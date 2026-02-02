import datetime as dt
import tempfile
import time
import unittest
from pathlib import Path

from tg_bot.state import BotState
from tg_bot.watch import Watcher


class _FakeAPI:
    def __init__(self) -> None:
        self.sent_chunks: list[tuple[int, str]] = []

    def send_chunks(self, *, chat_id: int, text: str, **_: object) -> None:
        self.sent_chunks.append((int(chat_id), str(text)))


class _WatcherSpy(Watcher):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[misc]
        self.idle_called = False

    def _tick_reminders(self, *_: object, **__: object) -> None:  # type: ignore[override]
        return

    def _tick_idle(self, *_: object, **__: object) -> None:  # type: ignore[override]
        self.idle_called = True


class TestWatcherNotifications(unittest.TestCase):
    def test_reminders_send_while_snoozed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('date:2025-12-28@12:00\tTest reminder\n', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.set_snooze(3600, kind='mute')
            self.assertTrue(st.is_snoozed())

            watcher = Watcher(
                repo_root=root,
                reminders_file=reminders,
                owner_chat_id=111,
                reminders_include_weekends=False,
                work_hours='00:00-23:59',
                include_weekends=True,
                idle_minutes=120,
                ack_minutes=20,
                idle_stage_minutes=[120, 140, 170],
                grace_minutes=5,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            api = _FakeAPI()
            now = dt.datetime(2025, 12, 28, 12, 0)
            watcher._tick_reminders(now=now, api=api, state=st, default_chat_id=111)

            self.assertEqual(len(api.sent_chunks), 1)
            chat_id, text = api.sent_chunks[0]
            self.assertEqual(chat_id, 111)
            self.assertIn('⏰', text)
            self.assertIn('12:00', text)
            self.assertIn('Test reminder', text)

    def test_weekly_reminders_fire_on_selected_weekdays(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('weekly:tue,fri@11:50|to=owner\tPrep for 12:00 call\n', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            watcher = Watcher(
                repo_root=root,
                reminders_file=reminders,
                owner_chat_id=111,
                reminders_include_weekends=True,
                work_hours='00:00-23:59',
                include_weekends=True,
                idle_minutes=120,
                ack_minutes=20,
                idle_stage_minutes=[120, 140, 170],
                grace_minutes=5,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            api = _FakeAPI()
            now = dt.datetime(2025, 12, 30, 11, 50)  # Tuesday
            self.assertEqual(now.date().weekday(), 1)
            watcher._tick_reminders(now=now, api=api, state=st, default_chat_id=111)

            self.assertEqual(len(api.sent_chunks), 1)
            _, text = api.sent_chunks[0]
            self.assertIn('⏰', text)
            self.assertIn('11:50', text)
            self.assertIn('Prep for 12:00 call', text)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('weekly:tue,fri@11:50|to=owner\tPrep for 12:00 call\n', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            watcher = Watcher(
                repo_root=root,
                reminders_file=reminders,
                owner_chat_id=111,
                reminders_include_weekends=True,
                work_hours='00:00-23:59',
                include_weekends=True,
                idle_minutes=120,
                ack_minutes=20,
                idle_stage_minutes=[120, 140, 170],
                grace_minutes=5,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            api = _FakeAPI()
            now = dt.datetime(2025, 12, 31, 11, 50)  # Wednesday
            self.assertEqual(now.date().weekday(), 2)
            watcher._tick_reminders(now=now, api=api, state=st, default_chat_id=111)

            self.assertEqual(api.sent_chunks, [])

    def test_idle_pings_skipped_when_initiatives_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = _FakeAPI()
            watcher = _WatcherSpy(
                repo_root=root,
                reminders_file=reminders,
                owner_chat_id=111,
                reminders_include_weekends=True,
                work_hours='00:00-23:59',
                include_weekends=True,
                idle_minutes=120,
                ack_minutes=20,
                idle_stage_minutes=[120, 140, 170],
                grace_minutes=5,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            watcher.tick(api=api, state=st)
            self.assertTrue(watcher.idle_called)

            watcher.idle_called = False
            st.ux_set_bot_initiatives_enabled(chat_id=111, value=False)
            self.assertFalse(st.ux_bot_initiatives_enabled(chat_id=111))

            # ensure it won't be skipped by snooze
            st.clear_snooze()
            with st.lock:
                st.last_user_msg_ts = time.time()
            st.save()

            watcher.tick(api=api, state=st)
            self.assertFalse(watcher.idle_called)
