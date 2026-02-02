import tempfile
import time
import unittest
from pathlib import Path

from tg_bot.state import BotState
from tg_bot.watch import Watcher


class _FakeAPI:
    def __init__(self) -> None:
        self.sent: list[tuple[int, int | None, str]] = []

    def send_message(
        self, *, chat_id: int, message_thread_id: int | None = None, text: str, **_: object
    ) -> dict[str, object]:
        self.sent.append((int(chat_id), int(message_thread_id) if message_thread_id is not None else None, str(text)))
        return {'ok': True, 'result': {'message_id': 1}}


class TestWatchIdleTopicDelivery(unittest.TestCase):
    def test_idle_pings_use_reminders_topic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.set_reminders_target(chat_id=-100, message_thread_id=777)

            with st.lock:
                st.last_user_msg_ts = time.time() - 70
            st.save()

            watcher = Watcher(
                repo_root=root,
                reminders_file=reminders,
                owner_chat_id=111,
                reminders_include_weekends=True,
                work_hours='00:00-23:59',
                include_weekends=True,
                idle_minutes=120,
                ack_minutes=20,
                idle_stage_minutes=[1],
                grace_minutes=5,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            api = _FakeAPI()
            watcher.tick(api=api, state=st)

            self.assertEqual(len(api.sent), 1)
            chat_id, thread_id, text = api.sent[0]
            self.assertEqual(chat_id, -100)
            self.assertEqual(thread_id, 777)
            self.assertIn('Давно тишина', text)

    def test_lunch_expired_ping_uses_reminders_topic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()
            st.set_reminders_target(chat_id=-100, message_thread_id=777)

            with st.lock:
                st.snooze_until_ts = time.time() - 1
                st.snooze_kind = 'lunch'
                st.last_user_msg_ts = time.time() - 600
            st.save()

            watcher = Watcher(
                repo_root=root,
                reminders_file=reminders,
                owner_chat_id=111,
                reminders_include_weekends=True,
                work_hours='00:00-23:59',
                include_weekends=True,
                idle_minutes=120,
                ack_minutes=20,
                idle_stage_minutes=[999_999],
                grace_minutes=5,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            api = _FakeAPI()
            watcher.tick(api=api, state=st)

            self.assertEqual(len(api.sent), 1)
            chat_id, thread_id, text = api.sent[0]
            self.assertEqual(chat_id, -100)
            self.assertEqual(thread_id, 777)
            self.assertIn('Обед закончился', text)
