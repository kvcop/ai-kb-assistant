import tempfile
import unittest
from pathlib import Path

from tg_bot.state import BotState


class TestBotStateRemindersTarget(unittest.TestCase):
    def test_reminders_target_persists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.set_reminders_target(chat_id=-100, message_thread_id=777)

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.reminders_target(), (-100, 777))

    def test_reminders_target_clears_thread_when_chat_zero(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.set_reminders_target(chat_id=0, message_thread_id=777)

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.reminders_target(), (0, 0))
