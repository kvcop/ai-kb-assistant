import tempfile
import unittest
from pathlib import Path

from tg_bot.app import _restart_ack_coalesce_key, _restart_ack_message_id_from_state
from tg_bot.state import BotState


class TestRestartMessageEditSafety(unittest.TestCase):
    def test_restart_ack_coalesce_key(self) -> None:
        self.assertEqual(_restart_ack_coalesce_key(chat_id=1, restart_message_id=2), 'ack:1:2')
        self.assertEqual(_restart_ack_coalesce_key(chat_id=0, restart_message_id=2), '')
        self.assertEqual(_restart_ack_coalesce_key(chat_id=1, restart_message_id=0), '')

    def test_restart_ack_message_id_from_state_uses_coalesce_binding(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            st.tg_bind_message_id_for_coalesce_key(chat_id=1, coalesce_key='ack:1:42', message_id=999)

            self.assertEqual(_restart_ack_message_id_from_state(st, chat_id=1, restart_message_id=42), 999)
            self.assertEqual(_restart_ack_message_id_from_state(st, chat_id=1, restart_message_id=43), 0)
