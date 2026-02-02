import tempfile
import unittest
from pathlib import Path

from tg_bot.state import BotState


class TestStateMattermostPersistence(unittest.TestCase):
    def test_mm_maps_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')

            st = BotState(path=state_path)
            st.load()
            st.mm_mark_pending(channel_id='chan-a', up_to_ts=111)
            st.mm_mark_sent(channel_id='chan-b', up_to_ts=222)
            st.mm_set_session_token('sess-123')
            st.mm_mark_mfa_required()

            st2 = BotState(path=state_path)
            st2.load()
            self.assertEqual(st2.mm_pending_up_to_ts('chan-a'), 111)
            self.assertEqual(st2.mm_sent_up_to_ts('chan-b'), 222)
            self.assertEqual(st2.mm_get_session_token(), 'sess-123')
            self.assertTrue(st2.mm_is_mfa_required())
