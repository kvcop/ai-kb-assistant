import tempfile
import unittest
from pathlib import Path

from tg_bot.app import _merge_pending_attachments
from tg_bot.state import BotState


class TestMergePendingAttachments(unittest.TestCase):
    def test_merge_without_pending_does_not_crash(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            attachments = [{'path': 'live.txt', 'name': 'live.txt', 'kind': 'document', 'size_bytes': 1}]
            out_attachments, out_reply = _merge_pending_attachments(
                state=st,
                chat_id=1,
                message_thread_id=0,
                attachments=attachments,
                reply_to=None,
            )

            self.assertEqual(out_attachments, attachments)
            self.assertIsNone(out_reply)

    def test_merge_uses_pending_reply_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            st.add_pending_attachments(
                chat_id=1,
                message_thread_id=2,
                attachments=[{'path': 'pending.txt', 'name': 'pending.txt', 'kind': 'document', 'size_bytes': 2}],
            )
            st.set_pending_reply_to(
                chat_id=1,
                message_thread_id=2,
                reply_to={'message_id': 99, 'text': 'ping'},
            )

            attachments = [{'path': 'live.txt', 'name': 'live.txt', 'kind': 'document', 'size_bytes': 1}]
            out_attachments, out_reply = _merge_pending_attachments(
                state=st,
                chat_id=1,
                message_thread_id=2,
                attachments=attachments,
                reply_to=None,
            )

            self.assertEqual(out_attachments[-1], attachments[0])
            self.assertEqual(out_attachments[0].get('path'), 'pending.txt')
            self.assertEqual(int(out_reply.get('message_id') or 0), 99)
