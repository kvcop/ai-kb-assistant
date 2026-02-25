import tempfile
import unittest
from pathlib import Path

from tg_bot.collect_payload import (
    build_collect_packet,
    collect_packet_send_decision,
    collect_preflight_budget_report,
)
from tg_bot.state import BotState


class TestCollectPayload(unittest.TestCase):
    def _make_packet(self, *, text: str) -> dict[str, object]:
        return build_collect_packet(
            instruction='summarize',
            items=[
                {
                    'message_id': 11,
                    'author': 'alice',
                    'text': text,
                    'attachments': [{'name': 'file-a.txt'}],
                }
            ],
            chat_id=101,
            message_thread_id=2,
            created_ts=1700000000.0,
        )

    def _make_state(self) -> tuple[BotState, tempfile.TemporaryDirectory]:
        td = tempfile.TemporaryDirectory()
        state_path = Path(td.name) / 'state.json'
        state_path.write_text('{}', encoding='utf-8')
        st = BotState(path=state_path)
        st.load()
        return st, td

    def test_build_collect_packet_and_preflight_ok(self) -> None:
        packet = self._make_packet(text='alpha beta')
        packet_without_ts = build_collect_packet(
            instruction='summarize',
            items=[{'message_id': 11, 'author': 'alice', 'text': 'alpha beta'}],
            chat_id=101,
            message_thread_id=2,
        )
        report = collect_preflight_budget_report(
            packet,
            max_payload_chars=10000,
            max_items=10,
            max_metadata_chars=1000,
        )

        self.assertEqual(packet['instruction'], 'summarize')
        self.assertEqual(packet['scope_metadata'], {'chat_id': 101, 'message_thread_id': 2})
        self.assertEqual(len(packet['items']), 1)
        item = packet['items'][0]
        self.assertEqual(item['item_metadata']['message_id'], 11)
        self.assertEqual(item['item_metadata']['author'], 'alice')
        self.assertEqual(item['item_metadata']['attachments_summary'], 'file-a.txt')
        self.assertTrue(report['ok'])
        self.assertEqual(report['metrics']['items_count'], 1)
        self.assertEqual(report['reasons'], [])
        self.assertGreater(packet_without_ts['created_ts'], 0.0)

    def test_collect_preflight_budget_pending(self) -> None:
        packet = self._make_packet(text='x' * 200)
        st, tempdir = self._make_state()
        try:
            decision = collect_packet_send_decision(
                packet,
                state=st,
                max_payload_chars=20,
                max_items=10,
                max_metadata_chars=1000,
                force=False,
            )
            self.assertFalse(decision['ok'])
            self.assertEqual(decision['decision'], 'pending')
            self.assertFalse(decision['forced'])
            stored = st.collect_packet_decision(chat_id=101, message_thread_id=2, packet_id=packet['packet_id'])
            self.assertIsNotNone(stored)
            self.assertEqual(stored.get('status'), 'pending')
            self.assertGreaterEqual(len(stored.get('reasons', [])), 1)
        finally:
            tempdir.cleanup()

    def test_collect_preflight_metadata_limit_pending(self) -> None:
        packet = build_collect_packet(
            instruction='summarize',
            items=[
                {
                    'message_id': 11,
                    'author': 'author-' + ('x' * 500),
                    'text': 'alpha beta',
                    'attachments': [
                        {'name': 'file-' + ('a' * 120)},
                        {'name': 'file-' + ('b' * 120)},
                        {'name': 'file-' + ('c' * 120)},
                        {'name': 'file-' + ('d' * 120)},
                    ],
                }
            ],
            chat_id=101,
            message_thread_id=2,
            created_ts=1700000000.0,
        )
        report = collect_preflight_budget_report(
            packet,
            max_payload_chars=100000,
            max_items=100,
            max_metadata_chars=400,
        )
        self.assertFalse(report['ok'])
        self.assertTrue(report['over_limit'])
        self.assertTrue(any(reason.startswith('metadata chars limit exceeded') for reason in report['reasons']))

        st, tempdir = self._make_state()
        try:
            decision = collect_packet_send_decision(
                packet,
                state=st,
                max_payload_chars=100000,
                max_items=100,
                max_metadata_chars=400,
                force=False,
            )
            self.assertFalse(decision['ok'])
            self.assertTrue(decision['over_limit'])
            self.assertEqual(decision['decision'], 'pending')
            self.assertFalse(decision['forced'])
            self.assertTrue(any(reason.startswith('metadata chars limit exceeded') for reason in decision['reasons']))
        finally:
            tempdir.cleanup()

    def test_collect_preflight_budget_force(self) -> None:
        packet = self._make_packet(text='x' * 200)
        st, tempdir = self._make_state()
        try:
            decision = collect_packet_send_decision(
                packet,
                state=st,
                max_payload_chars=20,
                max_items=10,
                max_metadata_chars=1000,
                force=True,
            )
            self.assertTrue(decision['ok'])
            self.assertEqual(decision['decision'], 'forced')
            self.assertTrue(decision['forced'])
            stored = st.collect_packet_decision(chat_id=101, message_thread_id=2, packet_id=packet['packet_id'])
            self.assertIsNotNone(stored)
            self.assertEqual(stored.get('status'), 'forced')
            self.assertGreaterEqual(len(stored.get('reasons', [])), 1)
        finally:
            tempdir.cleanup()

    def test_collect_packet_send_decision_retry_uses_stored_packet(self) -> None:
        packet = self._make_packet(text='x' * 200)
        st1, tempdir = self._make_state()
        try:
            first = collect_packet_send_decision(
                packet,
                state=st1,
                max_payload_chars=20,
                max_items=10,
                max_metadata_chars=1000,
                force=False,
            )
            self.assertEqual(first['decision'], 'pending')

            st2 = BotState(path=st1.path)
            st2.load()
            second = collect_packet_send_decision(
                packet,
                state=st2,
                max_payload_chars=20,
                max_items=10,
                max_metadata_chars=1000,
                force=True,
            )
            self.assertTrue(second['ok'])
            self.assertEqual(second['decision'], 'forced')
            stored = st2.collect_packet_decision(chat_id=101, message_thread_id=2, packet_id=packet['packet_id'])
            self.assertEqual(stored.get('status'), 'forced')
        finally:
            tempdir.cleanup()

    def test_collect_packet_send_decision_retries_pending_without_force(self) -> None:
        packet = self._make_packet(text='x' * 200)
        st, tempdir = self._make_state()
        try:
            first = collect_packet_send_decision(
                packet,
                state=st,
                max_payload_chars=20,
                max_items=10,
                max_metadata_chars=1000,
                force=False,
            )
            self.assertEqual(first['decision'], 'pending')

            second = collect_packet_send_decision(
                packet,
                state=st,
                max_payload_chars=20,
                max_items=10,
                max_metadata_chars=1000,
                force=False,
            )
            self.assertEqual(second['decision'], 'pending')
            self.assertFalse(second['ok'])
            stored = st.collect_packet_decision(chat_id=101, message_thread_id=2, packet_id=packet['packet_id'])
            self.assertIsNotNone(stored)
            self.assertEqual(stored.get('status'), 'pending')
        finally:
            tempdir.cleanup()
