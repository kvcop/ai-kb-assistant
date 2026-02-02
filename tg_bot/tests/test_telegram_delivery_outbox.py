import datetime as dt
import hashlib
import tempfile
import unittest
from pathlib import Path

from tg_bot.state import BotState
from tg_bot.telegram_api import TelegramDeliveryAPI
from tg_bot.watch import Watcher


class _FakeTelegramAPI:
    def __init__(self) -> None:
        self.fail_ops: set[str] = set()
        self.calls: list[tuple[str, int, str]] = []

    def get_updates(self, *_: object, **__: object) -> list[dict[str, object]]:
        return []

    def get_me(self, *_: object, **__: object) -> dict[str, object]:
        return {'ok': True, 'result': {'id': 1, 'username': 'fake_bot'}}

    def send_message(self, *, chat_id: int, text: str, **_: object) -> dict[str, object]:
        self.calls.append(('send_message', int(chat_id), str(text)))
        if 'send_message' in self.fail_ops:
            raise RuntimeError('The read operation timed out')
        return {'ok': True, 'result': {'message_id': 1}}

    def send_chunks(self, *, chat_id: int, text: str, **_: object) -> None:
        self.calls.append(('send_chunks', int(chat_id), str(text)))
        if 'send_chunks' in self.fail_ops:
            raise RuntimeError('The read operation timed out')

    def send_document(self, *, chat_id: int, document_path: str, **_: object) -> dict[str, object]:
        self.calls.append(('send_document', int(chat_id), str(document_path)))
        if 'send_document' in self.fail_ops:
            raise RuntimeError('The read operation timed out')
        return {'ok': True, 'result': {'message_id': 2}}

    def edit_message_text(self, *_: object, **__: object) -> dict[str, object]:
        if 'edit_message_text' in self.fail_ops:
            raise RuntimeError('The read operation timed out')
        return {'ok': True, 'result': {'message_id': 1}}

    def edit_message_reply_markup(self, *_: object, **__: object) -> dict[str, object]:
        if 'edit_message_reply_markup' in self.fail_ops:
            raise RuntimeError('The read operation timed out')
        return {'ok': True, 'result': {'message_id': 1}}

    def delete_message(self, *_: object, **__: object) -> dict[str, object]:
        chat_id = 0
        message_id = 0
        try:
            chat_id = int(__.get('chat_id') or 0)  # type: ignore[attr-defined]
        except Exception:
            chat_id = 0
        try:
            message_id = int(__.get('message_id') or 0)  # type: ignore[attr-defined]
        except Exception:
            message_id = 0
        if chat_id:
            self.calls.append(('delete_message', int(chat_id), str(int(message_id))))
        if 'delete_message' in self.fail_ops:
            raise RuntimeError('The read operation timed out')
        return {'ok': True, 'result': True}


class TestTelegramDeliveryOutbox(unittest.TestCase):
    def test_send_document_deferred_then_flushed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            f = root / 'hello.txt'
            f.write_text('hi', encoding='utf-8')

            api_raw = _FakeTelegramAPI()
            api_raw.fail_ops = {'send_document'}
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

            api.send_document(chat_id=111, document_path=str(f), caption='cap')
            outbox = st.tg_outbox_snapshot()
            self.assertEqual(len(outbox), 1)
            self.assertEqual(str(outbox[0].get('op') or ''), 'send_document')

            api_raw.fail_ops = set()
            outbox_now = st.tg_outbox_snapshot()
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertTrue(any(op == 'send_document' and cid == 111 for op, cid, _ in api_raw.calls))

    def test_upload_ack_deleted_after_send_document_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            f = root / 'hello.txt'
            f.write_text('hi', encoding='utf-8')

            ack_key = 'upload_ack:test'
            st.tg_bind_message_id_for_coalesce_key(chat_id=111, coalesce_key=ack_key, message_id=42)
            st.tg_outbox_enqueue(
                item={
                    'id': '1',
                    'op': 'send_document',
                    'chat_id': 111,
                    'params': {'chat_id': 111, 'document_path': str(f), 'timeout': 30},
                    'created_ts': 0.0,
                    'attempts': 1,
                    'next_attempt_ts': 0.0,
                    'last_error': '',
                    'meta': {'kind': 'upload', 'ack_chat_id': 111, 'ack_coalesce_key': ack_key},
                },
                max_items=500,
            )

            api_raw = _FakeTelegramAPI()
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

            api.flush_outbox(max_ops=10)

            outbox_now = st.tg_outbox_snapshot()
            self.assertEqual(len(outbox_now), 1)
            self.assertEqual(str(outbox_now[0].get('op') or ''), 'delete_message_by_key')

            # The outbox uses exponential backoff; force due now for deterministic testing.
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertTrue(any(op == 'send_document' and cid == 111 for op, cid, _ in api_raw.calls))
            self.assertTrue(
                any(op == 'delete_message' and cid == 111 and mid == '42' for op, cid, mid in api_raw.calls)
            )

    def test_flush_outbox_supports_negative_chat_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            st.tg_outbox_enqueue(
                item={
                    'id': '1',
                    'op': 'send_message',
                    'chat_id': -100,
                    'params': {'chat_id': -100, 'text': 'hi', 'timeout': 30},
                    'created_ts': 0.0,
                    'attempts': 1,
                    'next_attempt_ts': 0.0,
                    'last_error': '',
                },
                max_items=500,
            )

            api_raw = _FakeTelegramAPI()
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

            delivered = api.flush_outbox(max_ops=10)
            self.assertEqual(delivered, 1)
            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertEqual(api_raw.calls, [('send_message', -100, 'hi')])

    def test_reminders_deferred_then_marked_sent_on_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            reminders = root / 'reminders.md'
            reminders.write_text('date:2026-01-01@00:00|to=-100\tTest reminder\n', encoding='utf-8')

            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api_raw.fail_ops = {'send_chunks'}
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

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
                grace_minutes=90,
                gentle_default_minutes=60,
                gentle_auto_idle_minutes=0,
                gentle_ping_cooldown_minutes=0,
                gentle_stage_cap=0,
            )

            now = dt.datetime(2026, 1, 1, 0, 0)
            date_key = '2026-01-01'
            rid = hashlib.sha1(b'date:2026-01-01@00:00|to=-100\tTest reminder\t-100').hexdigest()

            watcher._tick_reminders(now=now, api=api, state=st, default_chat_id=111)

            self.assertTrue(st.reminders_was_pending(date_key, rid))
            self.assertFalse(st.reminders_was_sent(date_key, rid))

            outbox = st.tg_outbox_snapshot()
            self.assertEqual(len(outbox), 1)
            self.assertEqual(int(outbox[0].get('chat_id') or 0), -100)
            self.assertEqual(str(outbox[0].get('op') or ''), 'send_chunks')
            meta = outbox[0].get('meta') or {}
            self.assertEqual(str(meta.get('kind') or ''), 'reminders')
            self.assertEqual(str(meta.get('date_key') or ''), date_key)
            self.assertEqual(meta.get('reminder_ids'), [rid])

            watcher._tick_reminders(now=now, api=api, state=st, default_chat_id=111)
            self.assertEqual(len(st.tg_outbox_snapshot()), 1)
            self.assertEqual(len([c for c in api_raw.calls if c[0] == 'send_chunks']), 1)

            api_raw.fail_ops = set()
            # The outbox uses exponential backoff; force due now for deterministic testing.
            outbox_now = st.tg_outbox_snapshot()
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertFalse(st.reminders_was_pending(date_key, rid))
            self.assertTrue(st.reminders_was_sent(date_key, rid))
            # Restore notices are private-only (no spam in groups).
            self.assertFalse(
                any(op == 'send_message' and t.startswith('🌐 Сеть была недоступна') for op, _, t in api_raw.calls)
            )
            self.assertTrue(any('⏰ 00:00: Test reminder' in t for op, _, t in api_raw.calls if op == 'send_chunks'))

    def test_mattermost_deferred_then_marked_sent_on_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            st.mm_mark_pending(channel_id='chan-1', up_to_ts=123)

            st.tg_outbox_enqueue(
                item={
                    'id': '1',
                    'op': 'send_chunks',
                    'chat_id': 111,
                    'params': {'chat_id': 111, 'text': 'hi', 'chunk_size': 3900},
                    'created_ts': 0.0,
                    'attempts': 1,
                    'next_attempt_ts': 0.0,
                    'last_error': '',
                    'meta': {'kind': 'mattermost', 'channel_id': 'chan-1', 'up_to_ts': 123},
                },
                max_items=500,
            )

            api_raw = _FakeTelegramAPI()
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')
            delivered = api.flush_outbox(max_ops=10)
            self.assertEqual(delivered, 1)
            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertEqual(st.mm_sent_up_to_ts('chan-1'), 123)
            self.assertEqual(st.mm_pending_up_to_ts('chan-1'), 0)
            self.assertTrue(any(op == 'send_chunks' and cid == 111 for op, cid, _ in api_raw.calls))

    def test_restore_notice_not_spammed_while_outbox_keeps_failing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api_raw.fail_ops = {'send_chunks'}
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

            api.send_chunks(chat_id=111, text='hi')

            outbox_now = st.tg_outbox_snapshot()
            self.assertEqual(len(outbox_now), 1)

            # Force due now and flush twice; restore notice should be sent only once.
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            outbox_now2 = st.tg_outbox_snapshot()
            for it in outbox_now2:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now2)
            api.flush_outbox(max_ops=10)

            notices = [
                (op, cid, txt)
                for (op, cid, txt) in api_raw.calls
                if op == 'send_message' and txt.startswith('🌐 Сеть была недоступна')
            ]
            self.assertEqual(len(notices), 1)

    def test_restore_notice_skipped_when_only_edits_pending(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api_raw.fail_ops = {'edit_message_text'}
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

            api.edit_message_text(chat_id=111, message_id=1, text='hi')

            outbox_now = st.tg_outbox_snapshot()
            self.assertEqual(len(outbox_now), 1)
            self.assertEqual(str(outbox_now[0].get('op') or ''), 'edit_message_text')

            api_raw.fail_ops = set()
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertFalse(
                any(op == 'send_message' and t.startswith('🌐 Сеть была недоступна') for op, _, t in api_raw.calls),
                msg=f'unexpected restore notice calls: {api_raw.calls!r}',
            )

    def test_scheduled_delete_by_coalesce_key_runs_after_deferred_send(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api_raw.fail_ops = {'send_message'}
            api = TelegramDeliveryAPI(api=api_raw, state=st, log_path=root / 'net.log')

            key = 'done:111:10:abc'
            api.send_message(chat_id=111, text='✅ Готово', coalesce_key=key, timeout=10)
            api.schedule_delete_message_by_coalesce_key(chat_id=111, coalesce_key=key, delete_after_seconds=300)

            outbox_now = st.tg_outbox_snapshot()
            self.assertEqual(len(outbox_now), 2)

            api_raw.fail_ops = set()
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            self.assertEqual(st.tg_outbox_snapshot(), [])
            self.assertTrue(any(op == 'send_message' and txt == '✅ Готово' for op, _, txt in api_raw.calls))
            self.assertTrue(any(op == 'delete_message' for op, _, _ in api_raw.calls))
