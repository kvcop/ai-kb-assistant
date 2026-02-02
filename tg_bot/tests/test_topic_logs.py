import json
import tempfile
import unittest
from pathlib import Path

from tg_bot.state import BotState
from tg_bot.telegram_api import TelegramDeliveryAPI


class _FakeTelegramAPI:
    def __init__(self) -> None:
        self.fail_send = False
        self.next_message_id = 100

    def send_message(
        self, *, chat_id: int, message_thread_id: int | None = None, text: str, **_: object
    ) -> dict[str, object]:
        if self.fail_send:
            raise RuntimeError('The read operation timed out')
        self.next_message_id += 1
        return {
            'ok': True,
            'result': {
                'message_id': int(self.next_message_id),
                'message_thread_id': int(message_thread_id or 0),
                'text': text,
            },
        }

    def send_document(
        self, *, chat_id: int, message_thread_id: int | None = None, document_path: str, **_: object
    ) -> dict[str, object]:
        self.next_message_id += 1
        return {
            'ok': True,
            'result': {'message_id': int(self.next_message_id), 'message_thread_id': int(message_thread_id or 0)},
        }

    def send_chunks(self, *_: object, **__: object) -> None:
        return None

    def edit_message_text(self, *, chat_id: int, message_id: int, text: str, **_: object) -> dict[str, object]:
        return {'ok': True, 'result': {'message_id': int(message_id), 'message_thread_id': 222, 'text': text}}

    def edit_message_reply_markup(self, *_: object, **__: object) -> dict[str, object]:
        return {'ok': True, 'result': {'message_id': 1}}

    def delete_message(self, *_: object, **__: object) -> dict[str, object]:
        return {'ok': True, 'result': True}


class TestTopicLogs(unittest.TestCase):
    def test_incoming_outgoing_and_redaction(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api = TelegramDeliveryAPI(
                api=api_raw,
                state=st,
                log_path=root / 'net.log',
                topic_log_root=root / 'topics',
                topic_log_max_chars=200,
            )

            api.send_message(chat_id=111, message_thread_id=222, text='hello', parse_mode='HTML')
            api.log_incoming_message(
                chat_id=111,
                message_thread_id=222,
                chat_type='private',
                user_id=1,
                username='user',
                message_id=10,
                cmd='/mm-otp',
                text='/mm-otp 123456',
                attachments=None,
            )
            api.edit_message_text(chat_id=111, message_id=999, text='edited', parse_mode='HTML')

            p = root / 'topics' / '111' / '222' / 'events.jsonl'
            lines = p.read_text(encoding='utf-8').splitlines()
            self.assertGreaterEqual(len(lines), 3)
            items = [json.loads(x) for x in lines]

            out_send = [
                x for x in items if x.get('dir') == 'out' and x.get('op') == 'send_message' and x.get('text') == 'hello'
            ]
            self.assertTrue(out_send)

            in_otp = [x for x in items if x.get('dir') == 'in' and x.get('cmd') == '/mm-otp']
            self.assertTrue(in_otp)
            self.assertEqual(in_otp[-1].get('text'), '/mm-otp <redacted>')

            out_edit = [
                x
                for x in items
                if x.get('dir') == 'out' and x.get('op') == 'edit_message_text' and x.get('text') == 'edited'
            ]
            self.assertTrue(out_edit)

    def test_deferred_send_logged_and_flushed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api_raw.fail_send = True
            api = TelegramDeliveryAPI(
                api=api_raw,
                state=st,
                log_path=root / 'net.log',
                topic_log_root=root / 'topics',
                topic_log_max_chars=200,
            )

            r = api.send_message(chat_id=111, message_thread_id=222, text='queued')
            self.assertFalse(bool(r.get('ok', True)))
            self.assertTrue(bool(r.get('deferred', False)))

            api_raw.fail_send = False
            outbox_now = st.tg_outbox_snapshot()
            for it in outbox_now:
                it['next_attempt_ts'] = 0.0
            st.tg_outbox_replace(items=outbox_now)
            api.flush_outbox(max_ops=10)

            p = root / 'topics' / '111' / '222' / 'events.jsonl'
            lines = p.read_text(encoding='utf-8').splitlines()
            items = [json.loads(x) for x in lines]
            sent = [
                x
                for x in items
                if x.get('dir') == 'out'
                and x.get('op') == 'send_message'
                and x.get('text') == 'queued'
                and x.get('deferred') is False
            ]
            self.assertTrue(sent)

    def test_semantic_mode_filters_noise(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api_raw = _FakeTelegramAPI()
            api = TelegramDeliveryAPI(
                api=api_raw,
                state=st,
                log_path=root / 'net.log',
                topic_log_root=root / 'topics',
                topic_log_max_chars=200,
                topic_log_mode='semantic',
            )

            api.send_message(chat_id=111, message_thread_id=222, text='✅ Принял.')
            api.edit_message_text(chat_id=111, message_id=999, text='⏳ Работаю… 0:10', parse_mode='HTML')
            api.send_message(chat_id=111, message_thread_id=222, text='✅ Готово. Ответ ниже.')
            api.send_message(chat_id=111, message_thread_id=222, text='result')

            p = root / 'topics' / '111' / '222' / 'events.jsonl'
            lines = p.read_text(encoding='utf-8').splitlines()
            items = [json.loads(x) for x in lines]
            texts = [
                str(x.get('text') or '')
                for x in items
                if x.get('dir') == 'out' and x.get('op') in {'send_message', 'edit_message_text'}
            ]

            self.assertTrue(any(t == 'result' for t in texts))
            self.assertFalse(any(t.startswith('✅ Принял') for t in texts))
            self.assertFalse(any('⏳ Работаю' in t for t in texts))
            self.assertFalse(any(t.startswith('✅ Готово') for t in texts))
