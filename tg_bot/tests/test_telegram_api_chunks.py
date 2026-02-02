import unittest

from tg_bot.telegram_api import TelegramAPI


class _CapturingTelegramAPI(TelegramAPI):
    def __init__(self) -> None:
        super().__init__(token='x')
        object.__setattr__(self, 'calls', [])

    def send_message(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))  # type: ignore[attr-defined]
        return {'ok': True, 'result': {'message_id': len(self.calls)}}  # type: ignore[attr-defined]


class TestTelegramAPIChunks(unittest.TestCase):
    def test_send_chunks_single_message_when_small(self) -> None:
        api = _CapturingTelegramAPI()
        api.send_chunks(
            chat_id=1,
            message_thread_id=2,
            text='hello',
            chunk_size=10,
            parse_mode='HTML',
            reply_markup={'inline_keyboard': [[{'text': 'x', 'callback_data': 'y'}]]},
            reply_to_message_id=123,
        )

        self.assertEqual(len(api.calls), 1)
        self.assertEqual(api.calls[0].get('chat_id'), 1)
        self.assertEqual(api.calls[0].get('message_thread_id'), 2)
        self.assertEqual(api.calls[0].get('text'), 'hello')
        self.assertEqual(api.calls[0].get('parse_mode'), 'HTML')
        self.assertEqual(api.calls[0].get('reply_to_message_id'), 123)
        self.assertIsNotNone(api.calls[0].get('reply_markup'))

    def test_send_chunks_splits_and_attaches_markup_only_to_last(self) -> None:
        api = _CapturingTelegramAPI()
        text = 'a' * 8000
        markup = {'inline_keyboard': [[{'text': 'ok', 'callback_data': 'cb'}]]}
        api.send_chunks(
            chat_id=1,
            message_thread_id=2,
            text=text,
            chunk_size=3900,
            parse_mode='HTML',
            reply_markup=markup,
            reply_to_message_id=123,
        )

        self.assertEqual(len(api.calls), 3)
        parts = [str(c.get('text') or '') for c in api.calls]
        self.assertEqual([len(p) for p in parts], [3900, 3900, 200])
        self.assertEqual(''.join(parts), text)

        self.assertIsNone(api.calls[0].get('reply_markup'))
        self.assertIsNone(api.calls[1].get('reply_markup'))
        self.assertEqual(api.calls[2].get('reply_markup'), markup)
