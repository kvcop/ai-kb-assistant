import tempfile
import unittest
from pathlib import Path

from tg_bot.state import BotState
from tg_bot.telegram_api import TelegramDeliveryAPI


class _NoopAPI:
    pass


class TestTelegramDeliveryRetryable(unittest.TestCase):
    def test_retryable_http_errors_and_network(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = TelegramDeliveryAPI(api=_NoopAPI(), state=st, log_path=root / 'net.log')  # type: ignore[arg-type]

            self.assertTrue(
                api._is_retryable_error(
                    RuntimeError('Telegram URLError: [Errno -3] Temporary failure in name resolution')
                )
            )
            self.assertTrue(api._is_retryable_error(RuntimeError('Telegram HTTPError 429: Too Many Requests')))
            self.assertTrue(api._is_retryable_error(RuntimeError('Telegram HTTPError 500: Internal Server Error')))
            self.assertTrue(api._is_retryable_error(RuntimeError('Telegram HTTPError 503: Service Unavailable')))
            self.assertTrue(api._is_retryable_error(RuntimeError('The read operation timed out')))
            self.assertTrue(api._is_retryable_error(RuntimeError('Temporary failure in name resolution')))
            self.assertTrue(api._is_retryable_error(RuntimeError('Remote end closed connection without response')))

    def test_non_retryable_http_4xx(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_path = root / 'state.json'
            state_path.write_text('{}', encoding='utf-8')
            st = BotState(path=state_path)
            st.load()

            api = TelegramDeliveryAPI(api=_NoopAPI(), state=st, log_path=root / 'net.log')  # type: ignore[arg-type]

            self.assertFalse(api._is_retryable_error(RuntimeError('Telegram HTTPError 400: Bad Request')))
            self.assertFalse(api._is_retryable_error(RuntimeError('Telegram HTTPError 403: Forbidden')))
            self.assertFalse(
                api._is_retryable_error(RuntimeError("Telegram API error: {'ok': False, 'error_code': 400}"))
            )
