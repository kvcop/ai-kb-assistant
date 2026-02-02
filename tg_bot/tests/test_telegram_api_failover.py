import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from tg_bot.telegram_api import TelegramAPI


class _FakeHTTPResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> '_FakeHTTPResponse':
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


class TestTelegramAPIFailover(unittest.TestCase):
    def test_fallback_to_remote_on_local_urlerror_then_probe_back(self) -> None:
        now = [100.0]
        local_calls: list[str] = []
        remote_calls: list[str] = []

        def fake_time() -> float:
            return float(now[0])

        def fake_urlopen(req: object, timeout: int = 0) -> _FakeHTTPResponse:
            url = getattr(req, 'full_url', '')
            if url.startswith('http://local/'):
                local_calls.append(str(url))
                # First local request fails, subsequent ones succeed.
                if len(local_calls) == 1:
                    raise urllib.error.URLError('connection refused')
                return _FakeHTTPResponse(b'{"ok": true, "result": {"id": 1}}')
            if url.startswith('http://remote/'):
                remote_calls.append(str(url))
                return _FakeHTTPResponse(b'{"ok": true, "result": {"id": 2}}')
            raise AssertionError(f'unexpected url: {url}')

        api = TelegramAPI(
            token='t',
            local_root_url='http://local',
            remote_root_url='http://remote',
            prefer_local=True,
            local_probe_seconds=300,
        )

        with (
            patch('tg_bot.telegram_api.time.time', fake_time),
            patch('tg_bot.telegram_api.urllib.request.urlopen', fake_urlopen),
        ):
            # 1) Starts on local, local fails => retries on remote.
            me = api.get_me()
            self.assertEqual(me.get('result', {}).get('id'), 2)
            self.assertEqual(len(local_calls), 1)
            self.assertEqual(len(remote_calls), 1)

            # 2) Before probe interval, stays on remote (no local calls).
            now[0] = 200.0
            me2 = api.get_me()
            self.assertEqual(me2.get('result', {}).get('id'), 2)
            self.assertEqual(len(local_calls), 1)
            self.assertEqual(len(remote_calls), 2)

            # 3) After probe interval, probes local and switches back.
            now[0] = 400.0
            me3 = api.get_me()
            self.assertEqual(me3.get('result', {}).get('id'), 1)
            self.assertGreaterEqual(len(local_calls), 2)

    def test_download_file_to_accepts_absolute_local_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / 'src.bin'
            dst = root / 'dst.bin'
            data = b'hello'
            src.write_bytes(data)

            api = TelegramAPI(token='t')
            api.download_file_to(file_path=str(src), dest_path=dst, max_bytes=10 * 1024 * 1024)
            self.assertEqual(dst.read_bytes(), data)
