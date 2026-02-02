from __future__ import annotations

import json
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from hashlib import sha1
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from .state import BotState


@dataclass
class _TelegramEndpointState:
    active: str = 'local'  # local | remote
    next_probe_ts: float = 0.0
    lock: Lock = field(default_factory=Lock, repr=False, compare=False)


@dataclass(frozen=True)
class TelegramAPI:
    token: str
    local_root_url: str = 'http://127.0.0.1:8081'
    remote_root_url: str = 'https://api.telegram.org'
    prefer_local: bool = True
    local_probe_seconds: int = 300
    log_path: Path | None = None

    _endpoint: _TelegramEndpointState = field(
        default_factory=_TelegramEndpointState, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, 'local_root_url', self._normalize_root_url(self.local_root_url))
        object.__setattr__(self, 'remote_root_url', self._normalize_root_url(self.remote_root_url))

        with self._endpoint.lock:
            if self.prefer_local and self.local_root_url:
                self._endpoint.active = 'local'
            else:
                self._endpoint.active = 'remote'

    @property
    def base_url(self) -> str:
        return self._api_base_url_for_root(self._active_root_url())

    def _log_endpoint(self, msg: str) -> None:
        if not msg:
            return
        line = f'[tg-bot-api] {msg}'
        try:
            print(line, flush=True)
        except Exception:
            pass
        if self.log_path is None:
            return
        try:
            ts = time.strftime('%Y-%m-%d %H:%M:%S')
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            with self.log_path.open('a', encoding='utf-8') as f:
                f.write(f'[{ts}] {line}\n')
        except Exception:
            pass

    def _normalize_root_url(self, url: str) -> str:
        s = (url or '').strip()
        return s.rstrip('/')

    def _api_base_url_for_root(self, root_url: str) -> str:
        root = self._normalize_root_url(root_url)
        if not root:
            raise RuntimeError('Telegram API base URL is empty')
        return f'{root}/bot{self.token}/'

    def _file_base_url_for_root(self, root_url: str) -> str:
        root = self._normalize_root_url(root_url)
        if not root:
            raise RuntimeError('Telegram file base URL is empty')
        return f'{root}/file/bot{self.token}/'

    def _active_root_url(self) -> str:
        with self._endpoint.lock:
            active = self._endpoint.active
        if active == 'local' and self.local_root_url:
            return self.local_root_url
        return self.remote_root_url

    def _mark_local_dead(self, *, reason: str) -> None:
        if not (self.prefer_local and self.local_root_url):
            return
        probe_in = max(60, int(self.local_probe_seconds or 0))
        now = time.time()
        switched = False
        with self._endpoint.lock:
            if self._endpoint.active == 'local':
                self._endpoint.active = 'remote'
                self._endpoint.next_probe_ts = now + probe_in
                switched = True
        if switched:
            self._log_endpoint(
                f'Local Bot API unavailable ({reason}); switched to remote; will probe again in {probe_in}s'
            )

    def _maybe_probe_local(self) -> None:
        if not (self.prefer_local and self.local_root_url):
            return
        probe_in = max(60, int(self.local_probe_seconds or 0))
        now = time.time()

        should_probe = False
        with self._endpoint.lock:
            if self._endpoint.active == 'remote' and now >= float(self._endpoint.next_probe_ts or 0.0):
                self._endpoint.next_probe_ts = now + probe_in
                should_probe = True

        if not should_probe:
            return

        self._log_endpoint(f'Probing local Bot API at {self.local_root_url}...')
        try:
            self._request_json_once(
                base_url=self._api_base_url_for_root(self.local_root_url),
                method='getMe',
                params={},
                timeout=5,
            )
        except Exception as e:
            self._log_endpoint(f'Local Bot API probe failed: {e}')
            return

        switched = False
        with self._endpoint.lock:
            if self._endpoint.active != 'local':
                self._endpoint.active = 'local'
                self._endpoint.next_probe_ts = 0.0
                switched = True
        if switched:
            self._log_endpoint('Local Bot API is back; switched to local')

    def _request_json_once(self, *, base_url: str, method: str, params: dict[str, Any], timeout: int) -> dict[str, Any]:
        url = base_url + method
        data = None
        headers = {'Content-Type': 'application/json; charset=utf-8'}

        # Telegram accepts JSON POST for most methods; for getUpdates we prefer GET.
        if method == 'getUpdates':
            query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            if query:
                url = url + '?' + query
            req = urllib.request.Request(url, method='GET', headers=headers)
        else:
            payload = json.dumps(params, ensure_ascii=False).encode('utf-8')
            data = payload
            req = urllib.request.Request(url, data=data, method='POST', headers=headers)

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            try:
                raw = e.read().decode('utf-8', errors='replace')
            except Exception:
                raw = str(e)
            raise RuntimeError(f'Telegram HTTPError {e.code}: {raw}') from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            raise RuntimeError(f'Telegram URLError: {e}') from e

        try:
            obj_raw = json.loads(raw or '{}')
        except json.JSONDecodeError as e:
            raise RuntimeError(f'Telegram invalid JSON: {raw[:500]}') from e

        if not isinstance(obj_raw, dict):
            raise RuntimeError(f'Telegram invalid JSON (not an object): {raw[:500]}')

        obj: dict[str, Any] = obj_raw

        if not obj.get('ok', False):
            raise RuntimeError(f'Telegram API error: {obj}')

        return obj

    def _request_json(self, method: str, params: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
        if params is None:
            params = {}

        self._maybe_probe_local()

        base_url = self.base_url
        try:
            return self._request_json_once(base_url=base_url, method=method, params=params, timeout=timeout)
        except RuntimeError as e:
            if (
                self.local_root_url
                and self.prefer_local
                and base_url.startswith(self._api_base_url_for_root(self.local_root_url))
            ):
                if str(e).startswith('Telegram URLError:'):
                    self._mark_local_dead(reason=str(e))
                    base_url = self._api_base_url_for_root(self.remote_root_url)
                    return self._request_json_once(base_url=base_url, method=method, params=params, timeout=timeout)
            raise

    def _request_multipart(
        self,
        method: str,
        *,
        fields: dict[str, Any] | None = None,
        files: dict[str, tuple[str, bytes]] | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        self._maybe_probe_local()

        base_url = self.base_url
        try:
            return self._request_multipart_once(
                base_url=base_url, method=method, fields=fields, files=files, timeout=timeout
            )
        except RuntimeError as e:
            if (
                self.local_root_url
                and self.prefer_local
                and base_url.startswith(self._api_base_url_for_root(self.local_root_url))
            ):
                if str(e).startswith('Telegram URLError:'):
                    self._mark_local_dead(reason=str(e))
                    base_url = self._api_base_url_for_root(self.remote_root_url)
                    return self._request_multipart_once(
                        base_url=base_url, method=method, fields=fields, files=files, timeout=timeout
                    )
            raise

    def _request_multipart_once(
        self,
        *,
        base_url: str,
        method: str,
        fields: dict[str, Any] | None = None,
        files: dict[str, tuple[str, bytes]] | None = None,
        timeout: int = 60,
    ) -> dict[str, Any]:
        url = base_url + method
        fields = dict(fields or {})
        files = dict(files or {})

        boundary = '----tg-bot-' + uuid4().hex
        body = bytearray()

        def _add(s: str) -> None:
            body.extend(s.encode('utf-8'))

        def _field(name: str, value: object) -> None:
            if value is None:
                return
            _add(f'--{boundary}\r\n')
            _add(f'Content-Disposition: form-data; name="{name}"\r\n\r\n')
            if isinstance(value, (dict, list)):
                _add(json.dumps(value, ensure_ascii=False))
            else:
                _add(str(value))
            _add('\r\n')

        for k, v in fields.items():
            key = str(k or '').strip()
            if not key:
                continue
            _field(key, v)

        for form_field, (filename, content) in files.items():
            name = str(form_field or '').strip()
            if not name:
                continue
            fname = str(filename or 'file').replace('"', '').strip() or 'file'
            payload = bytes(content or b'')
            _add(f'--{boundary}\r\n')
            _add(f'Content-Disposition: form-data; name="{name}"; filename="{fname}"\r\n')
            _add('Content-Type: application/octet-stream\r\n\r\n')
            body.extend(payload)
            _add('\r\n')

        _add(f'--{boundary}--\r\n')

        headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}
        req = urllib.request.Request(url, data=bytes(body), method='POST', headers=headers)

        try:
            with urllib.request.urlopen(req, timeout=int(timeout)) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            try:
                raw = e.read().decode('utf-8', errors='replace')
            except Exception:
                raw = str(e)
            raise RuntimeError(f'Telegram HTTPError {e.code}: {raw}') from e
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            raise RuntimeError(f'Telegram URLError: {e}') from e

        try:
            obj_raw = json.loads(raw or '{}')
        except json.JSONDecodeError as e:
            raise RuntimeError(f'Telegram invalid JSON: {raw[:500]}') from e

        if not isinstance(obj_raw, dict):
            raise RuntimeError(f'Telegram invalid JSON (not an object): {raw[:500]}')

        obj: dict[str, Any] = obj_raw

        if not obj.get('ok', False):
            raise RuntimeError(f'Telegram API error: {obj}')

        return obj

    def get_updates(self, *, offset: int | None, timeout: int, limit: int = 100) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            'timeout': int(timeout),
            'limit': int(limit),
        }
        if offset is not None:
            params['offset'] = int(offset)
        obj = self._request_json('getUpdates', params=params, timeout=timeout + 5)
        result = obj.get('result') or []
        if not isinstance(result, list):
            return []
        return [x for x in result if isinstance(x, dict)]

    def send_message(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        text: str,
        disable_web_page_preview: bool = True,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        timeout: int = 30,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'text': text,
            'disable_web_page_preview': bool(disable_web_page_preview),
        }
        if message_thread_id is not None:
            params['message_thread_id'] = int(message_thread_id)
        if reply_to_message_id is not None:
            params['reply_to_message_id'] = int(reply_to_message_id)
        if parse_mode:
            params['parse_mode'] = str(parse_mode)
        if reply_markup is not None:
            params['reply_markup'] = reply_markup
        return self._request_json('sendMessage', params=params, timeout=int(timeout))

    def send_document(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        document_path: str | Path,
        filename: str | None = None,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        timeout: int = 120,
        max_bytes: int = 50 * 1024 * 1024,
    ) -> dict[str, Any]:
        p = Path(document_path).expanduser()
        if not p.exists() or not p.is_file():
            raise RuntimeError(f'Telegram send_document: file not found: {p}')
        if max_bytes > 0:
            try:
                size = int(p.stat().st_size)
            except Exception:
                size = 0
            if size > 0 and size > int(max_bytes):
                raise RuntimeError(f'Telegram send_document: file too large ({size} bytes > {int(max_bytes)})')

        data = p.read_bytes()
        fields: dict[str, Any] = {'chat_id': int(chat_id)}
        if message_thread_id is not None:
            fields['message_thread_id'] = int(message_thread_id)
        if caption:
            fields['caption'] = str(caption)
        if reply_to_message_id is not None:
            fields['reply_to_message_id'] = int(reply_to_message_id)
        if parse_mode:
            fields['parse_mode'] = str(parse_mode)
        if reply_markup is not None:
            fields['reply_markup'] = json.dumps(reply_markup, ensure_ascii=False)

        fname = str(filename or p.name).strip() or p.name
        return self._request_multipart(
            'sendDocument',
            fields=fields,
            files={'document': (fname, data)},
            timeout=int(timeout),
        )

    def send_media_group_documents(
        self,
        *,
        chat_id: int,
        document_paths: list[str | Path],
        caption: str | None = None,
        message_thread_id: int | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
        timeout: int = 180,
        max_bytes: int = 50 * 1024 * 1024,
    ) -> dict[str, Any]:
        """Send up to 10 documents as a single media group (album).

        Caption is applied only to the first document (Telegram limitation).
        """
        if not document_paths:
            raise RuntimeError('Telegram send_media_group_documents: no files')
        if len(document_paths) > 10:
            raise RuntimeError('Telegram send_media_group_documents: too many files (> 10)')

        files: dict[str, tuple[str, bytes]] = {}
        media: list[dict[str, Any]] = []

        for idx, doc_path in enumerate(document_paths):
            p = Path(doc_path).expanduser()
            if not p.exists() or not p.is_file():
                raise RuntimeError(f'Telegram send_media_group_documents: file not found: {p}')
            if max_bytes > 0:
                try:
                    size = int(p.stat().st_size)
                except Exception:
                    size = 0
                if size > 0 and size > int(max_bytes):
                    raise RuntimeError(
                        f'Telegram send_media_group_documents: file too large ({size} bytes > {int(max_bytes)})'
                    )

            data = p.read_bytes()
            field = f'file{idx}'
            files[field] = (p.name, data)

            item: dict[str, Any] = {'type': 'document', 'media': f'attach://{field}'}
            if idx == 0 and caption:
                item['caption'] = str(caption)
                if parse_mode:
                    item['parse_mode'] = str(parse_mode)
            media.append(item)

        fields: dict[str, Any] = {
            'chat_id': int(chat_id),
            'media': media,
        }
        if message_thread_id is not None:
            fields['message_thread_id'] = int(message_thread_id)
        if reply_to_message_id is not None:
            # Deprecated in Bot API, but still accepted; keep it simple.
            fields['reply_to_message_id'] = int(reply_to_message_id)

        return self._request_multipart(
            'sendMediaGroup',
            fields=fields,
            files=files,
            timeout=int(timeout),
        )

    def send_chunks(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        text: str,
        chunk_size: int = 3900,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        reply_to_message_id: int | None = None,
    ) -> None:
        # Telegram limit is 4096; leave margin.
        text = text or ''
        if len(text) <= chunk_size:
            self.send_message(
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            return
        start = 0
        while start < len(text):
            part = text[start : start + chunk_size]
            # Attach markup only to the last chunk (so the buttons stay near the bottom).
            self.send_message(
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                text=part,
                parse_mode=parse_mode,
                reply_markup=reply_markup if (start + chunk_size) >= len(text) else None,
                reply_to_message_id=reply_to_message_id,
            )
            start += chunk_size

    def answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None = None,
        show_alert: bool = False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'callback_query_id': str(callback_query_id),
            'show_alert': bool(show_alert),
        }
        if text:
            params['text'] = str(text)
        return self._request_json('answerCallbackQuery', params=params, timeout=15)

    def edit_message_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_id': int(message_id),
            'reply_markup': reply_markup or {},
        }
        return self._request_json('editMessageReplyMarkup', params=params, timeout=20)

    def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        disable_web_page_preview: bool = True,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_id': int(message_id),
            'text': text,
            'disable_web_page_preview': bool(disable_web_page_preview),
        }
        if parse_mode:
            params['parse_mode'] = str(parse_mode)
        if reply_markup is not None:
            params['reply_markup'] = reply_markup
        return self._request_json('editMessageText', params=params, timeout=30)

    def edit_forum_topic(
        self,
        *,
        chat_id: int,
        message_thread_id: int,
        name: str,
        timeout: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_thread_id': int(message_thread_id),
            'name': str(name),
        }
        return self._request_json('editForumTopic', params=params, timeout=int(timeout))

    def delete_message(self, *, chat_id: int, message_id: int) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_id': int(message_id),
        }
        return self._request_json('deleteMessage', params=params, timeout=20)

    def send_chat_action(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        action: str = 'typing',
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'action': str(action),
        }
        if message_thread_id is not None:
            params['message_thread_id'] = int(message_thread_id)
        return self._request_json('sendChatAction', params=params, timeout=10)

    def get_me(self) -> dict[str, Any]:
        return self._request_json('getMe', params={}, timeout=20)

    def get_file(self, *, file_id: str, timeout: int = 30) -> dict[str, Any]:
        params: dict[str, Any] = {'file_id': str(file_id)}
        obj = self._request_json('getFile', params=params, timeout=int(timeout))
        result = obj.get('result') or {}
        if not isinstance(result, dict):
            raise RuntimeError(f'Telegram getFile invalid response: {obj!r}')
        return result

    def download_file_to(
        self,
        *,
        file_path: str,
        dest_path: Path,
        timeout: int = 60,
        max_bytes: int = 50 * 1024 * 1024,
    ) -> None:
        """Download a file from Telegram to dest_path (atomic write).

        Use get_file() first to resolve file_path from file_id.
        """
        file_path = str(file_path or '').strip()
        if not file_path:
            raise RuntimeError('Telegram download_file_to: empty file_path')

        p_local = Path(file_path)
        if p_local.is_absolute():
            if not p_local.exists() or not p_local.is_file():
                raise RuntimeError(f'Telegram download_file_to: local file not found: {p_local}')
            try:
                size = int(p_local.stat().st_size)
            except Exception:
                size = 0
            if max_bytes > 0 and size > 0 and size > int(max_bytes):
                raise RuntimeError(f'Telegram download_file_to: file too large ({size} bytes)')

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest_path.with_suffix(dest_path.suffix + '.tmp')
            try:
                with p_local.open('rb') as src, tmp.open('wb') as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
            except Exception:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                raise
            os.replace(tmp, dest_path)
            return

        self._maybe_probe_local()
        active_root = self._active_root_url()
        rel_path = file_path.lstrip('/')
        url = self._file_base_url_for_root(active_root) + rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest_path.with_suffix(dest_path.suffix + '.tmp')

        req = urllib.request.Request(url, method='GET')
        total = 0
        try:
            with urllib.request.urlopen(req, timeout=int(timeout)) as resp, tmp.open('wb') as f:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if max_bytes > 0 and total > max_bytes:
                        raise RuntimeError(f'Telegram download_file_to: file too large ({total} bytes)')
                    f.write(chunk)
        except Exception as e:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            if self.local_root_url and self.prefer_local and active_root == self.local_root_url:
                self._mark_local_dead(reason=str(e))
                active_root = self.remote_root_url
                url = self._file_base_url_for_root(active_root) + rel_path
                req = urllib.request.Request(url, method='GET')
                total = 0
                try:
                    with urllib.request.urlopen(req, timeout=int(timeout)) as resp, tmp.open('wb') as f:
                        while True:
                            chunk = resp.read(64 * 1024)
                            if not chunk:
                                break
                            total += len(chunk)
                            if max_bytes > 0 and total > max_bytes:
                                raise RuntimeError(f'Telegram download_file_to: file too large ({total} bytes)')
                            f.write(chunk)
                except Exception:
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
                    raise
            else:
                raise

        os.replace(tmp, dest_path)


class TelegramDeliveryAPI:
    """TelegramAPI wrapper that persists failed sends/edits and retries with backoff.

    Intended to survive VPN/DNS outages: we queue outgoing operations and replay them once Telegram is reachable again.
    """

    _HTTP_ERR_RE = re.compile(r'Telegram HTTPError\s+(\d+):')
    _MM_OTP_RE = re.compile(r'(?is)^\s*/mm-otp\b')
    _SENSITIVE_KV_RE = re.compile(r'(?m)(?i)\b([A-Z0-9_]*(?:TOKEN|PASSWORD|SECRET)[A-Z0-9_]*)\s*=\s*([^\s]+)')
    _BEARER_RE = re.compile(r'(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{10,}')
    _TOPIC_LOG_NOISE_PREFIXES: tuple[str, ...] = (
        '✅ принял',
        '🔄 принял',
        '💬 принял',
        '🎙️ принял',
        '📎 файлы сохранил',
        '🕓 перезапускаюсь',
        '🕓 уже перезапускаюсь',
        '🕓 жду завершения',
    )

    def __init__(
        self,
        *,
        api: TelegramAPI,
        state: BotState,
        log_path: Path,
        topic_log_root: Path | None = None,
        topic_log_max_chars: int = 2000,
        topic_log_mode: str | None = None,
        max_outbox_items: int = 500,
        backoff_base_seconds: float = 2.0,
        backoff_max_seconds: float = 300.0,
    ) -> None:
        self._api = api
        self._state = state
        self._log_path = log_path
        self._topic_log_root = topic_log_root
        self._topic_log_max_chars = max(0, int(topic_log_max_chars))
        mode_raw = str((topic_log_mode or os.getenv('TG_TOPIC_LOG_MODE') or 'semantic') or '').strip().lower()
        if mode_raw in {'all', 'full', 'debug'}:
            self._topic_log_mode = 'all'
        else:
            self._topic_log_mode = 'semantic'
        self._max_outbox_items = int(max_outbox_items)
        self._backoff_base_seconds = float(max(0.5, backoff_base_seconds))
        self._backoff_max_seconds = float(max(5.0, backoff_max_seconds))
        self._flush_lock = Lock()

    # -----------------------------
    # Delegated read methods
    # -----------------------------
    def get_updates(self, *, offset: int | None, timeout: int, limit: int = 100) -> list[dict[str, Any]]:
        return self._api.get_updates(offset=offset, timeout=timeout, limit=limit)

    def get_me(self) -> dict[str, Any]:
        return self._api.get_me()

    def get_file(self, *, file_id: str, timeout: int = 30) -> dict[str, Any]:
        return self._api.get_file(file_id=file_id, timeout=timeout)

    def download_file_to(
        self, *, file_path: str, dest_path: Path, timeout: int = 60, max_bytes: int = 50 * 1024 * 1024
    ) -> None:
        return self._api.download_file_to(
            file_path=file_path, dest_path=dest_path, timeout=timeout, max_bytes=max_bytes
        )

    # -----------------------------
    # Logging / retry policy
    # -----------------------------
    def _log(self, line: str) -> None:
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._log_path.open('a', encoding='utf-8') as f:
            f.write(f'[{ts}] {line}\n')

    def _topic_log_path(self, *, chat_id: int, message_thread_id: int | None = None) -> Path | None:
        root = self._topic_log_root
        if root is None:
            return None
        try:
            cid = int(chat_id)
        except Exception:
            cid = 0
        if cid == 0:
            return None
        try:
            tid = int(message_thread_id or 0)
        except Exception:
            tid = 0
        if tid < 0:
            tid = 0
        return root / str(int(cid)) / str(int(tid)) / 'events.jsonl'

    def _topic_log_preview_text(self, text: object, *, cmd: str | None = None) -> str:
        if not isinstance(text, str):
            return ''
        s = text.replace('\r', '').strip()

        cmd_norm = str(cmd or '').strip().casefold()
        if cmd_norm == '/mm-otp' or self._MM_OTP_RE.match(s):
            return '/mm-otp <redacted>'

        try:
            s = self._SENSITIVE_KV_RE.sub(lambda m: f'{m.group(1)}=<redacted>', s)
        except Exception:
            pass
        try:
            s = self._BEARER_RE.sub('Bearer <redacted>', s)
        except Exception:
            pass

        s = s.replace('\n', ' ').strip()
        s = ' '.join(s.split())
        max_chars = int(self._topic_log_max_chars or 0)
        if max_chars > 0 and len(s) > max_chars:
            s = s[: max(0, max_chars - 1)] + '…'
        return s

    def _topic_log_should_write(self, item: dict[str, Any]) -> bool:
        if self._topic_log_mode == 'all':
            return True

        try:
            direction = str(item.get('dir') or '').strip().lower()
        except Exception:
            direction = ''

        if direction == 'in':
            return True

        # Default: keep only chat-visible content (not internal retries/queue/debug).
        if bool(item.get('deferred', False)):
            return False

        meta = item.get('meta')
        if isinstance(meta, dict):
            kind = str(meta.get('kind') or '').strip().lower()
            if kind == 'restore_notice':
                return False

        op = str(item.get('op') or '').strip()
        if op not in {'send_message', 'edit_message_text'}:
            return True

        text = item.get('text')
        if not isinstance(text, str):
            return True
        norm = text.strip().casefold()
        if not norm:
            return True

        if '⏳ работаю' in norm:
            return False

        for pref in self._TOPIC_LOG_NOISE_PREFIXES:
            if norm.startswith(pref):
                return False

        if norm.startswith('✅ готово') and len(norm) <= 80:
            return False

        if norm.startswith('🌐 сеть была недоступна'):
            return False

        return True

    def _topic_log_append(self, *, chat_id: int, message_thread_id: int | None, item: dict[str, Any]) -> None:
        try:
            if not self._topic_log_should_write(item):
                return
        except Exception:
            return
        path = self._topic_log_path(chat_id=chat_id, message_thread_id=message_thread_id)
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except Exception:
            return

    def log_incoming_message(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None,
        chat_type: str,
        user_id: int,
        username: str,
        message_id: int,
        cmd: str,
        text: object,
        attachments: list[dict[str, object]] | None = None,
        reply_to_message_id: int | None = None,
    ) -> None:
        self._topic_log_append(
            chat_id=int(chat_id),
            message_thread_id=message_thread_id,
            item={
                'ts': float(time.time()),
                'dir': 'in',
                'chat_id': int(chat_id),
                'thread_id': int(message_thread_id or 0),
                'chat_type': str(chat_type or ''),
                'user_id': int(user_id),
                'username': str(username or '').strip().lstrip('@'),
                'message_id': int(message_id),
                'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
                'cmd': str(cmd or '').strip(),
                'text': self._topic_log_preview_text(text, cmd=cmd),
                'attachments': list(attachments) if isinstance(attachments, list) and attachments else [],
            },
        )

    def _backoff(self, attempts: int) -> float:
        # 2,4,8... up to max; then every max seconds.
        a = max(1, int(attempts))
        base = self._backoff_base_seconds * (2.0 ** float(a - 1))
        return float(min(self._backoff_max_seconds, max(0.5, base)))

    def _is_retryable_error(self, e: Exception) -> bool:
        s = str(e)
        if 'Telegram URLError' in s:
            return True
        m = self._HTTP_ERR_RE.search(s)
        if m:
            try:
                code = int(m.group(1))
            except Exception:
                code = 0
            if code == 429 or (500 <= code <= 599):
                return True
        low = s.lower()
        if 'temporary failure in name resolution' in low:
            return True
        if 'name or service not known' in low:
            return True
        if 'getaddrinfo failed' in low:
            return True
        if 'network is unreachable' in low:
            return True
        if 'remote end closed connection without response' in low:
            return True
        if 'timed out' in low:
            return True
        return False

    def _enqueue(
        self,
        *,
        chat_id: int,
        op: str,
        params: dict[str, Any],
        error: Exception,
        coalesce_key: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        attempts = 1
        item: dict[str, Any] = {
            'id': uuid4().hex,
            'op': str(op or '').strip()[:32],
            'chat_id': int(chat_id),
            'params': dict(params),
            'created_ts': float(now),
            'attempts': int(attempts),
            'next_attempt_ts': float(now + self._backoff(attempts)),
            'last_error': str(error)[:400],
        }
        if coalesce_key:
            item['coalesce_key'] = str(coalesce_key).strip()[:64]
        if isinstance(meta, dict) and meta:
            try:
                item['meta'] = dict(meta)
            except Exception:
                pass
        self._state.tg_outbox_enqueue(item=item, max_items=self._max_outbox_items)
        self._state.tg_mark_offline(chat_id=int(chat_id), ts=now)
        self._log(f'[outbox] enqueue chat_id={int(chat_id)} op={op} err={str(error)[:200]}')
        return {'ok': False, 'deferred': True, 'error': str(error)[:400]}

    # -----------------------------
    # Outgoing methods (queue on network issues)
    # -----------------------------
    def send_message(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        text: str,
        disable_web_page_preview: bool = True,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        coalesce_key: str | None = None,
        meta: dict[str, Any] | None = None,
        timeout: int = 30,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_thread_id': int(message_thread_id) if message_thread_id is not None else None,
            'text': text,
            'disable_web_page_preview': bool(disable_web_page_preview),
            'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
            'parse_mode': str(parse_mode) if parse_mode else None,
            'reply_markup': reply_markup,
            'timeout': int(timeout),
        }
        try:
            resp = self._api.send_message(
                chat_id=int(chat_id),
                message_thread_id=message_thread_id,
                text=text,
                disable_web_page_preview=disable_web_page_preview,
                reply_to_message_id=reply_to_message_id,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                timeout=timeout,
            )
            if coalesce_key:
                try:
                    msg_id = int(((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id') or 0)
                except Exception:
                    msg_id = 0
                if msg_id > 0:
                    self._state.tg_bind_message_id_for_coalesce_key(
                        chat_id=int(chat_id), coalesce_key=str(coalesce_key), message_id=int(msg_id)
                    )
            try:
                result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                self._topic_log_append(
                    chat_id=int(chat_id),
                    message_thread_id=(int(tid) if tid > 0 else message_thread_id),
                    item={
                        'ts': float(time.time()),
                        'dir': 'out',
                        'op': 'send_message',
                        'chat_id': int(chat_id),
                        'thread_id': int(tid or (message_thread_id or 0)),
                        'message_id': int(((result.get('message_id') or 0) if isinstance(result, dict) else 0) or 0),
                        'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
                        'parse_mode': str(parse_mode) if parse_mode else None,
                        'text': self._topic_log_preview_text(text),
                        'deferred': False,
                        'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                    },
                )
            except Exception:
                pass
            return resp
        except Exception as e:
            if self._is_retryable_error(e):
                try:
                    self._topic_log_append(
                        chat_id=int(chat_id),
                        message_thread_id=message_thread_id,
                        item={
                            'ts': float(time.time()),
                            'dir': 'out',
                            'op': 'send_message',
                            'chat_id': int(chat_id),
                            'thread_id': int(message_thread_id or 0),
                            'reply_to_message_id': int(reply_to_message_id)
                            if reply_to_message_id is not None
                            else None,
                            'parse_mode': str(parse_mode) if parse_mode else None,
                            'text': self._topic_log_preview_text(text),
                            'deferred': True,
                            'error': str(e)[:250],
                            'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                        },
                    )
                except Exception:
                    pass
                return self._enqueue(
                    chat_id=int(chat_id),
                    op='send_message',
                    params=params,
                    error=e,
                    coalesce_key=coalesce_key,
                    meta=meta,
                )
            raise

    def send_document(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        document_path: str | Path,
        filename: str | None = None,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        timeout: int = 120,
        max_bytes: int = 50 * 1024 * 1024,
        coalesce_key: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        doc_path_s = str(document_path)
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_thread_id': int(message_thread_id) if message_thread_id is not None else None,
            'document_path': doc_path_s,
            'filename': str(filename) if filename else None,
            'caption': str(caption) if caption else None,
            'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
            'parse_mode': str(parse_mode) if parse_mode else None,
            'reply_markup': reply_markup,
            'timeout': int(timeout),
            'max_bytes': int(max_bytes),
        }
        try:
            resp = self._api.send_document(
                chat_id=int(chat_id),
                message_thread_id=message_thread_id,
                document_path=doc_path_s,
                filename=filename,
                caption=caption,
                reply_to_message_id=reply_to_message_id,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                timeout=timeout,
                max_bytes=max_bytes,
            )
            if coalesce_key:
                try:
                    msg_id = int(((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id') or 0)
                except Exception:
                    msg_id = 0
                if msg_id > 0:
                    self._state.tg_bind_message_id_for_coalesce_key(
                        chat_id=int(chat_id), coalesce_key=str(coalesce_key), message_id=int(msg_id)
                    )
            try:
                result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                self._topic_log_append(
                    chat_id=int(chat_id),
                    message_thread_id=(int(tid) if tid > 0 else message_thread_id),
                    item={
                        'ts': float(time.time()),
                        'dir': 'out',
                        'op': 'send_document',
                        'chat_id': int(chat_id),
                        'thread_id': int(tid or (message_thread_id or 0)),
                        'message_id': int(((result.get('message_id') or 0) if isinstance(result, dict) else 0) or 0),
                        'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
                        'filename': str(filename) if filename else None,
                        'document_path': str(doc_path_s),
                        'caption': self._topic_log_preview_text(caption),
                        'deferred': False,
                        'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                    },
                )
            except Exception:
                pass
            return resp
        except Exception as e:
            if self._is_retryable_error(e):
                try:
                    self._topic_log_append(
                        chat_id=int(chat_id),
                        message_thread_id=message_thread_id,
                        item={
                            'ts': float(time.time()),
                            'dir': 'out',
                            'op': 'send_document',
                            'chat_id': int(chat_id),
                            'thread_id': int(message_thread_id or 0),
                            'reply_to_message_id': int(reply_to_message_id)
                            if reply_to_message_id is not None
                            else None,
                            'filename': str(filename) if filename else None,
                            'document_path': str(doc_path_s),
                            'caption': self._topic_log_preview_text(caption),
                            'deferred': True,
                            'error': str(e)[:250],
                            'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                        },
                    )
                except Exception:
                    pass
                return self._enqueue(
                    chat_id=int(chat_id),
                    op='send_document',
                    params=params,
                    error=e,
                    coalesce_key=coalesce_key,
                    meta=meta,
                )
            raise

    def send_chunks(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        text: str,
        chunk_size: int = 3900,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        reply_to_message_id: int | None = None,
        coalesce_key: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_thread_id': int(message_thread_id) if message_thread_id is not None else None,
            'text': text,
            'chunk_size': int(chunk_size),
            'parse_mode': str(parse_mode) if parse_mode else None,
            'reply_markup': reply_markup,
            'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
        }
        try:
            self._api.send_chunks(
                chat_id=int(chat_id),
                message_thread_id=message_thread_id,
                text=text,
                chunk_size=chunk_size,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            try:
                self._topic_log_append(
                    chat_id=int(chat_id),
                    message_thread_id=message_thread_id,
                    item={
                        'ts': float(time.time()),
                        'dir': 'out',
                        'op': 'send_chunks',
                        'chat_id': int(chat_id),
                        'thread_id': int(message_thread_id or 0),
                        'reply_to_message_id': int(reply_to_message_id) if reply_to_message_id is not None else None,
                        'parse_mode': str(parse_mode) if parse_mode else None,
                        'chunk_size': int(chunk_size),
                        'text': self._topic_log_preview_text(text),
                        'deferred': False,
                        'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                    },
                )
            except Exception:
                pass
            return {'ok': True, 'deferred': False}
        except Exception as e:
            if self._is_retryable_error(e):
                try:
                    self._topic_log_append(
                        chat_id=int(chat_id),
                        message_thread_id=message_thread_id,
                        item={
                            'ts': float(time.time()),
                            'dir': 'out',
                            'op': 'send_chunks',
                            'chat_id': int(chat_id),
                            'thread_id': int(message_thread_id or 0),
                            'reply_to_message_id': int(reply_to_message_id)
                            if reply_to_message_id is not None
                            else None,
                            'parse_mode': str(parse_mode) if parse_mode else None,
                            'chunk_size': int(chunk_size),
                            'text': self._topic_log_preview_text(text),
                            'deferred': True,
                            'error': str(e)[:250],
                            'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                        },
                    )
                except Exception:
                    pass
                return self._enqueue(
                    chat_id=int(chat_id),
                    op='send_chunks',
                    params=params,
                    error=e,
                    coalesce_key=coalesce_key,
                    meta=meta,
                )
            raise

    def edit_message_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_id': int(message_id),
            'reply_markup': reply_markup,
        }
        try:
            return self._api.edit_message_reply_markup(
                chat_id=int(chat_id), message_id=int(message_id), reply_markup=reply_markup
            )
        except Exception as e:
            if self._is_retryable_error(e):
                ck = f'edit_message_reply_markup:{int(message_id)}'
                return self._enqueue(
                    chat_id=int(chat_id), op='edit_message_reply_markup', params=params, error=e, coalesce_key=ck
                )
            raise

    def edit_message_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        disable_web_page_preview: bool = True,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_id': int(message_id),
            'text': text,
            'disable_web_page_preview': bool(disable_web_page_preview),
            'parse_mode': str(parse_mode) if parse_mode else None,
            'reply_markup': reply_markup,
        }
        try:
            resp = self._api.edit_message_text(
                chat_id=int(chat_id),
                message_id=int(message_id),
                text=text,
                disable_web_page_preview=disable_web_page_preview,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
            try:
                result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                self._topic_log_append(
                    chat_id=int(chat_id),
                    message_thread_id=(int(tid) if tid > 0 else None),
                    item={
                        'ts': float(time.time()),
                        'dir': 'out',
                        'op': 'edit_message_text',
                        'chat_id': int(chat_id),
                        'thread_id': int(tid),
                        'message_id': int(message_id),
                        'parse_mode': str(parse_mode) if parse_mode else None,
                        'text': self._topic_log_preview_text(text),
                        'deferred': False,
                    },
                )
            except Exception:
                pass
            return resp
        except Exception as e:
            if self._is_retryable_error(e):
                ck = f'edit_message_text:{int(message_id)}'
                try:
                    self._topic_log_append(
                        chat_id=int(chat_id),
                        message_thread_id=None,
                        item={
                            'ts': float(time.time()),
                            'dir': 'out',
                            'op': 'edit_message_text',
                            'chat_id': int(chat_id),
                            'thread_id': 0,
                            'message_id': int(message_id),
                            'parse_mode': str(parse_mode) if parse_mode else None,
                            'text': self._topic_log_preview_text(text),
                            'deferred': True,
                            'error': str(e)[:250],
                        },
                    )
                except Exception:
                    pass
                return self._enqueue(
                    chat_id=int(chat_id), op='edit_message_text', params=params, error=e, coalesce_key=ck
                )
            raise

    def edit_forum_topic(
        self,
        *,
        chat_id: int,
        message_thread_id: int,
        name: str,
        timeout: int = 20,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_thread_id': int(message_thread_id),
            'name': str(name),
            'timeout': int(timeout),
        }
        try:
            return self._api.edit_forum_topic(
                chat_id=int(chat_id),
                message_thread_id=int(message_thread_id),
                name=str(name),
                timeout=int(timeout),
            )
        except Exception as e:
            if self._is_retryable_error(e):
                ck = f'forum_topic:{int(message_thread_id)}'
                return self._enqueue(
                    chat_id=int(chat_id), op='edit_forum_topic', params=params, error=e, coalesce_key=ck
                )
            raise

    def edit_message_text_by_coalesce_key(
        self,
        *,
        chat_id: int,
        coalesce_key: str,
        text: str,
        disable_web_page_preview: bool = True,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Edit a message whose message_id is not known yet (deferred send), identified by coalesce_key.

        This is useful for progress/ack messages created via `send_message(..., coalesce_key=...)` when the
        initial send was deferred into the outbox.
        """
        key = str(coalesce_key or '').strip()[:64]
        if not key:
            return {'ok': False, 'deferred': False, 'error': 'empty coalesce_key'}

        mid = self._state.tg_message_id_for_coalesce_key(chat_id=int(chat_id), coalesce_key=key)
        if int(mid) > 0:
            return self.edit_message_text(
                chat_id=int(chat_id),
                message_id=int(mid),
                text=text,
                disable_web_page_preview=disable_web_page_preview,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )

        # Not bound yet: persist desired state to disk and retry later.
        now = time.time()
        item_id = uuid4().hex
        ck_hash = sha1(key.encode('utf-8')).hexdigest()[:16]
        item: dict[str, Any] = {
            'id': item_id,
            'op': 'edit_message_text_by_key',
            'chat_id': int(chat_id),
            'params': {
                'chat_id': int(chat_id),
                'coalesce_key': key,
                'text': text,
                'disable_web_page_preview': bool(disable_web_page_preview),
                'parse_mode': str(parse_mode) if parse_mode else None,
                'reply_markup': reply_markup,
            },
            'created_ts': float(now),
            'attempts': 0,
            'next_attempt_ts': float(now + 1.0),
            'last_error': 'waiting for message_id',
            'coalesce_key': f'edit_key:{ck_hash}',
        }
        self._state.tg_outbox_enqueue(item=item, max_items=self._max_outbox_items)
        return {'ok': False, 'deferred': True, 'error': 'waiting for message_id'}

    def schedule_delete_message_by_coalesce_key(
        self,
        *,
        chat_id: int,
        coalesce_key: str,
        delete_after_seconds: int = 0,
    ) -> dict[str, Any]:
        """Schedule deletion of a message referenced by coalesce_key.

        Unlike a Timer, this survives bot restarts and temporary network outages, since it is persisted
        into the Telegram outbox and resolved to message_id when available.
        """
        try:
            cid = int(chat_id)
        except Exception:
            return {'ok': False, 'deferred': False, 'error': 'invalid chat_id'}
        if cid == 0:
            return {'ok': False, 'deferred': False, 'error': 'invalid chat_id'}
        key = str(coalesce_key or '').strip()[:64]
        if not key:
            return {'ok': False, 'deferred': False, 'error': 'empty coalesce_key'}
        try:
            delay = int(delete_after_seconds or 0)
        except Exception:
            delay = 0
        delay = max(0, delay)
        now = time.time()

        item_id = uuid4().hex
        ck_hash = sha1(key.encode('utf-8')).hexdigest()[:16]
        item: dict[str, Any] = {
            'id': item_id,
            'op': 'delete_message_by_key',
            'chat_id': int(cid),
            'params': {'chat_id': int(cid), 'coalesce_key': key},
            'created_ts': float(now),
            'attempts': 0,
            'next_attempt_ts': float(now + float(delay)),
            'last_error': '',
            'coalesce_key': f'delete_key:{ck_hash}',
        }
        self._state.tg_outbox_enqueue(item=item, max_items=self._max_outbox_items)
        return {'ok': True, 'deferred': True}

    def delete_message(self, *, chat_id: int, message_id: int) -> dict[str, Any]:
        params: dict[str, Any] = {
            'chat_id': int(chat_id),
            'message_id': int(message_id),
        }
        try:
            return self._api.delete_message(chat_id=int(chat_id), message_id=int(message_id))
        except Exception as e:
            if self._is_retryable_error(e):
                ck = f'delete_message:{int(message_id)}'
                return self._enqueue(chat_id=int(chat_id), op='delete_message', params=params, error=e, coalesce_key=ck)
            raise

    def answer_callback_query(
        self, *, callback_query_id: str, text: str | None = None, show_alert: bool = False
    ) -> dict[str, Any]:
        # Callback queries are ephemeral; retrying them later doesn't help much.
        try:
            return self._api.answer_callback_query(
                callback_query_id=callback_query_id, text=text, show_alert=show_alert
            )
        except Exception as e:
            if self._is_retryable_error(e):
                self._log(f'[tg] answer_callback_query failed err={str(e)[:200]}')
                return {'ok': False, 'deferred': True, 'error': str(e)[:400]}
            raise

    def send_chat_action(
        self,
        *,
        chat_id: int,
        message_thread_id: int | None = None,
        action: str = 'typing',
    ) -> dict[str, Any]:
        # Typing is best-effort; don't queue.
        try:
            return self._api.send_chat_action(chat_id=int(chat_id), message_thread_id=message_thread_id, action=action)
        except Exception as e:
            if self._is_retryable_error(e):
                return {'ok': False, 'deferred': True, 'error': str(e)[:200]}
            raise

    # -----------------------------
    # Flush queued operations
    # -----------------------------
    def flush_outbox(self, *, max_ops: int = 20) -> int:
        if not self._flush_lock.acquire(blocking=False):
            return 0
        try:
            now = time.time()
            outbox = self._state.tg_outbox_snapshot()
            if not outbox:
                return 0

            outbox.sort(key=lambda x: (float(x.get('next_attempt_ts') or 0.0), float(x.get('created_ts') or 0.0)))

            # Restore notices are meant to accompany re-sent *messages* after a network outage.
            # If the outbox only contains edits/deletes, the notice is noisy and misleading ("delivering messages").
            chats_with_due_send_ops: set[int] = set()
            for it in outbox:
                if not isinstance(it, dict):
                    continue
                try:
                    cid = int(it.get('chat_id') or 0)
                except Exception:
                    cid = 0
                if cid <= 0:
                    continue
                try:
                    next_ts = float(it.get('next_attempt_ts') or 0.0)
                except Exception:
                    next_ts = 0.0
                if next_ts > now:
                    continue
                op = str(it.get('op') or '').strip()
                if op in {'send_message', 'send_chunks', 'send_document'}:
                    chats_with_due_send_ops.add(cid)

            delivered = 0
            changed = False
            remaining: list[dict[str, Any]] = []
            notice_attempted: set[int] = set()

            def _fmt_age(seconds: float) -> str:
                seconds = max(0.0, float(seconds))
                if seconds < 60:
                    return f'{int(seconds)}с'
                minutes = seconds / 60.0
                if minutes < 60:
                    return f'{int(minutes)}м'
                hours = minutes / 60.0
                if hours < 48:
                    return f'{hours:.1f}ч'
                days = hours / 24.0
                return f'{days:.1f}д'

            for item in outbox:
                if delivered >= int(max_ops):
                    remaining.append(item)
                    continue

                try:
                    chat_id = int(item.get('chat_id') or 0)
                except Exception:
                    chat_id = 0
                # Telegram group/supergroup/channel ids are negative; only 0 is invalid.
                if chat_id == 0:
                    continue

                try:
                    next_ts = float(item.get('next_attempt_ts') or 0.0)
                except Exception:
                    next_ts = 0.0
                if next_ts > now:
                    remaining.append(item)
                    continue

                op = str(item.get('op') or '').strip()
                params = item.get('params') or {}
                if not isinstance(params, dict):
                    params = {}

                reminder_meta: tuple[str, list[str]] | None = None
                mm_meta: tuple[str, int] | None = None
                upload_meta: tuple[int, str] | None = None
                meta = item.get('meta')
                if isinstance(meta, dict):
                    kind = str(meta.get('kind') or '').strip()
                    date_key = str(meta.get('date_key') or '').strip()
                    ids = meta.get('reminder_ids')
                    if kind == 'reminders' and date_key and isinstance(ids, list) and ids:
                        cleaned_ids = [str(x).strip() for x in ids if str(x).strip()]
                        if cleaned_ids:
                            reminder_meta = (date_key, cleaned_ids)
                    if kind == 'mattermost':
                        channel_id = str(meta.get('channel_id') or '').strip()
                        try:
                            up_to_ts = int(meta.get('up_to_ts') or 0)
                        except Exception:
                            up_to_ts = 0
                        if channel_id and up_to_ts > 0:
                            mm_meta = (channel_id, int(up_to_ts))
                    if kind == 'upload':
                        try:
                            ack_chat_id = int(meta.get('ack_chat_id') or 0)
                        except Exception:
                            ack_chat_id = 0
                        ack_coalesce_key = str(meta.get('ack_coalesce_key') or '').strip()[:64]
                        if ack_chat_id != 0 and ack_coalesce_key:
                            upload_meta = (ack_chat_id, ack_coalesce_key)

                offline_since = self._state.tg_offline_since(chat_id=chat_id)
                notice_sent_ts = self._state.tg_offline_notice_sent_ts(chat_id=chat_id)
                should_send_notice = bool(
                    offline_since > 0
                    and notice_sent_ts <= 0
                    and chat_id not in notice_attempted
                    and chat_id in chats_with_due_send_ops
                    and chat_id > 0
                )
                if should_send_notice:
                    notice_attempted.add(chat_id)
                    try:
                        age = _fmt_age(now - float(offline_since))
                        resp0 = self._api.send_message(
                            chat_id=int(chat_id),
                            text=f'🌐 Сеть была недоступна {age}. Сейчас восстановилась — доставляю накопленные сообщения.',
                            timeout=10,
                        )
                        try:
                            result0 = (resp0.get('result') or {}) if isinstance(resp0, dict) else {}
                            tid0 = int(result0.get('message_thread_id') or 0) if isinstance(result0, dict) else 0
                            self._topic_log_append(
                                chat_id=int(chat_id),
                                message_thread_id=(int(tid0) if tid0 > 0 else None),
                                item={
                                    'ts': float(time.time()),
                                    'dir': 'out',
                                    'op': 'send_message',
                                    'chat_id': int(chat_id),
                                    'thread_id': int(tid0),
                                    'message_id': int(
                                        ((result0.get('message_id') or 0) if isinstance(result0, dict) else 0) or 0
                                    ),
                                    'text': self._topic_log_preview_text(
                                        f'🌐 Сеть была недоступна {age}. Сейчас восстановилась — доставляю накопленные сообщения.'
                                    ),
                                    'deferred': False,
                                    'meta': {'kind': 'restore_notice'},
                                },
                            )
                        except Exception:
                            pass
                        # Mark that we already notified the user for this offline epoch to avoid spam
                        # in unstable network conditions (e.g. edit retries timing out while send works).
                        self._state.tg_mark_offline_notice_sent(chat_id=int(chat_id), ts=now)
                        self._log(f'[net] restore-notice chat_id={int(chat_id)} offline={age}')
                    except Exception as e:
                        attempts = int(item.get('attempts') or 0) + 1
                        item['attempts'] = attempts
                        item['next_attempt_ts'] = float(now + self._backoff(attempts))
                        item['last_error'] = str(e)[:400]
                        remaining.append(item)
                        changed = True
                        self._log(f'[net] restore-notice failed chat_id={int(chat_id)} err={str(e)[:200]}')
                        continue

                try:
                    if op == 'send_message':
                        resp = self._api.send_message(**{k: v for k, v in params.items() if v is not None})
                        try:
                            result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                            tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                            if tid <= 0:
                                try:
                                    tid = int(params.get('message_thread_id') or 0)
                                except Exception:
                                    tid = 0
                            self._topic_log_append(
                                chat_id=int(chat_id),
                                message_thread_id=(int(tid) if tid > 0 else None),
                                item={
                                    'ts': float(time.time()),
                                    'dir': 'out',
                                    'op': 'send_message',
                                    'chat_id': int(chat_id),
                                    'thread_id': int(tid),
                                    'message_id': int(
                                        ((result.get('message_id') or 0) if isinstance(result, dict) else 0) or 0
                                    ),
                                    'reply_to_message_id': (
                                        int(params.get('reply_to_message_id') or 0)
                                        if int(params.get('reply_to_message_id') or 0) > 0
                                        else None
                                    ),
                                    'parse_mode': str(params.get('parse_mode') or '').strip() or None,
                                    'text': self._topic_log_preview_text(params.get('text')),
                                    'deferred': False,
                                    'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                                },
                            )
                        except Exception:
                            pass
                        coalesce_key = item.get('coalesce_key')
                        if coalesce_key:
                            try:
                                msg_id = int(
                                    ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id')
                                    or 0
                                )
                            except Exception:
                                msg_id = 0
                            if msg_id > 0:
                                self._state.tg_bind_message_id_for_coalesce_key(
                                    chat_id=int(chat_id),
                                    coalesce_key=str(coalesce_key),
                                    message_id=int(msg_id),
                                )
                    elif op == 'send_document':
                        call_params = {k: v for k, v in params.items() if v is not None}
                        resp = self._api.send_document(**call_params)
                        try:
                            result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                            tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                            if tid <= 0:
                                try:
                                    tid = int(params.get('message_thread_id') or 0)
                                except Exception:
                                    tid = 0
                            self._topic_log_append(
                                chat_id=int(chat_id),
                                message_thread_id=(int(tid) if tid > 0 else None),
                                item={
                                    'ts': float(time.time()),
                                    'dir': 'out',
                                    'op': 'send_document',
                                    'chat_id': int(chat_id),
                                    'thread_id': int(tid),
                                    'message_id': int(
                                        ((result.get('message_id') or 0) if isinstance(result, dict) else 0) or 0
                                    ),
                                    'reply_to_message_id': (
                                        int(params.get('reply_to_message_id') or 0)
                                        if int(params.get('reply_to_message_id') or 0) > 0
                                        else None
                                    ),
                                    'filename': str(params.get('filename') or '').strip() or None,
                                    'document_path': str(params.get('document_path') or ''),
                                    'caption': self._topic_log_preview_text(params.get('caption')),
                                    'deferred': False,
                                    'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                                },
                            )
                        except Exception:
                            pass
                        coalesce_key = item.get('coalesce_key')
                        if coalesce_key:
                            try:
                                msg_id = int(
                                    ((resp.get('result') or {}) if isinstance(resp, dict) else {}).get('message_id')
                                    or 0
                                )
                            except Exception:
                                msg_id = 0
                            if msg_id > 0:
                                self._state.tg_bind_message_id_for_coalesce_key(
                                    chat_id=int(chat_id),
                                    coalesce_key=str(coalesce_key),
                                    message_id=int(msg_id),
                                )
                    elif op == 'send_chunks':
                        self._api.send_chunks(**{k: v for k, v in params.items() if v is not None})
                        try:
                            tid = 0
                            try:
                                tid = int(params.get('message_thread_id') or 0)
                            except Exception:
                                tid = 0
                            self._topic_log_append(
                                chat_id=int(chat_id),
                                message_thread_id=(int(tid) if tid > 0 else None),
                                item={
                                    'ts': float(time.time()),
                                    'dir': 'out',
                                    'op': 'send_chunks',
                                    'chat_id': int(chat_id),
                                    'thread_id': int(tid),
                                    'reply_to_message_id': (
                                        int(params.get('reply_to_message_id') or 0)
                                        if int(params.get('reply_to_message_id') or 0) > 0
                                        else None
                                    ),
                                    'chunk_size': int(params.get('chunk_size') or 0),
                                    'parse_mode': str(params.get('parse_mode') or '').strip() or None,
                                    'text': self._topic_log_preview_text(params.get('text')),
                                    'deferred': False,
                                    'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                                },
                            )
                        except Exception:
                            pass
                    elif op == 'edit_message_text':
                        resp = self._api.edit_message_text(**{k: v for k, v in params.items() if v is not None})
                        try:
                            result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                            tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                            self._topic_log_append(
                                chat_id=int(chat_id),
                                message_thread_id=(int(tid) if tid > 0 else None),
                                item={
                                    'ts': float(time.time()),
                                    'dir': 'out',
                                    'op': 'edit_message_text',
                                    'chat_id': int(chat_id),
                                    'thread_id': int(tid),
                                    'message_id': int(params.get('message_id') or 0),
                                    'parse_mode': str(params.get('parse_mode') or '').strip() or None,
                                    'text': self._topic_log_preview_text(params.get('text')),
                                    'deferred': False,
                                    'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                                },
                            )
                        except Exception:
                            pass
                    elif op == 'edit_message_text_by_key':
                        coalesce_key = str(params.get('coalesce_key') or '').strip()
                        if not coalesce_key:
                            self._log(
                                f'[outbox] drop invalid edit_message_text_by_key (missing coalesce_key) chat_id={int(chat_id)}'
                            )
                            changed = True
                            continue

                        mid = self._state.tg_message_id_for_coalesce_key(
                            chat_id=int(chat_id), coalesce_key=coalesce_key
                        )
                        if int(mid) <= 0:
                            attempts = int(item.get('attempts') or 0) + 1
                            item['attempts'] = attempts
                            item['next_attempt_ts'] = float(now + self._backoff(attempts))
                            item['last_error'] = 'waiting for message_id'
                            remaining.append(item)
                            changed = True
                            continue

                        call_params = {k: v for k, v in params.items() if v is not None}
                        call_params.pop('coalesce_key', None)
                        call_params['message_id'] = int(mid)
                        resp = self._api.edit_message_text(**call_params)
                        try:
                            result = (resp.get('result') or {}) if isinstance(resp, dict) else {}
                            tid = int(result.get('message_thread_id') or 0) if isinstance(result, dict) else 0
                            self._topic_log_append(
                                chat_id=int(chat_id),
                                message_thread_id=(int(tid) if tid > 0 else None),
                                item={
                                    'ts': float(time.time()),
                                    'dir': 'out',
                                    'op': 'edit_message_text',
                                    'chat_id': int(chat_id),
                                    'thread_id': int(tid),
                                    'message_id': int(mid),
                                    'parse_mode': str(call_params.get('parse_mode') or '').strip() or None,
                                    'text': self._topic_log_preview_text(call_params.get('text')),
                                    'deferred': False,
                                    'meta': dict(meta) if isinstance(meta, dict) and meta else None,
                                },
                            )
                        except Exception:
                            pass
                    elif op == 'edit_forum_topic':
                        self._api.edit_forum_topic(**{k: v for k, v in params.items() if v is not None})
                    elif op == 'edit_message_reply_markup':
                        self._api.edit_message_reply_markup(**{k: v for k, v in params.items() if v is not None})
                    elif op == 'delete_message_by_key':
                        coalesce_key = str(params.get('coalesce_key') or '').strip()
                        if not coalesce_key:
                            self._log(
                                f'[outbox] drop invalid delete_message_by_key (missing coalesce_key) chat_id={int(chat_id)}'
                            )
                            changed = True
                            continue

                        mid = self._state.tg_message_id_for_coalesce_key(
                            chat_id=int(chat_id), coalesce_key=coalesce_key
                        )
                        if int(mid) <= 0:
                            attempts = int(item.get('attempts') or 0) + 1
                            item['attempts'] = attempts
                            item['next_attempt_ts'] = float(now + self._backoff(attempts))
                            item['last_error'] = 'waiting for message_id'
                            remaining.append(item)
                            changed = True
                            continue

                        self._api.delete_message(chat_id=int(chat_id), message_id=int(mid))
                    elif op == 'delete_message':
                        self._api.delete_message(**{k: v for k, v in params.items() if v is not None})
                    else:
                        self._log(f'[outbox] drop unknown op={op} chat_id={int(chat_id)}')
                        changed = True
                        continue
                except Exception as e:
                    # Treat a no-op edit as success (prevents stuck retries when we keep replaying the last state).
                    if (
                        op in {'edit_message_text', 'edit_message_text_by_key'}
                        and 'message is not modified' in str(e).lower()
                    ):
                        delivered += 1
                        changed = True
                        continue
                    if op in {'delete_message', 'delete_message_by_key'}:
                        err_low = str(e).lower()
                        if ('message to delete not found' in err_low) or ('message_id_invalid' in err_low):
                            delivered += 1
                            changed = True
                            continue

                    if self._is_retryable_error(e):
                        attempts = int(item.get('attempts') or 0) + 1
                        item['attempts'] = attempts
                        item['next_attempt_ts'] = float(now + self._backoff(attempts))
                        item['last_error'] = str(e)[:400]
                        remaining.append(item)
                        changed = True
                        self._state.tg_mark_offline(chat_id=int(chat_id), ts=now)
                        self._log(
                            f'[outbox] retry chat_id={int(chat_id)} op={op} attempts={attempts} err={str(e)[:200]}'
                        )
                        continue

                    # Best-effort fallback: if we cannot edit a message anymore (deleted/too old),
                    # try to deliver the text as a new message so the user still gets the content.
                    if op == 'edit_message_text':
                        err_s = str(e)
                        err_low = err_s.lower()
                        edit_mid = 0
                        try:
                            edit_mid = int(params.get('message_id') or 0)
                        except Exception:
                            edit_mid = 0

                        if (
                            ('message to edit not found' in err_low)
                            or ("message can't be edited" in err_low)
                            or ('message_id_invalid' in err_low)
                        ):
                            send_params: dict[str, Any] = {k: v for k, v in params.items() if v is not None}
                            send_params.pop('message_id', None)
                            send_params.pop('timeout', None)
                            send_params['timeout'] = 30
                            try:
                                resp_fb = self._api.send_message(**send_params)
                                try:
                                    result_fb = (resp_fb.get('result') or {}) if isinstance(resp_fb, dict) else {}
                                    tid_fb = (
                                        int(result_fb.get('message_thread_id') or 0)
                                        if isinstance(result_fb, dict)
                                        else 0
                                    )
                                    self._topic_log_append(
                                        chat_id=int(chat_id),
                                        message_thread_id=(int(tid_fb) if tid_fb > 0 else None),
                                        item={
                                            'ts': float(time.time()),
                                            'dir': 'out',
                                            'op': 'send_message',
                                            'chat_id': int(chat_id),
                                            'thread_id': int(tid_fb),
                                            'message_id': int(
                                                (
                                                    (result_fb.get('message_id') or 0)
                                                    if isinstance(result_fb, dict)
                                                    else 0
                                                )
                                                or 0
                                            ),
                                            'text': self._topic_log_preview_text(send_params.get('text')),
                                            'deferred': False,
                                            'meta': {
                                                'kind': 'edit_fallback_send',
                                                'from_edit_message_id': int(edit_mid),
                                            },
                                        },
                                    )
                                except Exception:
                                    pass
                            except Exception as send_e:
                                if self._is_retryable_error(send_e):
                                    attempts = int(item.get('attempts') or 0) + 1
                                    item['attempts'] = attempts
                                    item['next_attempt_ts'] = float(now + self._backoff(attempts))
                                    item['last_error'] = str(send_e)[:400]
                                    item['op'] = 'send_message'
                                    item['params'] = send_params
                                    item['coalesce_key'] = (
                                        f'send_message_fallback:{int(edit_mid)}'
                                        if edit_mid > 0
                                        else 'send_message_fallback'
                                    )
                                    remaining.append(item)
                                    changed = True
                                    self._state.tg_mark_offline(chat_id=int(chat_id), ts=now)
                                    self._log(
                                        f'[outbox] fallback-send retry chat_id={int(chat_id)} from_edit_mid={int(edit_mid)} '
                                        f'attempts={attempts} err={str(send_e)[:200]}'
                                    )
                                    continue
                                self._log(
                                    f'[outbox] fallback-send drop chat_id={int(chat_id)} from_edit_mid={int(edit_mid)} err={str(send_e)[:250]}'
                                )
                                changed = True
                                continue

                            delivered += 1
                            changed = True
                            self._log(f'[outbox] fallback-send ok chat_id={int(chat_id)} from_edit_mid={int(edit_mid)}')
                            continue

                    self._log(f'[outbox] drop chat_id={int(chat_id)} op={op} err={str(e)[:250]}')
                    changed = True
                    continue

                if reminder_meta is not None:
                    date_key, reminder_ids = reminder_meta
                    self._state.reminders_clear_pending_many(date_key, reminder_ids)
                    self._state.reminders_mark_sent_many(date_key, reminder_ids)

                if mm_meta is not None:
                    channel_id, up_to_ts = mm_meta
                    self._state.mm_mark_sent(channel_id=channel_id, up_to_ts=int(up_to_ts))

                if op == 'send_document' and upload_meta is not None:
                    ack_chat_id, ack_coalesce_key = upload_meta
                    # NOTE: we cannot call tg_outbox_enqueue() here because we'll overwrite the outbox
                    # with tg_outbox_replace(items=remaining) after the loop. Append to `remaining` instead.
                    ck_hash = sha1(ack_coalesce_key.encode('utf-8')).hexdigest()[:16]
                    remaining.append(
                        {
                            'id': uuid4().hex,
                            'op': 'delete_message_by_key',
                            'chat_id': int(ack_chat_id),
                            'params': {'chat_id': int(ack_chat_id), 'coalesce_key': str(ack_coalesce_key)},
                            'created_ts': float(now),
                            'attempts': 0,
                            'next_attempt_ts': float(now),
                            'last_error': '',
                            'coalesce_key': f'delete_key:{ck_hash}',
                        }
                    )

                delivered += 1
                changed = True

            if changed:
                self._state.tg_outbox_replace(items=remaining)

            # If a chat has no remaining deferred ops, clear its "offline" state (and allow a fresh
            # restore notice next time). This prevents repeated "restored" notices when a single
            # op keeps failing and re-queues itself.
            if changed:
                remaining_chat_ids: set[int] = set()
                for it in remaining:
                    if not isinstance(it, dict):
                        continue
                    try:
                        cid = int(it.get('chat_id') or 0)
                    except Exception:
                        cid = 0
                    if cid != 0:
                        remaining_chat_ids.add(cid)
                try:
                    offline_chat_ids = self._state.tg_offline_chat_ids_snapshot()
                except Exception:
                    offline_chat_ids = []
                for cid in offline_chat_ids:
                    if int(cid) != 0 and int(cid) not in remaining_chat_ids:
                        self._state.tg_clear_offline(chat_id=int(cid))

            return int(delivered)
        finally:
            try:
                self._flush_lock.release()
            except Exception:
                pass
