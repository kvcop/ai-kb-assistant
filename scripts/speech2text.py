#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import http.client
import json
import mimetypes
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

DEFAULT_BASE_URL = 'http://127.0.0.1:8000/api'
DEFAULT_TIMEOUT_S = 300
DEFAULT_POLL_INTERVAL_S = 2.0
DEFAULT_TASK_NOT_FOUND_GRACE_S = 20.0


class Speech2TextError(RuntimeError):
    pass


@dataclass(frozen=True)
class HttpResponse:
    status: int
    reason: str
    headers: dict[str, str]
    body: bytes


def _default_token_path() -> Path:
    return Path.home() / '.config' / 'speech2text' / 'token'


def _clamp_timeout(timeout_s: int) -> int:
    if timeout_s <= 0:
        raise Speech2TextError('--timeout must be > 0')
    if timeout_s > DEFAULT_TIMEOUT_S:
        raise Speech2TextError(f'--timeout must be <= {DEFAULT_TIMEOUT_S} seconds')
    return timeout_s


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding='utf-8').strip()


def _write_secret_file(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.strip() + '\n', encoding='utf-8')
    os.chmod(path, 0o600)


def _resolve_token(explicit: str | None, token_file: Path | None) -> str | None:
    if explicit:
        return explicit.strip()

    for env_key in ('SPEECH2TEXT_TOKEN', 'SPEECH2TEXT_JWT_TOKEN', 'X_JWT_TOKEN'):
        env_val = os.environ.get(env_key)
        if env_val:
            return env_val.strip()

    path = token_file or _default_token_path()
    if path.exists():
        return _read_text_file(path)

    return None


def _http_request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_s: int,
) -> HttpResponse:
    parts = urlsplit(url)
    if parts.scheme not in {'http', 'https'}:
        raise Speech2TextError(f'Unsupported scheme: {parts.scheme!r}')

    host = parts.hostname
    if not host:
        raise Speech2TextError(f'Invalid URL: {url!r}')

    port = parts.port or (443 if parts.scheme == 'https' else 80)
    path = parts.path or '/'
    if parts.query:
        path += f'?{parts.query}'

    conn_cls: type[http.client.HTTPConnection]
    if parts.scheme == 'https':
        conn_cls = http.client.HTTPSConnection
    else:
        conn_cls = http.client.HTTPConnection

    conn = conn_cls(host, port, timeout=timeout_s)
    try:
        conn.request(method.upper(), path, body=body, headers=headers or {})
        resp = conn.getresponse()
        resp_body = resp.read()
        return HttpResponse(
            status=resp.status,
            reason=resp.reason,
            headers={k.lower(): v for k, v in resp.getheaders()},
            body=resp_body,
        )
    finally:
        conn.close()


def _http_request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout_s: int,
) -> dict[str, Any]:
    resp = _http_request(method, url, headers=headers, body=body, timeout_s=timeout_s)
    if not (200 <= resp.status <= 299):
        text = resp.body.decode('utf-8', errors='replace')
        raise Speech2TextError(f'HTTP {resp.status} {resp.reason} for {url}: {text}')
    try:
        return json.loads(resp.body.decode('utf-8'))
    except json.JSONDecodeError as err:
        text = resp.body.decode('utf-8', errors='replace')
        raise Speech2TextError(f'Non-JSON response for {url}: {text}') from err


def _base_url(base_url: str) -> str:
    return base_url.rstrip('/')


def register_user(*, base_url: str, user_name: str, password: str, timeout_s: int) -> None:
    url = f'{_base_url(base_url)}/register'
    body = json.dumps({'user_name': user_name, 'password': password}).encode('utf-8')
    resp = _http_request('POST', url, headers={'content-type': 'application/json'}, body=body, timeout_s=timeout_s)
    if resp.status == 201:
        return
    text = resp.body.decode('utf-8', errors='replace')
    raise Speech2TextError(f'Register failed: HTTP {resp.status} {resp.reason}: {text}')


def login(*, base_url: str, username: str, password: str, timeout_s: int) -> str:
    url = f'{_base_url(base_url)}/login'
    body = urlencode({'username': username, 'password': password}).encode('utf-8')
    data = _http_request_json(
        'POST',
        url,
        headers={'content-type': 'application/x-www-form-urlencoded'},
        body=body,
        timeout_s=timeout_s,
    )

    for key in ('x-jwt-token', 'jwt_token', 'access_token', 'token'):
        token = data.get(key)
        if isinstance(token, str) and token.strip():
            return token.strip()

    raise Speech2TextError(f'Unexpected /login response keys: {sorted(data.keys())}')


def _guess_data_format(path: Path) -> str:
    ext = path.suffix.lower().lstrip('.')
    if ext in {'mp4', 'mkv', 'mov', 'avi', 'webm'}:
        return 'VIDEO'
    return 'AUDIO'


def _post_multipart_file(
    url: str,
    *,
    token: str,
    field_name: str,
    file_path: Path,
    timeout_s: int,
) -> dict[str, Any]:
    parts = urlsplit(url)
    if parts.scheme not in {'http', 'https'}:
        raise Speech2TextError(f'Unsupported scheme: {parts.scheme!r}')

    host = parts.hostname
    if not host:
        raise Speech2TextError(f'Invalid URL: {url!r}')

    port = parts.port or (443 if parts.scheme == 'https' else 80)
    path = parts.path or '/'
    if parts.query:
        path += f'?{parts.query}'

    boundary = uuid.uuid4().hex
    filename = file_path.name
    content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'

    prefix = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
        f'Content-Type: {content_type}\r\n'
        '\r\n'
    ).encode()
    suffix = f'\r\n--{boundary}--\r\n'.encode()

    file_size = file_path.stat().st_size
    content_length = len(prefix) + file_size + len(suffix)

    headers = {
        'content-type': f'multipart/form-data; boundary={boundary}',
        'content-length': str(content_length),
        'x-jwt-token': token,
    }

    conn_cls: type[http.client.HTTPConnection]
    if parts.scheme == 'https':
        conn_cls = http.client.HTTPSConnection
    else:
        conn_cls = http.client.HTTPConnection

    conn = conn_cls(host, port, timeout=timeout_s)
    try:
        conn.putrequest('POST', path)
        for k, v in headers.items():
            conn.putheader(k, v)
        conn.endheaders()

        conn.send(prefix)
        with file_path.open('rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                conn.send(chunk)
        conn.send(suffix)

        resp = conn.getresponse()
        resp_body = resp.read()
        if not (200 <= resp.status <= 299):
            text = resp_body.decode('utf-8', errors='replace')
            raise Speech2TextError(f'HTTP {resp.status} {resp.reason} for {url}: {text}')
        return json.loads(resp_body.decode('utf-8'))
    finally:
        conn.close()


def add_task(
    *,
    base_url: str,
    token: str,
    uploaded_file: Path,
    data_format: str,
    diarization: bool,
    max_speakers: int,
    timeout_s: int,
) -> str:
    diarization_str = 'true' if diarization else 'false'
    url = (
        f'{_base_url(base_url)}/tasks/add'
        f'?max_speakers={max_speakers}&diarization={diarization_str}&data_format={data_format}'
    )
    data = _post_multipart_file(
        url,
        token=token,
        field_name='uploaded_file',
        file_path=uploaded_file,
        timeout_s=timeout_s,
    )
    task_id = data.get('id')
    if not isinstance(task_id, str) or not task_id.strip():
        raise Speech2TextError(f'Unexpected /tasks/add response: {data}')
    return task_id


def get_task_info(
    *,
    base_url: str,
    token: str,
    task_id: str,
    timeout_s: int,
    allow_not_found: bool = False,
) -> dict[str, Any] | None:
    url = f'{_base_url(base_url)}/tasks/{task_id}'
    resp = _http_request('GET', url, headers={'x-jwt-token': token}, timeout_s=timeout_s)
    if resp.status == 404 and allow_not_found:
        return None
    if not (200 <= resp.status <= 299):
        text = resp.body.decode('utf-8', errors='replace')
        raise Speech2TextError(f'HTTP {resp.status} {resp.reason} for {url}: {text}')
    try:
        return json.loads(resp.body.decode('utf-8'))
    except json.JSONDecodeError as err:
        text = resp.body.decode('utf-8', errors='replace')
        raise Speech2TextError(f'Non-JSON response for {url}: {text}') from err


def get_result_txt(*, base_url: str, token: str, task_id: str, timeout_s: int) -> str:
    url = f'{_base_url(base_url)}/results/txt?{urlencode({"task_id": task_id})}'
    data = _http_request_json('GET', url, headers={'x-jwt-token': token}, timeout_s=timeout_s)
    result = data.get('result')
    if not isinstance(result, str):
        raise Speech2TextError(f'Unexpected /results/txt response: {data}')
    return result


def wait_task(
    *,
    base_url: str,
    token: str,
    task_id: str,
    timeout_s: int,
    poll_interval_s: float,
    task_not_found_grace_s: float = DEFAULT_TASK_NOT_FOUND_GRACE_S,
) -> dict[str, Any]:
    started = time.monotonic()
    last_status: str | None = None
    grace_s = max(0.0, min(float(task_not_found_grace_s), float(timeout_s)))
    while True:
        if time.monotonic() - started > timeout_s:
            raise Speech2TextError(f'Timed out waiting for task {task_id} (>{timeout_s}s)')

        info = get_task_info(base_url=base_url, token=token, task_id=task_id, timeout_s=timeout_s, allow_not_found=True)
        if info is None:
            # Some deployments create a task id before it's visible in storage (eventual consistency).
            # Treat 404 as transient only for a short grace window to avoid masking real "wrong id" errors.
            if time.monotonic() - started <= grace_s:
                if last_status != 'NOT_FOUND':
                    print(f'[speech2text] {task_id}: NOT_FOUND (waiting for registration)', file=sys.stderr)
                    last_status = 'NOT_FOUND'
                time.sleep(poll_interval_s)
                continue
            # Raise the original HTTP error (includes server details), but if it becomes visible
            # right now (race), proceed normally.
            info2 = get_task_info(
                base_url=base_url, token=token, task_id=task_id, timeout_s=timeout_s, allow_not_found=False
            )
            if info2 is None:
                raise Speech2TextError(f'Task {task_id} not found')
            info = info2
        status = info.get('status')
        if isinstance(status, str) and status != last_status:
            print(f'[speech2text] {task_id}: {status}', file=sys.stderr)
            last_status = status

        if status in {'COMPLETED', 'FAULTED'}:
            return info

        time.sleep(poll_interval_s)


def _read_password(args: argparse.Namespace) -> str:
    if args.password is not None:
        return args.password
    if args.password_stdin:
        return sys.stdin.readline().rstrip('\n')
    return getpass.getpass('Speech2Text password: ')


def _write_output(path: Path | None, text: str) -> None:
    if path is None:
        sys.stdout.write(text)
        if not text.endswith('\n'):
            sys.stdout.write('\n')
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='CLI client for Voice Recognition (speech2text) service.')
    p.add_argument('--base-url', default=DEFAULT_BASE_URL, help=f'API base URL (default: {DEFAULT_BASE_URL})')
    p.add_argument(
        '--timeout',
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help=f'Per-command timeout in seconds (max {DEFAULT_TIMEOUT_S})',
    )

    sub = p.add_subparsers(dest='cmd', required=True)

    doctor = sub.add_parser('doctor', help='Check connectivity and print OpenAPI title.')
    doctor.add_argument('--openapi', default='/openapi.json', help='OpenAPI path (default: /openapi.json)')

    reg = sub.add_parser('register', help='Register a new user in the service.')
    reg.add_argument('--user-name', required=True, help='User login for /register (field: user_name)')
    reg.add_argument('--password', help='Password (discouraged; prefer prompt or --password-stdin)')
    reg.add_argument('--password-stdin', action='store_true', help='Read password from stdin')

    login_p = sub.add_parser('login', help='Login and save token to a local file (600).')
    login_p.add_argument('--username', required=True, help='Username for /login')
    login_p.add_argument('--password', help='Password (discouraged; prefer prompt or --password-stdin)')
    login_p.add_argument('--password-stdin', action='store_true', help='Read password from stdin')
    login_p.add_argument('--token-file', type=Path, default=_default_token_path(), help='Where to store token')

    submit = sub.add_parser('submit', help='Upload file and create a transcription task.')
    submit.add_argument('file', type=Path, help='Path to audio/video file')
    submit.add_argument('--token', help='JWT token (or set SPEECH2TEXT_TOKEN / token file)')
    submit.add_argument('--token-file', type=Path, help='Token file path (default: ~/.config/speech2text/token)')
    submit.add_argument('--diarization', action='store_true', help='Enable diarization (default: off)')
    submit.add_argument('--max-speakers', type=int, default=1, help='Max speakers (default: 1)')
    submit.add_argument(
        '--data-format',
        choices=['AUDIO', 'VIDEO', 'AUTO'],
        default='AUTO',
        help='Input type (default: AUTO by extension)',
    )

    wait = sub.add_parser('wait', help='Wait for task completion (COMPLETED/FAULTED).')
    wait.add_argument('task_id', help='Task UUID')
    wait.add_argument('--token', help='JWT token (or set SPEECH2TEXT_TOKEN / token file)')
    wait.add_argument('--token-file', type=Path, help='Token file path (default: ~/.config/speech2text/token)')
    wait.add_argument('--poll-interval', type=float, default=DEFAULT_POLL_INTERVAL_S, help='Seconds between polls')

    res = sub.add_parser('result', help='Fetch /results/txt for a completed task.')
    res.add_argument('task_id', help='Task UUID')
    res.add_argument('--token', help='JWT token (or set SPEECH2TEXT_TOKEN / token file)')
    res.add_argument('--token-file', type=Path, help='Token file path (default: ~/.config/speech2text/token)')
    res.add_argument('--out', type=Path, help='Write transcript to file instead of stdout')

    transcribe = sub.add_parser('transcribe', help='Submit + wait + fetch transcript (default: no diarization).')
    transcribe.add_argument('file', type=Path, help='Path to audio/video file')
    transcribe.add_argument('--token', help='JWT token (or set SPEECH2TEXT_TOKEN / token file)')
    transcribe.add_argument('--token-file', type=Path, help='Token file path (default: ~/.config/speech2text/token)')
    transcribe.add_argument('--diarization', action='store_true', help='Enable diarization (default: off)')
    transcribe.add_argument('--max-speakers', type=int, default=1, help='Max speakers (default: 1)')
    transcribe.add_argument(
        '--data-format',
        choices=['AUDIO', 'VIDEO', 'AUTO'],
        default='AUTO',
        help='Input type (default: AUTO by extension)',
    )
    transcribe.add_argument(
        '--poll-interval', type=float, default=DEFAULT_POLL_INTERVAL_S, help='Seconds between polls'
    )
    transcribe.add_argument('--out', type=Path, help='Write transcript to file instead of stdout')

    return p


def main() -> int:
    args = _build_parser().parse_args()
    timeout_s = _clamp_timeout(args.timeout)
    base_url = args.base_url

    if args.cmd == 'doctor':
        url = f'{_base_url(base_url)}{args.openapi}'
        spec = _http_request_json('GET', url, timeout_s=timeout_s)
        title = spec.get('info', {}).get('title')
        print(f'ok: {title}')
        return 0

    if args.cmd == 'register':
        password = _read_password(args)
        register_user(base_url=base_url, user_name=args.user_name, password=password, timeout_s=timeout_s)
        print('ok')
        return 0

    if args.cmd == 'login':
        password = _read_password(args)
        token = login(base_url=base_url, username=args.username, password=password, timeout_s=timeout_s)
        _write_secret_file(args.token_file, token)
        print(f'ok: token saved to {args.token_file}')
        return 0

    token = _resolve_token(getattr(args, 'token', None), getattr(args, 'token_file', None))
    if not token:
        raise Speech2TextError(
            'Missing token. Provide --token, set SPEECH2TEXT_TOKEN, or run: python3 scripts/speech2text.py login ...'
        )

    if args.cmd == 'submit':
        file_path: Path = args.file
        if not file_path.is_file():
            raise Speech2TextError(f'File not found: {file_path}')
        data_format = _guess_data_format(file_path) if args.data_format == 'AUTO' else args.data_format
        task_id = add_task(
            base_url=base_url,
            token=token,
            uploaded_file=file_path,
            data_format=data_format,
            diarization=args.diarization,
            max_speakers=args.max_speakers,
            timeout_s=timeout_s,
        )
        print(task_id)
        return 0

    if args.cmd == 'wait':
        info = wait_task(
            base_url=base_url,
            token=token,
            task_id=args.task_id,
            timeout_s=timeout_s,
            poll_interval_s=args.poll_interval,
        )
        status = info.get('status')
        if status == 'FAULTED':
            err = info.get('error') or 'unknown error'
            raise Speech2TextError(f'Task {args.task_id} faulted: {err}')
        print('ok')
        return 0

    if args.cmd == 'result':
        text = get_result_txt(base_url=base_url, token=token, task_id=args.task_id, timeout_s=timeout_s)
        _write_output(args.out, text)
        return 0

    if args.cmd == 'transcribe':
        file_path = args.file
        if not file_path.is_file():
            raise Speech2TextError(f'File not found: {file_path}')
        data_format = _guess_data_format(file_path) if args.data_format == 'AUTO' else args.data_format
        task_id = add_task(
            base_url=base_url,
            token=token,
            uploaded_file=file_path,
            data_format=data_format,
            diarization=args.diarization,
            max_speakers=args.max_speakers,
            timeout_s=timeout_s,
        )
        info = wait_task(
            base_url=base_url,
            token=token,
            task_id=task_id,
            timeout_s=timeout_s,
            poll_interval_s=args.poll_interval,
        )
        status = info.get('status')
        if status == 'FAULTED':
            err = info.get('error') or 'unknown error'
            raise Speech2TextError(f'Task {task_id} faulted: {err}')
        text = get_result_txt(base_url=base_url, token=token, task_id=task_id, timeout_s=timeout_s)
        _write_output(args.out, text)
        return 0

    raise Speech2TextError(f'Unknown command: {args.cmd}')


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Speech2TextError as e:
        print(f'speech2text: {e}', file=sys.stderr)
        raise SystemExit(2) from e
