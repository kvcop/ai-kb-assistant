from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

DEFAULT_BASE_URL = 'http://127.0.0.1:8000/api'
DEFAULT_TIMEOUT_S = 300


def _load_dotenv(path: Path) -> None:
    """Best-effort .env loader (no dependencies).

    Supports:
      - KEY=VALUE
      - export KEY=VALUE

    Does not override already-set env vars.
    """
    try:
        if not path.exists():
            return
        content = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[len('export ') :].strip()
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


def _clamp_int(value: Any, *, default: int, lo: int, hi: int) -> int:
    try:
        v = int(value)
    except Exception:
        return default
    return max(lo, min(hi, v))


def _clamp_float(value: Any, *, default: float, lo: float, hi: float) -> float:
    try:
        v = float(value)
    except Exception:
        return default
    return max(lo, min(hi, v))


class Speech2TextMCPServer:
    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = repo_root
        self._cli_path = repo_root / 'scripts' / 'speech2text.py'

    def _write(self, obj: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + '\n')
        sys.stdout.flush()

    def _ok_tool_result(self, *, text: str, structured: dict[str, Any]) -> dict[str, Any]:
        return {
            'content': [{'type': 'text', 'text': text}],
            'structuredContent': structured,
        }

    def _tools(self) -> list[dict[str, Any]]:
        doctor_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'base_url': {'type': 'string'},
                'timeout_seconds': {'type': 'integer'},
            },
            'additionalProperties': False,
        }
        doctor_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'base_url': {'type': 'string'},
                'exit_code': {'type': 'integer'},
                'stdout': {'type': 'string'},
                'stderr': {'type': 'string'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'base_url', 'exit_code', 'stdout', 'stderr', 'note'],
            'additionalProperties': False,
        }

        transcribe_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'path': {'type': 'string'},
                'base_url': {'type': 'string'},
                'timeout_seconds': {'type': 'integer'},
                'poll_interval_seconds': {'type': 'number'},
                'diarization': {'type': 'boolean'},
                'max_speakers': {'type': 'integer'},
                'data_format': {'type': 'string', 'enum': ['AUTO', 'AUDIO', 'VIDEO']},
            },
            'required': ['path'],
            'additionalProperties': False,
        }
        transcribe_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'path': {'type': 'string'},
                'base_url': {'type': 'string'},
                'exit_code': {'type': 'integer'},
                'transcript': {'type': 'string'},
                'stdout': {'type': 'string'},
                'stderr': {'type': 'string'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'path', 'base_url', 'exit_code', 'transcript', 'stdout', 'stderr', 'note'],
            'additionalProperties': False,
        }

        return [
            {
                'name': 'doctor',
                'title': 'Voice Recognition Doctor',
                'description': 'Check connectivity to the Speech2Text service.',
                'inputSchema': doctor_schema,
                'outputSchema': doctor_out,
            },
            {
                'name': 'transcribe',
                'title': 'Transcribe Audio/Video',
                'description': 'Submit + wait + fetch transcript via Speech2Text.',
                'inputSchema': transcribe_schema,
                'outputSchema': transcribe_out,
            },
        ]

    def _resolve_base_url(self, explicit: str | None) -> str:
        if explicit and explicit.strip():
            return explicit.strip().rstrip('/')
        for k in ('SPEECH2TEXT_BASE_URL', 'SPEECH2TEXT_API_BASE_URL'):
            v = os.getenv(k)
            if v and v.strip():
                return v.strip().rstrip('/')
        return DEFAULT_BASE_URL.rstrip('/')

    def _run_cli(self, args: list[str], *, timeout_seconds: int) -> tuple[int, str, str]:
        if not self._cli_path.exists():
            return (2, '', f'speech2text CLI not found: {self._cli_path}')
        try:
            completed = subprocess.run(
                [sys.executable, str(self._cli_path), *args],
                cwd=str(self._repo_root),
                capture_output=True,
                text=True,
                timeout=max(1, int(timeout_seconds)),
            )
            return (int(completed.returncode), completed.stdout or '', completed.stderr or '')
        except subprocess.TimeoutExpired:
            return (124, '', f'timeout after {timeout_seconds}s')
        except Exception as e:
            return (2, '', f'failed to run speech2text CLI: {e!r}')

    def serve_forever(self) -> None:
        for raw_line in sys.stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except Exception:
                continue
            if not isinstance(req, dict):
                continue

            method = req.get('method')
            req_id = req.get('id')
            if not method or not isinstance(method, str):
                continue

            if req_id is None:
                continue

            if method == 'initialize':
                self._write(
                    {
                        'jsonrpc': '2.0',
                        'id': req_id,
                        'result': {
                            'protocolVersion': '2024-11-05',
                            'capabilities': {'tools': {'listChanged': False}},
                            'serverInfo': {'name': 'speech2text-mcp', 'version': '0.1.0'},
                        },
                    }
                )
                continue

            if method == 'tools/list':
                self._write({'jsonrpc': '2.0', 'id': req_id, 'result': {'tools': self._tools()}})
                continue

            if method == 'tools/call':
                params = req.get('params') or {}
                name = params.get('name') if isinstance(params, dict) else None
                args = (params.get('arguments') or {}) if isinstance(params, dict) else {}
                if not isinstance(name, str) or not isinstance(args, dict):
                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK (invalid arguments).',
                                structured={
                                    'ok': False,
                                    'exit_code': 2,
                                    'note': 'invalid_arguments',
                                },
                            ),
                        }
                    )
                    continue

                if name == 'doctor':
                    base_url = self._resolve_base_url(str(args.get('base_url') or '').strip() or None)
                    timeout_s = _clamp_int(args.get('timeout_seconds'), default=20, lo=1, hi=DEFAULT_TIMEOUT_S)
                    code, out, err = self._run_cli(
                        ['--base-url', base_url, '--timeout', str(timeout_s), 'doctor'],
                        timeout_seconds=timeout_s,
                    )
                    ok = code == 0
                    note = 'ok' if ok else 'failed'
                    text = out.strip() or ('OK' if ok else 'ERROR')
                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text=text,
                                structured={
                                    'ok': bool(ok),
                                    'base_url': base_url,
                                    'exit_code': int(code),
                                    'stdout': out,
                                    'stderr': err,
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                if name == 'transcribe':
                    path_raw = str(args.get('path') or '').strip()
                    base_url = self._resolve_base_url(str(args.get('base_url') or '').strip() or None)
                    timeout_s = _clamp_int(
                        args.get('timeout_seconds'), default=DEFAULT_TIMEOUT_S, lo=1, hi=DEFAULT_TIMEOUT_S
                    )
                    poll_s = _clamp_float(args.get('poll_interval_seconds'), default=2.0, lo=0.25, hi=60.0)
                    diarization = bool(args.get('diarization') or False)
                    max_speakers = _clamp_int(args.get('max_speakers'), default=1, lo=1, hi=50)
                    data_format = str(args.get('data_format') or 'AUTO').strip().upper()
                    if data_format not in {'AUTO', 'AUDIO', 'VIDEO'}:
                        data_format = 'AUTO'

                    p0 = Path(path_raw).expanduser()
                    if not p0.is_absolute():
                        p0 = (Path.cwd() / p0).resolve()

                    if not path_raw or not p0.is_file():
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text=f'File not found: {p0}',
                                    structured={
                                        'ok': False,
                                        'path': str(p0),
                                        'base_url': base_url,
                                        'exit_code': 2,
                                        'transcript': '',
                                        'stdout': '',
                                        'stderr': f'file_not_found: {p0}',
                                        'note': 'file_not_found',
                                    },
                                ),
                            }
                        )
                        continue

                    cli_args = [
                        '--base-url',
                        base_url,
                        '--timeout',
                        str(timeout_s),
                        'transcribe',
                        str(p0),
                        '--poll-interval',
                        str(poll_s),
                        '--max-speakers',
                        str(max_speakers),
                        '--data-format',
                        data_format,
                    ]
                    if diarization:
                        cli_args.append('--diarization')

                    code, out, err = self._run_cli(cli_args, timeout_seconds=timeout_s)
                    transcript = out.strip()
                    ok = code == 0 and bool(transcript)
                    note = 'ok' if ok else 'failed'
                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text=transcript if transcript else (err.strip() or 'ERROR'),
                                structured={
                                    'ok': bool(ok),
                                    'path': str(p0),
                                    'base_url': base_url,
                                    'exit_code': int(code),
                                    'transcript': transcript,
                                    'stdout': out,
                                    'stderr': err,
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                self._write(
                    {
                        'jsonrpc': '2.0',
                        'id': req_id,
                        'error': {'code': -32601, 'message': f'Unknown tool: {name}'},
                    }
                )
                continue

            self._write(
                {
                    'jsonrpc': '2.0',
                    'id': req_id,
                    'error': {'code': -32601, 'message': f'Method not found: {method}'},
                }
            )


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    _load_dotenv(repo_root / 'tg_bot' / '.env')
    _load_dotenv(repo_root / '.env.tg_bot')
    Speech2TextMCPServer(repo_root=repo_root).serve_forever()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
