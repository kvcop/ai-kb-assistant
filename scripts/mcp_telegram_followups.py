from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def _now() -> float:
    return time.time()


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return {}
    try:
        obj = json.loads(raw or '{}')
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(content, encoding='utf-8')
    os.replace(tmp, path)


def _scope_key(*, chat_id: int, message_thread_id: int) -> str:
    return f'{int(chat_id)}:{int(message_thread_id or 0)}'


class MCPServer:
    def __init__(self, *, bot_state_path: Path, followups_ack_path: Path, default_chat_id: int) -> None:
        self._bot_state_path = bot_state_path
        self._followups_ack_path = followups_ack_path
        self._default_chat_id = int(default_chat_id)

    def _write(self, obj: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + '\n')
        sys.stdout.flush()

    def _ok_tool_result(self, *, text: str, structured: dict[str, Any]) -> dict[str, Any]:
        return {
            'content': [{'type': 'text', 'text': text}],
            'structuredContent': structured,
        }

    def _followups_enabled_for_chat(self, *, chat_id: int) -> bool:
        st = _read_json_dict(self._bot_state_path)
        raw = st.get('ux_mcp_live_enabled_by_chat')
        if not isinstance(raw, dict):
            return True
        v = raw.get(str(int(chat_id)))
        if v is None:
            return True
        return bool(v)

    def _tools(self) -> list[dict[str, Any]]:
        followups_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'after_message_id': {'type': 'integer'},
                'limit': {'type': 'integer'},
                'timeout_seconds': {'type': 'number'},
            },
            'additionalProperties': False,
        }
        followups_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'followups': {'type': 'array', 'items': {'type': 'object', 'additionalProperties': True}},
                'latest_message_id': {'type': 'integer'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'chat_id', 'message_thread_id', 'followups', 'latest_message_id', 'note'],
            'additionalProperties': False,
        }

        ack_followups_schema: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'last_message_id': {'type': 'integer'},
            },
            'required': ['last_message_id'],
            'additionalProperties': False,
        }
        ack_followups_out: dict[str, Any] = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'type': 'object',
            'properties': {
                'ok': {'type': 'boolean'},
                'chat_id': {'type': 'integer'},
                'message_thread_id': {'type': 'integer'},
                'last_message_id': {'type': 'integer'},
                'note': {'type': 'string'},
            },
            'required': ['ok', 'chat_id', 'message_thread_id', 'last_message_id', 'note'],
            'additionalProperties': False,
        }

        return [
            {
                'name': 'get_followups',
                'title': 'Get Telegram Follow-ups',
                'description': 'Read pending follow-up messages captured by tg_bot while Codex is running in a scope.',
                'inputSchema': followups_schema,
                'outputSchema': followups_out,
            },
            {
                'name': 'wait_followups',
                'title': 'Wait for Telegram Follow-ups',
                'description': 'Block up to timeout_seconds waiting for new follow-ups, then return them.',
                'inputSchema': followups_schema,
                'outputSchema': followups_out,
            },
            {
                'name': 'ack_followups',
                'title': 'Acknowledge Telegram Follow-ups',
                'description': 'Mark follow-ups as processed up to last_message_id for this scope (used to dedupe queued events).',
                'inputSchema': ack_followups_schema,
                'outputSchema': ack_followups_out,
            },
        ]

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

            # Notifications have no id: do not respond.
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
                            'serverInfo': {'name': 'telegram-followups-mcp', 'version': '0.1.0'},
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
                                    'ok': True,
                                    'chat_id': int(self._default_chat_id),
                                    'message_thread_id': 0,
                                    'followups': [],
                                    'latest_message_id': 0,
                                    'note': 'invalid_arguments',
                                },
                            ),
                        }
                    )
                    continue

                if name in {'get_followups', 'wait_followups'}:
                    chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    after_message_id = int(args.get('after_message_id') or 0)
                    limit = int(args.get('limit') or 50)
                    limit = max(1, min(200, int(limit or 50)))
                    timeout_s = float(args.get('timeout_seconds') or 0.0)
                    timeout_s = max(0.0, min(300.0, timeout_s))

                    if chat_id != 0 and not self._followups_enabled_for_chat(chat_id=chat_id):
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (followups disabled in Settings).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'followups': [],
                                        'latest_message_id': int(after_message_id),
                                        'note': 'disabled_by_settings',
                                    },
                                ),
                            }
                        )
                        continue

                    note = 'ok'
                    followups: list[dict[str, Any]] = []
                    latest_mid = 0
                    sk = _scope_key(chat_id=chat_id, message_thread_id=message_thread_id)

                    start = _now()
                    while True:
                        try:
                            st = _read_json_dict(self._bot_state_path)
                            raw = st.get('pending_followups_by_scope') or {}
                            per_scope = raw.get(sk) if isinstance(raw, dict) else None
                            items = per_scope if isinstance(per_scope, list) else []
                            cleaned: list[dict[str, Any]] = []
                            for it in items:
                                if not isinstance(it, dict):
                                    continue
                                try:
                                    mid = int(it.get('message_id') or 0)
                                except Exception:
                                    mid = 0
                                if mid <= 0 or mid <= after_message_id:
                                    continue
                                cleaned.append(dict(it))
                            cleaned.sort(key=lambda x: int(x.get('message_id') or 0))
                            if cleaned:
                                followups = cleaned[:limit]
                                try:
                                    latest_mid = int(followups[-1].get('message_id') or 0)
                                except Exception:
                                    latest_mid = 0
                            else:
                                followups = []
                                latest_mid = int(after_message_id or 0)
                        except Exception as e:
                            note = f'read_failed: {e!r}'
                            followups = []
                            latest_mid = int(after_message_id or 0)

                        if name == 'get_followups':
                            break
                        if followups:
                            break
                        if timeout_s <= 0:
                            break
                        if (_now() - start) >= timeout_s:
                            break
                        time.sleep(0.5)

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK',
                                structured={
                                    'ok': True,
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'followups': followups,
                                    'latest_message_id': int(latest_mid),
                                    'note': note,
                                },
                            ),
                        }
                    )
                    continue

                if name == 'ack_followups':
                    chat_id = int(args.get('chat_id') or self._default_chat_id or 0)
                    message_thread_id = int(args.get('message_thread_id') or 0)
                    message_thread_id = max(0, int(message_thread_id or 0))
                    last_message_id = int(args.get('last_message_id') or 0)
                    last_message_id = max(0, int(last_message_id or 0))
                    sk = _scope_key(chat_id=chat_id, message_thread_id=message_thread_id)

                    if chat_id != 0 and not self._followups_enabled_for_chat(chat_id=chat_id):
                        self._write(
                            {
                                'jsonrpc': '2.0',
                                'id': req_id,
                                'result': self._ok_tool_result(
                                    text='OK (followups disabled in Settings).',
                                    structured={
                                        'ok': True,
                                        'chat_id': int(chat_id),
                                        'message_thread_id': int(message_thread_id),
                                        'last_message_id': int(last_message_id),
                                        'note': 'disabled_by_settings',
                                    },
                                ),
                            }
                        )
                        continue

                    note = 'ack_ok'
                    try:
                        cur = _read_json_dict(self._followups_ack_path)
                        version = int(cur.get('version') or 1)
                        acked = cur.get('acked_by_scope') if isinstance(cur.get('acked_by_scope'), dict) else {}
                        if not isinstance(acked, dict):
                            acked = {}
                        prev = 0
                        try:
                            prev = int(acked.get(sk) or 0)
                        except Exception:
                            prev = 0
                        if last_message_id > prev:
                            acked[sk] = int(last_message_id)
                        payload = {'version': int(version), 'acked_by_scope': acked}
                        _atomic_write_text(self._followups_ack_path, json.dumps(payload, ensure_ascii=False, indent=2))
                    except Exception as e:
                        note = f'ack_failed: {e!r}'

                    self._write(
                        {
                            'jsonrpc': '2.0',
                            'id': req_id,
                            'result': self._ok_tool_result(
                                text='OK',
                                structured={
                                    'ok': True,
                                    'chat_id': int(chat_id),
                                    'message_thread_id': int(message_thread_id),
                                    'last_message_id': int(last_message_id),
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
    bot_state_path = Path(
        os.getenv('TG_BOT_STATE_PATH', str(repo_root / 'logs' / 'tg-bot' / 'state.json'))
    ).expanduser()
    followups_ack_path = Path(
        os.getenv('TG_MCP_FOLLOWUPS_ACK_PATH', str(repo_root / '.mcp' / 'telegram-followups-ack.json'))
    ).expanduser()
    default_chat_id = int(os.getenv('TG_MCP_DEFAULT_CHAT_ID', os.getenv('TG_OWNER_CHAT_ID', '0')) or 0)
    server = MCPServer(
        bot_state_path=bot_state_path,
        followups_ack_path=followups_ack_path,
        default_chat_id=default_chat_id,
    )
    server.serve_forever()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
