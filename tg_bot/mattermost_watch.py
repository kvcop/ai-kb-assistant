from __future__ import annotations

import html
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

from .config import BotConfig
from .state import BotState
from .telegram_api import TelegramDeliveryAPI

try:
    from mattermostdriver import Driver as _Driver
except Exception:  # pragma: no cover
    _Driver = None

Driver: Any = _Driver


@dataclass
class _MMChannel:
    id: str
    display_name: str
    name: str
    team_id: str
    type: str


@dataclass
class _MMTeam:
    id: str
    name: str
    display_name: str


def _now_ms() -> int:
    return int(time.time() * 1000.0)


def _fmt_age_minutes(ms: int) -> str:
    try:
        ms_i = int(ms)
    except Exception:
        ms_i = 0
    if ms_i <= 0:
        return '?'
    minutes = max(0, int(ms_i // 60000))
    if minutes < 60:
        return f'{minutes}м'
    hours = minutes // 60
    rem = minutes % 60
    if hours < 48:
        return f'{hours}ч {rem}м' if rem else f'{hours}ч'
    days = hours // 24
    rem_h = hours % 24
    return f'{days}д {rem_h}ч' if rem_h else f'{days}д'


def _clamp_line(s: str, max_chars: int) -> str:
    out = (s or '').replace('\n', ' ').replace('\r', ' ').strip()
    out = ' '.join(out.split())
    if max_chars > 0 and len(out) > max_chars:
        out = out[: max(0, int(max_chars) - 1)] + '…'
    return out


def _html_text(s: str) -> str:
    return html.escape(str(s or ''), quote=False)


def _html_attr(s: str) -> str:
    return html.escape(str(s or ''), quote=True)


def _looks_like_mm_id(s: str) -> bool:
    raw = str(s or '').strip()
    if len(raw) != 26:
        return False
    for ch in raw:
        if ('a' <= ch <= 'z') or ('0' <= ch <= '9'):
            continue
        return False
    return True


def _mm_collect_posts_for_batch(
    posts: list[dict[str, Any]],
    *,
    me_id: str,
    sent_cutoff: int,
    cutoff_ms: int,
) -> tuple[list[tuple[int, dict[str, Any]]], int, int, int]:
    """Collect unread posts eligible for forwarding and compute batching triggers.

    Returns:
        items: Eligible posts sorted by create_at asc as (create_at_ms, post_dict).
        due_count: How many eligible posts are older than cutoff_ms (trigger condition).
        max_due_ts: Max create_at among due posts (0 if none).
        max_ts: Max create_at among all eligible posts (0 if none).
    """
    uid = str(me_id or '').strip()
    items: list[tuple[int, dict[str, Any]]] = []
    due_count = 0
    max_due_ts = 0
    max_ts = 0
    for p in posts:
        if not isinstance(p, dict):
            continue
        pid = str(p.get('id') or '').strip()
        if not pid:
            continue
        if str(p.get('user_id') or '').strip() == uid:
            continue
        try:
            create_at = int(p.get('create_at') or 0)
        except Exception:
            create_at = 0
        if create_at <= 0:
            continue
        if create_at <= int(sent_cutoff):
            continue
        if int(p.get('delete_at') or 0) > 0:
            continue
        ptype = str(p.get('type') or '').strip()
        if ptype and ptype.startswith('system_'):
            continue
        msg = str(p.get('message') or '').strip()
        if not msg:
            continue
        items.append((int(create_at), p))
        if int(create_at) > int(max_ts):
            max_ts = int(create_at)
        if int(create_at) <= int(cutoff_ms):
            due_count += 1
            if int(create_at) > int(max_due_ts):
                max_due_ts = int(create_at)

    items.sort(key=lambda t: t[0])  # oldest first
    return items, int(due_count), int(max_due_ts), int(max_ts)


class MattermostWatcher:
    def __init__(self, cfg: BotConfig) -> None:
        self._cfg = cfg
        self._last_poll_ts: float = 0.0
        self._driver: Any | None = None
        self._host: str = ''
        self._scheme: str = ''
        self._port: int = 0
        self._basepath: str = ''
        self._me: dict[str, Any] | None = None
        self._user_by_id: dict[str, dict[str, Any]] = {}
        self._team_by_id: dict[str, _MMTeam] = {}
        self._channel_by_id: dict[str, _MMChannel] = {}

    def _parse_url(self) -> tuple[str, str, int, str]:
        raw = (self._cfg.mm_url or '').strip()
        scheme = (self._cfg.mm_scheme or 'https').strip() or 'https'
        port = int(self._cfg.mm_port or (443 if scheme == 'https' else 80))
        basepath = (self._cfg.mm_basepath or '').strip()
        host = raw

        if raw and '://' in raw:
            try:
                u = urllib.parse.urlparse(raw)
            except Exception:
                u = None
            if u is not None:
                if u.scheme:
                    scheme = u.scheme
                if u.hostname:
                    host = u.hostname
                if u.port:
                    port = int(u.port)
                if u.path and u.path != '/' and not basepath:
                    basepath = u.path

        if '/' in host and '://' not in raw:
            # Allow MM_URL="host/path" as a shorthand.
            host_part, _, path_part = host.partition('/')
            host = host_part.strip()
            if path_part and not basepath:
                basepath = '/' + path_part.strip()

        basepath = basepath.strip()
        if basepath and not basepath.startswith('/'):
            basepath = '/' + basepath
        if basepath.endswith('/'):
            basepath = basepath.rstrip('/')
        if basepath.endswith('/api/v4'):
            basepath = basepath[: -len('/api/v4')].rstrip('/')

        return (host.strip(), scheme.strip(), int(port), basepath)

    def _base_http_url(self) -> str:
        host, scheme, port, basepath = (self._host, self._scheme, self._port, self._basepath)
        if not host:
            return ''
        default_port = 443 if scheme == 'https' else 80
        port_part = f':{int(port)}' if int(port) and int(port) != int(default_port) else ''
        return f'{scheme}://{host}{port_part}{basepath}'

    def _drop_driver(self) -> None:
        self._driver = None
        self._me = None
        self._user_by_id.clear()
        self._team_by_id.clear()
        self._channel_by_id.clear()

    def _is_auth_error(self, e: Exception) -> bool:
        low = str(e).lower()
        if 'unauthorized' in low or 'forbidden' in low:
            return True
        if ' 401' in low or ' 403' in low:
            return True
        if 'status code: 401' in low or 'status code: 403' in low:
            return True
        if 'httperror 401' in low or 'httperror 403' in low:
            return True
        if '"status":401' in low or '"status":403' in low:
            return True
        return False

    def _looks_like_mfa_required(self, e: Exception) -> bool:
        low = str(e).lower()
        if 'mfa' in low or 'мфа' in low or 'multi-factor' in low or 'two-factor' in low or '2fa' in low:
            return True
        if ('one-time' in low or 'однораз' in low or 'otp' in low) and (
            'token' in low or 'code' in low or 'код' in low
        ):
            return True
        if 'двухфактор' in low:
            return True
        return False

    def _maybe_prompt_mfa(self, *, api: TelegramDeliveryAPI, state: BotState) -> None:
        if not state.mm_should_prompt_mfa(min_interval_seconds=300):
            return
        chat_id, thread_id_raw = state.reminders_target()
        if int(chat_id or 0) == 0:
            return
        thread_id: int | None = int(thread_id_raw) if int(thread_id_raw or 0) != 0 else None
        api.send_message(
            chat_id=int(chat_id),
            message_thread_id=thread_id,
            text='🔐 Mattermost: нужен одноразовый 2FA/MFA код для входа. Пришли сюда: /mm-otp 123456',
            disable_web_page_preview=True,
        )

    def _api_basepath(self, ui_basepath: str) -> str:
        p = str(ui_basepath or '').strip()
        if not p:
            return '/api/v4'
        if p.endswith('/api/v4'):
            return p
        return p.rstrip('/') + '/api/v4'

    def _auth_mode(self) -> str:
        mode = str(getattr(self._cfg, 'mm_auth_mode', '') or '').strip().lower()
        if mode in {'token', 'pat'}:
            return 'token'
        if mode in {'login', 'password'}:
            return 'login'
        if mode in {'', 'auto'}:
            if str(getattr(self._cfg, 'mm_token', '') or '').strip():
                return 'token'
            if (
                str(getattr(self._cfg, 'mm_login_id', '') or '').strip()
                and str(getattr(self._cfg, 'mm_password', '') or '').strip()
            ):
                return 'login'
            return 'auto'
        return 'auto'

    def _connect(self, *, api: TelegramDeliveryAPI, state: BotState) -> bool:
        if not self._cfg.mm_enabled:
            return False
        if Driver is None:
            return False
        if not (self._cfg.mm_url or '').strip():
            return False

        host, scheme, port, basepath = self._parse_url()
        if not host:
            return False
        self._host, self._scheme, self._port, self._basepath = host, scheme, int(port), basepath

        mode = self._auth_mode()
        if mode == 'auto':
            return False

        opts: dict[str, Any] = {
            'url': host,
            'scheme': scheme,
            'port': int(port),
            'verify': bool(self._cfg.mm_verify),
            'timeout': int(self._cfg.mm_timeout_seconds),
        }
        opts['basepath'] = self._api_basepath(basepath)

        if mode == 'token':
            if not (self._cfg.mm_token or '').strip():
                return False
            opts['token'] = str(self._cfg.mm_token)
        elif mode == 'login':
            cache_session_token = bool(getattr(self._cfg, 'mm_cache_session_token', True))

            if cache_session_token:
                cached = state.mm_get_session_token()
                if cached:
                    try:
                        token_opts = dict(opts)
                        token_opts['token'] = cached
                        driver = Driver(token_opts)
                        driver.login()
                        me = driver.users.get_user('me')
                        if isinstance(me, dict) and str(me.get('id') or '').strip():
                            self._driver = driver
                            self._me = dict(me)
                            try:
                                state.mm_clear_mfa_required()
                            except Exception:
                                pass
                            return True
                    except Exception as e:
                        self._drop_driver()
                        if self._is_auth_error(e):
                            try:
                                state.mm_clear_session_token()
                            except Exception:
                                pass

            # If we already know MFA is required, don't keep retrying without a fresh OTP.
            try:
                if state.mm_is_mfa_required() and (not state.mm_has_mfa_token(max_age_seconds=120)):
                    self._maybe_prompt_mfa(api=api, state=state)
                    return False
            except Exception:
                pass

            # Consume once from state (set by /mm-otp). OTP is short-lived, so we keep max_age small.
            mfa_token = state.mm_consume_mfa_token(max_age_seconds=120)

            if not (getattr(self._cfg, 'mm_login_id', '') or '').strip():
                return False
            if not (getattr(self._cfg, 'mm_password', '') or '').strip():
                return False
            opts['login_id'] = str(self._cfg.mm_login_id)
            opts['password'] = str(self._cfg.mm_password)
            if mfa_token:
                opts['mfa_token'] = str(mfa_token)
        else:
            return False

        try:
            driver = Driver(opts)
            driver.login()
            me = driver.users.get_user('me')
        except Exception as e:
            self._drop_driver()
            if mode == 'login' and self._looks_like_mfa_required(e):
                try:
                    state.mm_mark_mfa_required()
                except Exception:
                    pass
                if mfa_token:
                    chat_id, thread_id_raw = state.reminders_target()
                    if int(chat_id or 0) != 0:
                        thread_id: int | None = int(thread_id_raw) if int(thread_id_raw or 0) != 0 else None
                        api.send_message(
                            chat_id=int(chat_id),
                            message_thread_id=thread_id,
                            text='🔐 Mattermost: код не подошёл или протух. Пришли новый: /mm-otp 123456',
                            disable_web_page_preview=True,
                        )
                        try:
                            state.mm_mark_mfa_prompted()
                        except Exception:
                            pass
                else:
                    self._maybe_prompt_mfa(api=api, state=state)
            return False

        if not isinstance(me, dict) or not str(me.get('id') or '').strip():
            self._drop_driver()
            return False

        self._driver = driver
        self._me = dict(me)
        if mode == 'login':
            try:
                state.mm_clear_mfa_required()
            except Exception:
                pass
            try:
                if bool(getattr(self._cfg, 'mm_cache_session_token', True)):
                    token = str(getattr(getattr(driver, 'client', None), 'token', '') or '').strip()
                    if token:
                        state.mm_set_session_token(token)
            except Exception:
                pass
        return True

    def _me_id(self) -> str:
        if not isinstance(self._me, dict):
            return ''
        return str(self._me.get('id') or '').strip()

    def _team(self, team_id: str) -> _MMTeam | None:
        tid = str(team_id or '').strip()
        if not tid:
            return None
        cached = self._team_by_id.get(tid)
        if cached is not None:
            return cached
        if self._driver is None:
            return None
        try:
            raw = self._driver.teams.get_team(tid)
        except Exception as e:
            if self._is_auth_error(e):
                self._drop_driver()
            return None
        if not isinstance(raw, dict):
            return None
        team = _MMTeam(
            id=str(raw.get('id') or '').strip(),
            name=str(raw.get('name') or '').strip(),
            display_name=str(raw.get('display_name') or '').strip() or str(raw.get('name') or '').strip(),
        )
        if team.id:
            self._team_by_id[team.id] = team
        return team

    def _channel(self, channel_id: str) -> _MMChannel | None:
        cid = str(channel_id or '').strip()
        if not cid:
            return None
        cached = self._channel_by_id.get(cid)
        if cached is not None:
            return cached
        if self._driver is None:
            return None
        try:
            raw = self._driver.channels.get_channel(cid)
        except Exception as e:
            if self._is_auth_error(e):
                self._drop_driver()
            return None
        if not isinstance(raw, dict):
            return None
        ch = _MMChannel(
            id=str(raw.get('id') or '').strip(),
            display_name=str(raw.get('display_name') or '').strip() or str(raw.get('name') or '').strip(),
            name=str(raw.get('name') or '').strip(),
            team_id=str(raw.get('team_id') or '').strip(),
            type=str(raw.get('type') or '').strip(),
        )
        if ch.id:
            self._channel_by_id[ch.id] = ch
        return ch

    def _user(self, user_id: str) -> dict[str, Any] | None:
        uid = str(user_id or '').strip()
        if not uid:
            return None
        cached = self._user_by_id.get(uid)
        if cached is not None:
            return cached
        if self._driver is None:
            return None
        try:
            raw = self._driver.users.get_user(uid)
        except Exception as e:
            if self._is_auth_error(e):
                self._drop_driver()
            return None
        if not isinstance(raw, dict):
            return None
        cleaned = dict(raw)
        self._user_by_id[uid] = cleaned
        return cleaned

    def _mm_user_label(self, user_id: str) -> str:
        u = self._user(user_id)
        username = str((u or {}).get('username') or '').strip()
        if username:
            return f'@{username}'

        first = str((u or {}).get('first_name') or '').strip()
        last = str((u or {}).get('last_name') or '').strip()
        full_name = ' '.join([x for x in [first, last] if x]).strip()
        if full_name:
            return full_name

        nickname = str((u or {}).get('nickname') or '').strip()
        if nickname:
            return nickname

        raw = str(user_id or '').strip()
        if len(raw) > 10:
            return f'{raw[:6]}…'
        return raw

    def _mm_dm_title(self, ch: _MMChannel, *, me_id: str) -> str:
        ctype = str(ch.type or '').strip().upper()
        if ctype not in {'D', 'G'}:
            return ''

        raw_name = str(ch.name or '').strip() or str(ch.display_name or '').strip()
        parts = [p for p in raw_name.split('__') if _looks_like_mm_id(p)]
        peers = [p for p in parts if p and p != me_id]
        if not peers:
            if raw_name and '__' not in raw_name:
                return raw_name
            return ''

        labels: list[str] = []
        seen: set[str] = set()
        for uid in peers:
            label = self._mm_user_label(uid)
            if label and label not in seen:
                labels.append(label)
                seen.add(label)

        if not labels:
            return ''

        if ctype == 'D':
            return labels[0]

        max_show = 3
        shown = labels[:max_show]
        remaining = len(labels) - len(shown)
        base = ', '.join(shown)
        return f'{base} (+{remaining})' if remaining > 0 else base

    def _mm_header(self, *, team: _MMTeam | None, ch: _MMChannel | None, me_id: str) -> str:
        header = '🟣 Mattermost'
        if not ch:
            return header

        ctype = str(ch.type or '').strip().upper()
        if ctype in {'D', 'G'}:
            title = self._mm_dm_title(ch, me_id=me_id)
            kind = 'Личка' if ctype == 'D' else 'Группа'
            if title:
                return f'🟣 Mattermost — {kind}: {_html_text(title)}'
            return f'🟣 Mattermost — {kind}'

        if team and team.display_name and ch.display_name:
            return f'🟣 Mattermost — {_html_text(team.display_name)} / {_html_text(ch.display_name)}'
        if ch.display_name:
            return f'🟣 Mattermost — {_html_text(ch.display_name)}'
        return header

    def _iter_channel_ids(self) -> list[str]:
        if self._driver is None:
            return []
        user_id = self._me_id()
        if not user_id:
            return []

        max_channels = int(self._cfg.mm_max_channels)
        extra_ids = list(getattr(self._cfg, 'mm_extra_channel_ids', []))

        # Explicit channel list wins (supports DMs too).
        if self._cfg.mm_channel_ids:
            selected: list[str] = []
            seen: set[str] = set()
            for x in [*self._cfg.mm_channel_ids, *extra_ids]:
                cid = str(x or '').strip()
                if cid and cid not in seen:
                    selected.append(cid)
                    seen.add(cid)
            return selected[:max_channels]

        explicit_team_scope = bool(self._cfg.mm_team_ids) or bool(self._cfg.mm_team_names)

        out: list[str] = []
        seen_ch: set[str] = set()

        def _add_channel_id(channel_id: str) -> None:
            if len(out) >= max_channels:
                return
            cid = str(channel_id or '').strip()
            if not cid or cid in seen_ch:
                return
            out.append(cid)
            seen_ch.add(cid)

        def _add_channel(ch: dict[str, Any]) -> None:
            cid = str(ch.get('id') or '').strip()
            if not cid:
                return
            _add_channel_id(cid)
            if cid not in self._channel_by_id and (
                str(ch.get('display_name') or '').strip() or str(ch.get('name') or '').strip()
            ):
                self._channel_by_id[cid] = _MMChannel(
                    id=cid,
                    display_name=str(ch.get('display_name') or '').strip() or str(ch.get('name') or '').strip(),
                    name=str(ch.get('name') or '').strip(),
                    team_id=str(ch.get('team_id') or '').strip(),
                    type=str(ch.get('type') or '').strip(),
                )

        team_ids: list[str] = []
        seen_team: set[str] = set()

        # Explicit team IDs.
        for raw in self._cfg.mm_team_ids:
            tid = str(raw or '').strip()
            if tid and tid not in seen_team:
                team_ids.append(tid)
                seen_team.add(tid)

        # Team names.
        if not team_ids:
            for raw in self._cfg.mm_team_names:
                name = str(raw or '').strip()
                if not name:
                    continue
                try:
                    team = self._driver.teams.get_team_by_name(name)
                except Exception:
                    continue
                if isinstance(team, dict):
                    tid = str(team.get('id') or '').strip()
                    if tid and tid not in seen_team:
                        team_ids.append(tid)
                        seen_team.add(tid)
                    if tid and str(team.get('name') or '').strip():
                        self._team_by_id[tid] = _MMTeam(
                            id=tid,
                            name=str(team.get('name') or '').strip(),
                            display_name=str(team.get('display_name') or '').strip()
                            or str(team.get('name') or '').strip(),
                        )

        # All user teams (default).
        if not explicit_team_scope and not team_ids:
            try:
                teams = self._driver.teams.get_user_teams(user_id)
            except Exception as e:
                if self._is_auth_error(e):
                    self._drop_driver()
                teams = []
            if isinstance(teams, list):
                for t in teams:
                    if not isinstance(t, dict):
                        continue
                    tid = str(t.get('id') or '').strip()
                    if not tid or tid in seen_team:
                        continue
                    team_ids.append(tid)
                    seen_team.add(tid)
                    if str(t.get('name') or '').strip():
                        self._team_by_id[tid] = _MMTeam(
                            id=tid,
                            name=str(t.get('name') or '').strip(),
                            display_name=str(t.get('display_name') or '').strip() or str(t.get('name') or '').strip(),
                        )

        # If team scope is explicit, prioritize team channels first (so they don't get drowned in DMs).
        if explicit_team_scope:
            for tid in team_ids:
                try:
                    channels = self._driver.channels.get_channels_for_user(user_id, tid)
                except Exception as e:
                    if self._is_auth_error(e):
                        self._drop_driver()
                    continue
                if not isinstance(channels, list):
                    continue
                for ch in channels:
                    if not isinstance(ch, dict):
                        continue
                    ctype = str(ch.get('type') or '').strip().upper()
                    if ctype in {'D', 'G'}:
                        continue
                    _add_channel(ch)
                    if len(out) >= max_channels:
                        return out

            for cid in extra_ids:
                _add_channel_id(cid)
                if len(out) >= max_channels:
                    return out

            # Include direct/group messages (best-effort).
            if bool(getattr(self._cfg, 'mm_include_dms', True)):
                try:
                    channels_all = self._driver.client.get(f'/users/{user_id}/channels')
                except Exception as e:
                    if self._is_auth_error(e):
                        self._drop_driver()
                    channels_all = []
                if isinstance(channels_all, list):
                    for ch in channels_all:
                        if not isinstance(ch, dict):
                            continue
                        ctype = str(ch.get('type') or '').strip().upper()
                        if ctype not in {'D', 'G'}:
                            continue
                        _add_channel(ch)
                        if len(out) >= max_channels:
                            return out
            return out

        for cid in extra_ids:
            _add_channel_id(cid)
            if len(out) >= max_channels:
                return out

        # Include direct/group messages (best-effort).
        if bool(getattr(self._cfg, 'mm_include_dms', True)):
            try:
                channels_all = self._driver.client.get(f'/users/{user_id}/channels')
            except Exception as e:
                if self._is_auth_error(e):
                    self._drop_driver()
                channels_all = []
            if isinstance(channels_all, list):
                for ch in channels_all:
                    if not isinstance(ch, dict):
                        continue
                    ctype = str(ch.get('type') or '').strip().upper()
                    if ctype not in {'D', 'G'}:
                        continue
                    _add_channel(ch)
                    if len(out) >= max_channels:
                        return out

        # Channels for teams.
        for tid in team_ids:
            try:
                channels = self._driver.channels.get_channels_for_user(user_id, tid)
            except Exception as e:
                if self._is_auth_error(e):
                    self._drop_driver()
                continue
            if not isinstance(channels, list):
                continue
            for ch in channels:
                if not isinstance(ch, dict):
                    continue
                _add_channel(ch)
                if len(out) >= max_channels:
                    return out
        return out

    def _fetch_unread_posts(self, *, channel_id: str) -> list[dict[str, Any]]:
        if self._driver is None:
            return []
        user_id = self._me_id()
        if not user_id:
            return []
        cid = str(channel_id or '').strip()
        if not cid:
            return []

        # Prefer explicit "unread posts" endpoint.
        try:
            unread = self._driver.posts.get_unread_posts_for_channel(user_id, cid)
        except Exception as e:
            if self._is_auth_error(e):
                self._drop_driver()
            unread = None

        posts: list[dict[str, Any]] = []
        if isinstance(unread, dict):
            posts_raw = unread.get('posts')
            order_raw = unread.get('order')
            if isinstance(posts_raw, dict) and isinstance(order_raw, list):
                for pid in order_raw:
                    key = str(pid or '').strip()
                    if not key:
                        continue
                    p = posts_raw.get(key)
                    if isinstance(p, dict):
                        posts.append(dict(p))
        if posts:
            return posts

        # Fallback: last_viewed_at -> posts since.
        last_viewed_at = 0
        try:
            member = self._driver.channels.get_channel_member(cid, user_id)
            if isinstance(member, dict):
                last_viewed_at = int(member.get('last_viewed_at') or 0)
        except Exception as e:
            if self._is_auth_error(e):
                self._drop_driver()
            last_viewed_at = 0
        if last_viewed_at <= 0:
            return []

        try:
            raw = self._driver.posts.get_posts_for_channel(cid, params={'since': int(last_viewed_at)})
        except Exception as e:
            if self._is_auth_error(e):
                self._drop_driver()
            return []
        if not isinstance(raw, dict):
            return []
        posts_raw = raw.get('posts')
        order_raw = raw.get('order')
        if not isinstance(posts_raw, dict) or not isinstance(order_raw, list):
            return []
        for pid in order_raw:
            key = str(pid or '').strip()
            if not key:
                continue
            p = posts_raw.get(key)
            if isinstance(p, dict):
                posts.append(dict(p))
        return posts

    def _post_permalink(self, *, team_name: str, post_id: str) -> str:
        base = self._base_http_url()
        tname = str(team_name or '').strip()
        pid = str(post_id or '').strip()
        if not base or not tname or not pid:
            return ''
        return f'{base}/{tname}/pl/{pid}'

    def tick(self, *, api: TelegramDeliveryAPI, state: BotState) -> None:
        if not self._cfg.mm_enabled:
            return

        now = time.time()
        force = False
        try:
            if (self._driver is None or self._me is None) and state.mm_has_mfa_token(max_age_seconds=120):
                force = True
        except Exception:
            force = False

        if (
            (not force)
            and self._last_poll_ts > 0
            and (now - self._last_poll_ts) < float(self._cfg.mm_poll_interval_seconds)
        ):
            return
        self._last_poll_ts = float(now)

        # Keep pending map in sync with outbox (prevents stuck pending if outbox was dropped).
        try:
            outbox = state.tg_outbox_snapshot()
            keep: dict[str, int] = {}
            for it in outbox:
                if not isinstance(it, dict):
                    continue
                meta = it.get('meta')
                if not isinstance(meta, dict):
                    continue
                if str(meta.get('kind') or '').strip() != 'mattermost':
                    continue
                cid = str(meta.get('channel_id') or '').strip()
                if not cid:
                    continue
                try:
                    ts = int(meta.get('up_to_ts') or 0)
                except Exception:
                    ts = 0
                if ts > 0:
                    keep[cid] = max(int(keep.get(cid) or 0), int(ts))
            state.mm_prune_pending(keep=keep)
        except Exception:
            pass

        if self._driver is None or self._me is None:
            if not self._connect(api=api, state=state):
                return

        reminders_chat_id, reminders_thread_id_raw = state.reminders_target()
        if int(reminders_chat_id or 0) == 0:
            return
        thread_id: int | None = int(reminders_thread_id_raw) if int(reminders_thread_id_raw or 0) != 0 else None

        user_id = self._me_id()
        if not user_id:
            return

        cutoff_ms = _now_ms() - int(max(1, int(self._cfg.mm_unread_minutes))) * 60 * 1000
        for channel_id in self._iter_channel_ids():
            cid = str(channel_id or '').strip()
            if not cid:
                continue

            sent_cutoff = state.mm_effective_cutoff_ts(cid)
            if sent_cutoff <= 0:
                # First time seeing this channel: set baseline to avoid forwarding a backlog.
                # Use `cutoff_ms` (now - unread_minutes), so we don't miss fresh messages that might become "stale" later.
                state.mm_mark_sent(channel_id=cid, up_to_ts=int(cutoff_ms))
                continue
            posts = self._fetch_unread_posts(channel_id=cid)
            if not posts:
                continue

            items, due_count, max_due_ts, up_to_ts = _mm_collect_posts_for_batch(
                posts, me_id=user_id, sent_cutoff=int(sent_cutoff), cutoff_ms=int(cutoff_ms)
            )
            if not items:
                continue
            # Trigger: only send when at least one unread post is older than MM_UNREAD_MINUTES.
            if int(due_count) <= 0:
                continue

            total_posts = len(items)
            shown_items = items[: int(self._cfg.mm_max_posts_per_channel)]

            # If we already replied in this channel after these posts, treat them as "read" and bump cutoff.
            # This helps when Mattermost considers a DM/channel "unread" until you open it, even if you replied elsewhere.
            if max_due_ts > 0 and self._driver is not None:
                my_latest_ts = 0
                try:
                    recent = self._driver.posts.get_posts_for_channel(cid, params={'page': 0, 'per_page': 30})
                    if isinstance(recent, dict):
                        posts_raw = recent.get('posts')
                        order_raw = recent.get('order')
                        if isinstance(posts_raw, dict) and isinstance(order_raw, list):
                            for pid in order_raw:
                                key = str(pid or '').strip()
                                if not key:
                                    continue
                                p_raw = posts_raw.get(key)
                                if not isinstance(p_raw, dict):
                                    continue
                                if str(p_raw.get('user_id') or '').strip() != user_id:
                                    continue
                                try:
                                    ts = int(p_raw.get('create_at') or 0)
                                except Exception:
                                    ts = 0
                                if ts > my_latest_ts:
                                    my_latest_ts = int(ts)
                except Exception as e:
                    if self._is_auth_error(e):
                        self._drop_driver()
                    my_latest_ts = 0
                if my_latest_ts > max_due_ts:
                    state.mm_mark_sent(channel_id=cid, up_to_ts=int(my_latest_ts))
                    continue

            ch = self._channel(cid)
            team: _MMTeam | None = self._team(ch.team_id) if (ch and ch.team_id) else None

            header = self._mm_header(team=team, ch=ch, me_id=user_id)

            base_url = self._base_http_url()
            base_url_link = f'<a href="{_html_attr(base_url)}">открыть</a>' if base_url else ''

            post_items: list[tuple[int, dict[str, Any], str]] = []
            for create_at, p in shown_items:
                author = str(p.get('user_id') or '').strip()
                uname = ''
                if author:
                    u = self._user(author)
                    uname = str((u or {}).get('username') or '').strip()
                label = uname or author or '?'
                msg = _clamp_line(str(p.get('message') or ''), int(self._cfg.mm_post_max_chars))
                age_s = _fmt_age_minutes(_now_ms() - int(create_at))
                pid = str(p.get('id') or '').strip()
                link = self._post_permalink(team_name=(team.name if team else ''), post_id=pid)
                if link:
                    post_items.append(
                        (
                            int(create_at),
                            dict(p),
                            f'- {age_s} {_html_text(label)}: {_html_text(msg)} (<a href="{_html_attr(link)}">пост</a>)',
                        )
                    )
                else:
                    post_items.append((int(create_at), dict(p), f'- {age_s} {_html_text(label)}: {_html_text(msg)}'))

            # Telegram max is 4096 chars; keep some margin and avoid chunking with HTML parse_mode.
            max_len = 3900

            def _build_text(lines: list[str]) -> str:
                return '\n'.join([x for x in lines if str(x or '').strip()])

            def _render(
                items: list[tuple[int, dict[str, Any], str]],
                *,
                total_posts: int = total_posts,
                due_count: int = due_count,
                header: str = header,
                base_url_link: str = base_url_link,
            ) -> str:
                shown = len(items)
                count_line = (
                    f'⏱️ Непрочитано: {int(total_posts)} (≥ {int(self._cfg.mm_unread_minutes)}м: {int(due_count)})'
                )
                if shown < int(total_posts):
                    count_line = f'{count_line} (показал {shown})'
                head_lines: list[str] = [header, count_line]
                if base_url_link:
                    head_lines.append(f'🔗 {base_url_link}')
                return _build_text(head_lines + [line for _, _, line in items])

            text = _render(post_items)
            while len(post_items) > 1 and len(text) > max_len:
                post_items.pop()
                text = _render(post_items)

            # As a last resort, clamp the last post line further to fit into a single message.
            if len(text) > max_len and post_items:
                ts, post, last_line = post_items[-1]
                # Try cutting only the message snippet, keeping the link intact.
                for new_max in (200, 150, 120, 100, 80, 60):
                    try:
                        raw_msg = _clamp_line(str(post.get('message') or ''), int(new_max))
                    except Exception:
                        raw_msg = ''
                    if not raw_msg:
                        continue
                    author = str(post.get('user_id') or '').strip()
                    uname = ''
                    if author:
                        u = self._user(author)
                        uname = str((u or {}).get('username') or '').strip()
                    label = uname or author or '?'
                    age_s = _fmt_age_minutes(_now_ms() - int(ts))
                    pid = str(post.get('id') or '').strip()
                    link = self._post_permalink(team_name=(team.name if team else ''), post_id=pid)
                    if link:
                        last_line = f'- {age_s} {_html_text(label)}: {_html_text(raw_msg)} (<a href="{_html_attr(link)}">пост</a>)'
                    else:
                        last_line = f'- {age_s} {_html_text(label)}: {_html_text(raw_msg)}'
                    post_items[-1] = (ts, post, last_line)
                    text = _render(post_items)
                    if len(text) <= max_len:
                        break

            if up_to_ts <= 0:
                continue

            meta = {'kind': 'mattermost', 'channel_id': cid, 'up_to_ts': int(up_to_ts)}
            coalesce_key = f'mm:{cid}'[:64]

            try:
                res = api.send_message(
                    chat_id=int(reminders_chat_id),
                    message_thread_id=thread_id,
                    text=text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    coalesce_key=coalesce_key,
                    meta=meta,
                )
            except Exception:
                continue

            deferred = bool(res.get('deferred')) if isinstance(res, dict) else False
            if deferred:
                state.mm_mark_pending(channel_id=cid, up_to_ts=int(up_to_ts))
            else:
                state.mm_mark_sent(channel_id=cid, up_to_ts=int(up_to_ts))
