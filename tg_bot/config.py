from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from re import Pattern


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


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v in {'1', 'true', 'yes', 'y', 'on'}:
        return True
    if v in {'0', 'false', 'no', 'n', 'off'}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v.strip().replace(',', '.'))
    except ValueError:
        return default


def _env_list_int(name: str) -> list[int]:
    v = os.getenv(name)
    if not v:
        return []
    out: list[int] = []
    for part in v.split(','):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            continue
    return out


def _env_list_str(name: str) -> list[str]:
    v = os.getenv(name)
    if not v:
        return []
    out: list[str] = []
    for part in v.split(','):
        s = part.strip()
        if s:
            out.append(s)
    return out


def _clean_increasing_positive(values: list[int]) -> list[int]:
    out: list[int] = []
    last = -1
    for v in values:
        try:
            n = int(v)
        except Exception:
            continue
        if n <= 0:
            continue
        if n <= last:
            continue
        out.append(n)
        last = n
    return out


@dataclass(frozen=True)
class BotConfig:
    repo_root: Path

    # Telegram
    tg_token: str
    tg_bot_api_local_url: str
    tg_bot_api_remote_url: str
    tg_bot_api_prefer_local: bool
    tg_bot_api_probe_seconds: int
    tg_poll_timeout_seconds: int
    tg_max_parallel_jobs: int
    tg_allowed_user_ids: list[int]
    tg_allowed_chat_ids: list[int]
    tg_owner_chat_id: int
    tg_ack_enabled: bool
    tg_ack_include_queue: bool
    tg_typing_enabled: bool
    tg_typing_interval_seconds: int
    tg_progress_edit_enabled: bool
    tg_progress_edit_interval_seconds: int
    tg_codex_parse_mode: str
    tg_uploads_dir: Path
    tg_upload_max_bytes: int
    tg_voice_auto_transcribe: bool
    tg_voice_echo_transcript: bool
    tg_voice_transcribe_timeout_seconds: int
    tg_voice_apply_typos: bool
    tg_voice_route_choice_menu_enabled: bool
    tg_voice_route_choice_timeout_seconds: int

    state_path: Path

    # Multi-chat KB workspaces (non-owner chats)
    tg_workspaces_dir: Path

    # Watcher
    watch_interval_seconds: int
    watch_work_hours: str
    watch_include_weekends: bool
    watch_idle_minutes: int
    watch_ack_minutes: int
    watch_idle_stage_minutes: list[int]
    watch_reminders_file: Path
    watch_reminder_grace_minutes: int
    watch_reminder_broadcast_chat_ids: list[int]
    watch_reminders_include_weekends: bool

    # Mattermost (optional)
    mm_enabled: bool
    mm_url: str
    mm_scheme: str
    mm_port: int
    mm_basepath: str
    mm_verify: bool
    mm_timeout_seconds: int
    mm_auth_mode: str  # token | login | auto
    mm_token: str
    mm_login_id: str
    mm_password: str
    mm_team_names: list[str]
    mm_team_ids: list[str]
    mm_channel_ids: list[str]
    mm_extra_channel_ids: list[str]
    mm_include_dms: bool
    mm_cache_session_token: bool
    mm_unread_minutes: int
    mm_poll_interval_seconds: int
    mm_max_channels: int
    mm_max_posts_per_channel: int
    mm_post_max_chars: int

    # Gentle mode ("щадящий режим")
    gentle_default_minutes: int
    gentle_ping_cooldown_minutes: int
    gentle_auto_idle_minutes: int
    gentle_auto_mute_window_minutes: int
    gentle_auto_mute_count: int
    gentle_stage_cap: int

    # History / context injection
    history_max_events: int
    history_context_limit: int
    history_entry_max_chars: int

    # Codex follow-up safety
    codex_followup_sandbox: str

    # Router
    router_mode: str  # codex | heuristic | hybrid
    router_force_write_prefix: str
    router_force_read_prefix: str
    router_force_danger_prefix: str
    router_confidence_threshold: float
    router_debug: bool
    router_dangerous_auto: bool
    router_min_profile: str  # read | write | danger

    # Codex
    codex_bin: str
    codex_model: str | None
    codex_timeout_seconds: int

    codex_chat_sandbox: str
    codex_auto_full_auto: bool
    codex_router_sandbox: str

    codex_home_chat: Path
    codex_home_auto: Path
    codex_home_router: Path
    codex_home_danger: Path

    automation_patterns: Pattern[str]  # legacy fallback / hybrid

    @staticmethod
    def default_repo_root() -> Path:
        # If tg_bot/ sits at repo root, parents[1] is repo root.
        here = Path(__file__).resolve()
        return Path(os.getenv('TG_REPO_ROOT', str(here.parents[1]))).resolve()

    @classmethod
    def from_env(cls) -> BotConfig:
        repo_root = cls.default_repo_root()

        # Load optional env files (if present).
        _load_dotenv(repo_root / 'tg_bot' / '.env')
        _load_dotenv(repo_root / '.env.tg_bot')

        tg_token = (os.getenv('TG_BOT_TOKEN') or '').strip()
        if not tg_token:
            raise RuntimeError('TG_BOT_TOKEN is required')

        tg_bot_api_local_url = (os.getenv('TG_BOT_API_LOCAL_URL') or 'http://127.0.0.1:8081').strip()
        tg_bot_api_remote_url = (os.getenv('TG_BOT_API_REMOTE_URL') or 'https://api.telegram.org').strip()
        tg_bot_api_prefer_local = _env_bool('TG_BOT_API_PREFER_LOCAL', False)
        tg_bot_api_probe_seconds = max(60, min(3600, _env_int('TG_BOT_API_PROBE_SECONDS', 300)))

        tg_poll_timeout_seconds = _env_int('TG_POLL_TIMEOUT_SECONDS', 25)
        tg_max_parallel_jobs = _env_int('TG_MAX_PARALLEL_JOBS', 5)
        tg_max_parallel_jobs = max(1, min(20, int(tg_max_parallel_jobs)))
        tg_allowed_user_ids = _env_list_int('TG_ALLOWED_USER_IDS')
        tg_allowed_chat_ids = _env_list_int('TG_ALLOWED_CHAT_IDS')
        tg_owner_chat_id = _env_int('TG_OWNER_CHAT_ID', 0)
        tg_ack_enabled = _env_bool('TG_ACK_ENABLED', True)
        tg_ack_include_queue = _env_bool('TG_ACK_INCLUDE_QUEUE', True)
        tg_typing_enabled = _env_bool('TG_TYPING_ENABLED', True)
        tg_typing_interval_seconds = max(2, min(10, _env_int('TG_TYPING_INTERVAL_SECONDS', 4)))
        tg_progress_edit_enabled = _env_bool('TG_PROGRESS_EDIT_ENABLED', True)
        tg_progress_edit_interval_seconds = max(10, min(300, _env_int('TG_PROGRESS_EDIT_INTERVAL_SECONDS', 20)))
        tg_codex_parse_mode = (os.getenv('TG_CODEX_PARSE_MODE', 'HTML') or '').strip()
        if tg_codex_parse_mode.lower() in {'0', 'none', 'off', 'false'}:
            tg_codex_parse_mode = ''

        uploads_dir_raw = (os.getenv('TG_UPLOADS_DIR') or '').strip()
        if uploads_dir_raw:
            p = Path(uploads_dir_raw)
            tg_uploads_dir = (p if p.is_absolute() else (repo_root / p)).resolve()
        else:
            tg_uploads_dir = (repo_root / 'tg_uploads').resolve()
        tg_upload_max_mb = max(0, _env_int('TG_UPLOAD_MAX_MB', 50))
        tg_upload_max_bytes = int(tg_upload_max_mb) * 1024 * 1024

        tg_voice_auto_transcribe = _env_bool('TG_VOICE_AUTO_TRANSCRIBE', False)
        tg_voice_echo_transcript = _env_bool('TG_VOICE_ECHO_TRANSCRIPT', False)
        tg_voice_transcribe_timeout_seconds = max(10, min(300, _env_int('TG_VOICE_TRANSCRIBE_TIMEOUT_SECONDS', 300)))
        tg_voice_apply_typos = _env_bool('TG_VOICE_APPLY_TYPO_GLOSSARY', True)
        tg_voice_route_choice_menu_enabled = _env_bool('TG_VOICE_ROUTE_CHOICE_MENU_ENABLED', True)
        tg_voice_route_choice_timeout_seconds = max(30, min(300, _env_int('TG_VOICE_ROUTE_CHOICE_TIMEOUT_SECONDS', 30)))

        state_path = Path(os.getenv('TG_BOT_STATE_PATH', str(repo_root / 'logs' / 'tg-bot' / 'state.json')))
        tg_workspaces_dir = Path(
            os.getenv('TG_WORKSPACES_DIR', str(repo_root / 'logs' / 'tg-bot' / 'workspaces'))
        ).resolve()

        # Watch config
        watch_interval_seconds = _env_int('WATCH_INTERVAL_SECONDS', 180)
        watch_work_hours = os.getenv('WATCH_WORK_HOURS', '09:30-19:00').strip()
        watch_include_weekends = _env_bool('WATCH_INCLUDE_WEEKENDS', False)
        watch_idle_minutes = _env_int('WATCH_IDLE_MINUTES', 120)
        watch_ack_minutes = _env_int('WATCH_ACK_MINUTES', 20)

        # Stage schedule: absolute minutes since last touch.
        stage_minutes = _clean_increasing_positive(_env_list_int('WATCH_IDLE_STAGE_MINUTES'))
        if not stage_minutes:
            # Default: 5 stages
            # 1) idle_minutes
            # 2) idle_minutes + ack_minutes
            # 3) +30m
            # 4) +90m
            # 5) +180m
            stage_minutes = _clean_increasing_positive(
                [
                    watch_idle_minutes,
                    watch_idle_minutes + max(5, watch_ack_minutes),
                    watch_idle_minutes + max(5, watch_ack_minutes) + 30,
                    watch_idle_minutes + max(5, watch_ack_minutes) + 90,
                    watch_idle_minutes + max(5, watch_ack_minutes) + 180,
                ]
            )

        watch_reminders_file = Path(
            os.getenv('WATCH_REMINDERS_FILE', str(repo_root / 'notes' / 'work' / 'reminders.md'))
        )
        watch_reminder_grace_minutes = _env_int('WATCH_REMINDER_GRACE_MINUTES', 90)
        watch_reminder_broadcast_chat_ids = _env_list_int('WATCH_REMINDER_BROADCAST_CHAT_IDS')
        watch_reminders_include_weekends = _env_bool('WATCH_REMINDERS_INCLUDE_WEEKENDS', watch_include_weekends)

        # Mattermost config (optional)
        mm_enabled = _env_bool('MM_ENABLED', False)
        mm_url = (os.getenv('MM_URL') or '').strip()
        mm_scheme = (os.getenv('MM_SCHEME') or 'https').strip() or 'https'
        mm_port = _env_int('MM_PORT', 443)
        mm_basepath = (os.getenv('MM_BASEPATH') or '').strip()
        mm_verify = _env_bool('MM_VERIFY', True)
        mm_timeout_seconds = max(1, min(120, _env_int('MM_TIMEOUT_SECONDS', 20)))
        mm_auth_mode = (os.getenv('MM_AUTH_MODE') or 'auto').strip().lower()
        if mm_auth_mode in {'pat', 'token'}:
            mm_auth_mode = 'token'
        elif mm_auth_mode in {'login', 'password'}:
            mm_auth_mode = 'login'
        elif mm_auth_mode in {'', 'auto'}:
            mm_auth_mode = 'auto'
        else:
            mm_auth_mode = 'auto'
        mm_token = (os.getenv('MM_TOKEN') or '').strip()
        mm_login_id = (os.getenv('MM_LOGIN_ID') or '').strip()
        mm_password = (os.getenv('MM_PASSWORD') or '').strip()
        mm_team_names = _env_list_str('MM_TEAM_NAMES')
        mm_team_ids = _env_list_str('MM_TEAM_IDS')
        mm_channel_ids = _env_list_str('MM_CHANNEL_IDS')
        mm_extra_channel_ids = _env_list_str('MM_EXTRA_CHANNEL_IDS')
        mm_include_dms = _env_bool('MM_INCLUDE_DMS', True)
        mm_cache_session_token = _env_bool('MM_CACHE_SESSION_TOKEN', True)
        mm_unread_minutes = max(1, min(24 * 60, _env_int('MM_UNREAD_MINUTES', 15)))
        mm_poll_interval_seconds = max(
            10, min(24 * 60 * 60, _env_int('MM_POLL_INTERVAL_SECONDS', watch_interval_seconds))
        )
        mm_max_channels = max(1, min(5000, _env_int('MM_MAX_CHANNELS', 100)))
        mm_max_posts_per_channel = max(1, min(200, _env_int('MM_MAX_POSTS_PER_CHANNEL', 20)))
        mm_post_max_chars = max(50, min(4000, _env_int('MM_POST_MAX_CHARS', 400)))

        # Gentle mode ("щадящий режим")
        gentle_default_minutes = _env_int('GENTLE_DEFAULT_MINUTES', 480)
        gentle_ping_cooldown_minutes = _env_int('GENTLE_PING_COOLDOWN_MINUTES', 90)
        gentle_auto_idle_minutes = _env_int('GENTLE_AUTO_IDLE_MINUTES', 240)
        gentle_auto_mute_window_minutes = _env_int('GENTLE_AUTO_MUTE_WINDOW_MINUTES', 180)
        gentle_auto_mute_count = _env_int('GENTLE_AUTO_MUTE_COUNT', 3)
        gentle_stage_cap = _env_int('GENTLE_STAGE_CAP', 4)

        # History / context injection
        history_max_events = _env_int('HISTORY_MAX_EVENTS', 120)
        history_context_limit = _env_int('HISTORY_CONTEXT_LIMIT', 30)
        history_entry_max_chars = _env_int('HISTORY_ENTRY_MAX_CHARS', 500)

        # Router config
        router_mode = (os.getenv('ROUTER_MODE') or 'hybrid').strip().lower()
        if router_mode not in {'codex', 'heuristic', 'hybrid'}:
            router_mode = 'hybrid'

        router_force_write_prefix = (os.getenv('ROUTER_FORCE_WRITE_PREFIX') or '!').strip() or '!'
        router_force_read_prefix = (os.getenv('ROUTER_FORCE_READ_PREFIX') or '?').strip() or '?'
        router_force_danger_prefix = (os.getenv('ROUTER_FORCE_DANGEROUS_PREFIX') or '∆').strip() or '∆'
        router_confidence_threshold = _env_float('ROUTER_CONFIDENCE_THRESHOLD', 0.6)
        router_debug = _env_bool('ROUTER_DEBUG', False) or _env_bool('TG_ROUTER_DEBUG', False)
        router_dangerous_auto = _env_bool('ROUTER_DANGEROUS_AUTO', False) or _env_bool('TG_DANGEROUS_AUTO', False)
        router_min_profile = (os.getenv('ROUTER_MIN_PROFILE') or os.getenv('TG_MIN_PROFILE') or 'read').strip().lower()
        if router_min_profile in {'dangerous', 'danger'}:
            router_min_profile = 'danger'
        if router_min_profile not in {'read', 'write', 'danger'}:
            router_min_profile = 'read'

        # Codex config
        codex_bin = os.getenv('CODEX_BIN', 'codex').strip()
        codex_model = (os.getenv('CODEX_MODEL') or '').strip() or None
        codex_timeout_seconds = _env_int('CODEX_TIMEOUT_SECONDS', 900)

        codex_chat_sandbox = os.getenv('CODEX_CHAT_SANDBOX', 'read-only').strip() or 'read-only'
        codex_auto_full_auto = _env_bool('CODEX_AUTO_FULL_AUTO', True)
        codex_router_sandbox = os.getenv('CODEX_ROUTER_SANDBOX', 'read-only').strip() or 'read-only'
        codex_followup_sandbox = os.getenv('CODEX_FOLLOWUP_SANDBOX', 'read-only').strip() or 'read-only'

        codex_home_chat = Path(os.getenv('CODEX_HOME_CHAT', str(repo_root / '.codex-tg' / 'chat'))).resolve()
        codex_home_auto = Path(os.getenv('CODEX_HOME_AUTO', str(repo_root / '.codex-tg' / 'auto'))).resolve()
        codex_home_router = Path(os.getenv('CODEX_HOME_ROUTER', str(repo_root / '.codex-tg' / 'router'))).resolve()
        codex_home_danger = Path(os.getenv('CODEX_HOME_DANGER', str(repo_root / '.codex-tg' / 'danger'))).resolve()

        # Legacy pattern routing (still useful as a fallback).
        default_patterns = r'(закончим\s+день|закрыть\s+день|конец\s+дня|end\s*of\s*day|day\s*end|eod)'
        patterns_raw = os.getenv('CODEX_AUTOMATION_PATTERNS', default_patterns).strip() or default_patterns
        try:
            compiled = re.compile(patterns_raw, flags=re.IGNORECASE)
        except re.error:
            compiled = re.compile(default_patterns, flags=re.IGNORECASE)

        return cls(
            repo_root=repo_root,
            tg_token=tg_token,
            tg_bot_api_local_url=tg_bot_api_local_url,
            tg_bot_api_remote_url=tg_bot_api_remote_url,
            tg_bot_api_prefer_local=tg_bot_api_prefer_local,
            tg_bot_api_probe_seconds=tg_bot_api_probe_seconds,
            tg_poll_timeout_seconds=tg_poll_timeout_seconds,
            tg_max_parallel_jobs=tg_max_parallel_jobs,
            tg_allowed_user_ids=tg_allowed_user_ids,
            tg_allowed_chat_ids=tg_allowed_chat_ids,
            tg_owner_chat_id=tg_owner_chat_id,
            tg_ack_enabled=tg_ack_enabled,
            tg_ack_include_queue=tg_ack_include_queue,
            tg_typing_enabled=tg_typing_enabled,
            tg_typing_interval_seconds=tg_typing_interval_seconds,
            tg_progress_edit_enabled=tg_progress_edit_enabled,
            tg_progress_edit_interval_seconds=tg_progress_edit_interval_seconds,
            tg_codex_parse_mode=tg_codex_parse_mode,
            tg_uploads_dir=tg_uploads_dir,
            tg_upload_max_bytes=tg_upload_max_bytes,
            tg_voice_auto_transcribe=tg_voice_auto_transcribe,
            tg_voice_echo_transcript=tg_voice_echo_transcript,
            tg_voice_transcribe_timeout_seconds=tg_voice_transcribe_timeout_seconds,
            tg_voice_apply_typos=tg_voice_apply_typos,
            tg_voice_route_choice_menu_enabled=tg_voice_route_choice_menu_enabled,
            tg_voice_route_choice_timeout_seconds=tg_voice_route_choice_timeout_seconds,
            state_path=state_path,
            tg_workspaces_dir=tg_workspaces_dir,
            watch_interval_seconds=watch_interval_seconds,
            watch_work_hours=watch_work_hours,
            watch_include_weekends=watch_include_weekends,
            watch_idle_minutes=watch_idle_minutes,
            watch_ack_minutes=watch_ack_minutes,
            watch_idle_stage_minutes=stage_minutes,
            watch_reminders_file=watch_reminders_file,
            watch_reminder_grace_minutes=watch_reminder_grace_minutes,
            watch_reminder_broadcast_chat_ids=watch_reminder_broadcast_chat_ids,
            watch_reminders_include_weekends=watch_reminders_include_weekends,
            mm_enabled=mm_enabled,
            mm_url=mm_url,
            mm_scheme=mm_scheme,
            mm_port=mm_port,
            mm_basepath=mm_basepath,
            mm_verify=mm_verify,
            mm_timeout_seconds=mm_timeout_seconds,
            mm_auth_mode=mm_auth_mode,
            mm_token=mm_token,
            mm_login_id=mm_login_id,
            mm_password=mm_password,
            mm_team_names=mm_team_names,
            mm_team_ids=mm_team_ids,
            mm_channel_ids=mm_channel_ids,
            mm_extra_channel_ids=mm_extra_channel_ids,
            mm_include_dms=mm_include_dms,
            mm_cache_session_token=mm_cache_session_token,
            mm_unread_minutes=mm_unread_minutes,
            mm_poll_interval_seconds=mm_poll_interval_seconds,
            mm_max_channels=mm_max_channels,
            mm_max_posts_per_channel=mm_max_posts_per_channel,
            mm_post_max_chars=mm_post_max_chars,
            gentle_default_minutes=gentle_default_minutes,
            gentle_ping_cooldown_minutes=gentle_ping_cooldown_minutes,
            gentle_auto_idle_minutes=gentle_auto_idle_minutes,
            gentle_auto_mute_window_minutes=gentle_auto_mute_window_minutes,
            gentle_auto_mute_count=gentle_auto_mute_count,
            gentle_stage_cap=gentle_stage_cap,
            history_max_events=history_max_events,
            history_context_limit=history_context_limit,
            history_entry_max_chars=history_entry_max_chars,
            router_mode=router_mode,
            router_force_write_prefix=router_force_write_prefix,
            router_force_read_prefix=router_force_read_prefix,
            router_force_danger_prefix=router_force_danger_prefix,
            router_confidence_threshold=router_confidence_threshold,
            router_debug=router_debug,
            router_dangerous_auto=router_dangerous_auto,
            router_min_profile=router_min_profile,
            codex_bin=codex_bin,
            codex_model=codex_model,
            codex_timeout_seconds=codex_timeout_seconds,
            codex_chat_sandbox=codex_chat_sandbox,
            codex_auto_full_auto=codex_auto_full_auto,
            codex_router_sandbox=codex_router_sandbox,
            codex_followup_sandbox=codex_followup_sandbox,
            codex_home_chat=codex_home_chat,
            codex_home_auto=codex_home_auto,
            codex_home_router=codex_home_router,
            codex_home_danger=codex_home_danger,
            automation_patterns=compiled,
        )
