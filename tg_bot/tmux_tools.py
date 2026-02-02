from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .config import _env_int, _load_dotenv
from .telegram_api import TelegramAPI


@dataclass(frozen=True)
class PaneInfo:
    session_name: str
    window_index: int
    window_name: str
    pane_index: int
    pane_id: str
    pane_active: bool
    pane_width: int
    pane_height: int
    pane_title: str

    @property
    def target(self) -> str:
        return f'{self.session_name}:{self.window_index}.{self.pane_index}'


def _run_tmux(args: Sequence[str], *, check: bool = True) -> str:
    proc = subprocess.run(
        ['tmux', *args],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    if check and proc.returncode != 0:
        stderr = (proc.stderr or '').strip()
        cmd = 'tmux ' + ' '.join(str(a) for a in args)
        raise RuntimeError(f'{cmd} failed (exit {proc.returncode}){": " + stderr if stderr else ""}')
    return proc.stdout or ''


def list_panes(session: str | None = None) -> list[PaneInfo]:
    fmt = (
        '#{session_name}\t#{window_index}\t#{window_name}\t#{pane_index}\t#{pane_id}\t#{pane_active}\t'
        '#{pane_width}\t#{pane_height}\t#{pane_title}'
    )
    args: list[str] = ['list-panes', '-a', '-F', fmt]
    if session:
        args.extend(['-t', str(session)])

    out = _run_tmux(args)
    panes: list[PaneInfo] = []
    for raw in out.splitlines():
        row = raw.rstrip('\n')
        if not row.strip():
            continue
        parts = row.split('\t')
        if len(parts) != 9:
            continue
        (
            session_name,
            window_index_s,
            window_name,
            pane_index_s,
            pane_id,
            pane_active_s,
            pane_width_s,
            pane_height_s,
            pane_title,
        ) = parts
        try:
            window_index = int(window_index_s.strip())
            pane_index = int(pane_index_s.strip())
            pane_width = int(pane_width_s.strip())
            pane_height = int(pane_height_s.strip())
        except ValueError:
            continue
        pane_active = pane_active_s.strip() == '1'
        panes.append(
            PaneInfo(
                session_name=session_name.strip(),
                window_index=window_index,
                window_name=window_name.strip(),
                pane_index=pane_index,
                pane_id=pane_id.strip(),
                pane_active=pane_active,
                pane_width=pane_width,
                pane_height=pane_height,
                pane_title=pane_title.strip(),
            )
        )

    panes.sort(key=lambda p: (p.session_name, p.window_index, p.pane_index))
    return panes


def _default_target() -> str:
    if os.getenv('TMUX'):
        pane_id = _run_tmux(['display-message', '-p', '#{pane_id}'], check=False).strip()
        if pane_id:
            return pane_id

    best_activity = -1
    best_pane = ''
    clients = _run_tmux(['list-clients', '-F', '#{client_activity}\t#{pane_id}'], check=False)
    for raw in clients.splitlines():
        row = raw.strip()
        if not row:
            continue
        try:
            activity_s, pane_id = row.split('\t', 1)
        except ValueError:
            continue
        try:
            activity = int(activity_s.strip())
        except ValueError:
            continue
        if activity >= best_activity and pane_id.strip():
            best_activity = activity
            best_pane = pane_id.strip()
    if best_pane:
        return best_pane

    panes = list_panes()
    if panes:
        return panes[0].pane_id

    raise RuntimeError('No tmux panes found (is tmux running?)')


def _pane_height(target: str) -> int:
    raw = _run_tmux(['display-message', '-p', '-t', target, '#{pane_height}'], check=False).strip()
    try:
        h = int(raw)
    except ValueError:
        return 25
    return max(1, h)


def capture_pane_text(target: str, *, lines: int | None = None) -> str:
    if lines is None:
        lines = _pane_height(target)
    lines = max(1, int(lines))
    return _run_tmux(['capture-pane', '-t', target, '-p', '-S', f'-{lines}', '-E', '-1'], check=False)


def snapshot_pane_text(target: str, *, lines: int | None = None) -> str:
    now = time.strftime('%Y-%m-%d %H:%M:%S %Z')
    out_lines = [
        f'tmux target: {target}',
        f'captured: {now}',
        '--------------------------------------------------',
        '',
        capture_pane_text(target, lines=lines).rstrip('\n'),
        '',
    ]
    return '\n'.join(out_lines).rstrip('\n') + '\n'


def snapshot_session_text(session: str, *, lines: int | None = None) -> str:
    now = time.strftime('%Y-%m-%d %H:%M:%S %Z')
    panes = list_panes(session)
    out: list[str] = [f'tmux session: {session}', f'captured: {now}', '']

    current_window: int | None = None
    current_window_name = ''
    for p in panes:
        if current_window != p.window_index:
            current_window = p.window_index
            current_window_name = p.window_name
            out.extend(
                [
                    '==================================================',
                    f'window {p.session_name}:{p.window_index} — {current_window_name}',
                    '==================================================',
                    '',
                ]
            )
        out.append(
            f'--- pane {p.session_name}:{p.window_index}.{p.pane_index} ({p.pane_id}) '
            f'active={1 if p.pane_active else 0} size={p.pane_width}x{p.pane_height} title={p.pane_title}'
        )
        out.append('')
        out.append(capture_pane_text(p.pane_id, lines=lines).rstrip('\n'))
        out.append('')
        out.append('')

    return '\n'.join(out).rstrip('\n') + '\n'


def _escape_imagemagick_text(text: str) -> str:
    # ImageMagick expands percent escapes; for terminal screenshots we want literal '%' symbols.
    return text.replace('%', '%%')


def render_text_to_png(text: str, out_path: Path, *, pointsize: int = 14) -> None:
    if not shutil.which('convert'):
        raise RuntimeError('ImageMagick `convert` not found in PATH')

    safe = _escape_imagemagick_text(text)
    # Safety: very large strings can hit OS argv limits. For tmux "visible" snapshots this should be fine.
    if len(safe) > 100_000:
        raise RuntimeError(f'Text too large to render ({len(safe)} chars); use .txt output or reduce captured lines')

    args = [
        'convert',
        '-encoding',
        'UTF-8',
        '-background',
        '#1e1e1e',
        '-fill',
        '#d4d4d4',
        '-font',
        'DejaVu-Sans-Mono',
        '-pointsize',
        str(int(pointsize)),
        '-interline-spacing',
        '2',
        '-bordercolor',
        '#1e1e1e',
        '-border',
        '12x12',
        f'label:{safe}',
        str(out_path),
    ]
    subprocess.run(args, check=True)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_tg_env(repo_root: Path) -> None:
    _load_dotenv(repo_root / 'tg_bot' / '.env')
    _load_dotenv(repo_root / '.env.tg_bot')


def _send_document(
    *,
    path: Path,
    caption: str,
    chat_id: int,
    message_thread_id: int | None,
) -> None:
    repo_root = _repo_root()
    _load_tg_env(repo_root)
    token = (os.getenv('TG_BOT_TOKEN') or '').strip()
    if not token:
        raise RuntimeError('Missing TG_BOT_TOKEN (expected in env or .env.tg_bot)')
    api = TelegramAPI(token=token)
    api.send_document(
        chat_id=int(chat_id),
        message_thread_id=message_thread_id,
        document_path=path,
        filename=path.name,
        caption=caption,
        parse_mode=None,
    )


def _send_message(*, text: str, chat_id: int, message_thread_id: int | None) -> None:
    repo_root = _repo_root()
    _load_tg_env(repo_root)
    token = (os.getenv('TG_BOT_TOKEN') or '').strip()
    if not token:
        raise RuntimeError('Missing TG_BOT_TOKEN (expected in env or .env.tg_bot)')
    api = TelegramAPI(token=token)
    api.send_message(chat_id=int(chat_id), message_thread_id=message_thread_id, text=text, parse_mode=None)


def _tg_params(chat_id: int | None, message_thread_id: int | None) -> tuple[int, int | None]:
    cid = int(chat_id or 0)
    if cid == 0:
        cid = _env_int('TG_OWNER_CHAT_ID', 0)
    if cid == 0:
        raise RuntimeError('Missing chat_id (use --chat-id or set TG_OWNER_CHAT_ID)')

    tid: int | None = None
    if message_thread_id is not None:
        t = int(message_thread_id)
        if t > 0:
            tid = t
    return cid, tid


def _truncate_for_tg(text: str, *, max_chars: int = 3500) -> str:
    s = text.replace('\r', '').strip()
    max_chars = max(0, int(max_chars))
    if max_chars and len(s) > max_chars:
        return s[: max(0, max_chars - 1)] + '…'
    return s


def _sanitize_basename(name: str) -> str:
    out = ''.join(ch if ch.isalnum() or ch in {'-', '_', '.'} else '_' for ch in name.strip())
    out = out.strip('._-') or 'tmux'
    return out[:80]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='tmux helpers: snapshots, tails, optional send-keys.')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_ls = sub.add_parser('ls', help='List panes (optionally within a session).')
    p_ls.add_argument('--session', default='', help='Session name/id (e.g. 3). Empty = all sessions.')

    p_snap = sub.add_parser('snap', help='Create a snapshot (.txt and optional .png) under tg_uploads/.')
    tgt = p_snap.add_mutually_exclusive_group(required=False)
    tgt.add_argument('--target', default='', help='tmux target (pane id like %%29 or target like 3:4.1).')
    tgt.add_argument('--session', default='', help='tmux session name/id (e.g. 3) to snapshot all panes.')
    p_snap.add_argument('--lines', type=int, default=0, help='Capture last N lines (0 = visible height).')
    p_snap.add_argument('--out-dir', default='tg_uploads', help='Output directory (default: tg_uploads).')
    p_snap.add_argument('--basename', default='', help='Base output name (without extension).')
    p_snap.add_argument('--no-png', action='store_true', help='Do not render PNG (text only).')
    p_snap.add_argument('--send', action='store_true', help='Send resulting file to Telegram (requires token).')
    p_snap.add_argument('--chat-id', type=int, default=0, help='Telegram chat id (default: TG_OWNER_CHAT_ID).')
    p_snap.add_argument('--thread-id', type=int, default=0, help='Telegram message_thread_id (optional).')
    p_snap.add_argument('--caption', default='', help='Telegram caption (optional).')

    p_tail = sub.add_parser('tail', help='Print tail from a pane (optionally send as Telegram message).')
    p_tail.add_argument('--target', default='', help='tmux target (default: active pane).')
    p_tail.add_argument('--lines', type=int, default=0, help='Capture last N lines (0 = visible height).')
    p_tail.add_argument('--send', action='store_true', help='Send as Telegram message (requires token).')
    p_tail.add_argument('--chat-id', type=int, default=0, help='Telegram chat id (default: TG_OWNER_CHAT_ID).')
    p_tail.add_argument('--thread-id', type=int, default=0, help='Telegram message_thread_id (optional).')

    p_keys = sub.add_parser('send-keys', help='DANGEROUS: send keys to a pane (requires --dangerous).')
    p_keys.add_argument('--target', default='', help='tmux target (default: active pane).')
    p_keys.add_argument('--text', default='', help='Text to type into the pane.')
    p_keys.add_argument('--enter', action='store_true', help='Send Enter after text/keys.')
    p_keys.add_argument('--dangerous', action='store_true', help='Required acknowledgement for send-keys.')
    p_keys.add_argument('--dry-run', action='store_true', help='Print the tmux command without executing.')
    p_keys.add_argument('keys', nargs='*', help='Additional tmux key names, e.g. C-c Enter.')

    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        if args.cmd == 'ls':
            panes = list_panes(args.session.strip() or None)
            for p in panes:
                print(
                    f'{p.target} {p.pane_id} active={1 if p.pane_active else 0} '
                    f'size={p.pane_width}x{p.pane_height} title={p.pane_title}'
                )
            return 0

        if args.cmd == 'tail':
            target = args.target.strip() or _default_target()
            lines = int(args.lines) if int(args.lines) > 0 else None
            text = capture_pane_text(target, lines=lines)
            if args.send:
                cid, tid = _tg_params(args.chat_id, args.thread_id if int(args.thread_id) > 0 else None)
                msg = _truncate_for_tg(f'tmux {target}\n\n{text}')
                _send_message(text=msg, chat_id=cid, message_thread_id=tid)
            else:
                sys.stdout.write(text)
            return 0

        if args.cmd == 'snap':
            out_dir = Path(str(args.out_dir)).expanduser()
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = time.strftime('%Y%m%d-%H%M%S')

            lines = int(args.lines) if int(args.lines) > 0 else None
            target_s = str(args.target or '').strip()
            session_s = str(args.session or '').strip()

            if session_s:
                text = snapshot_session_text(session_s, lines=lines)
                base_default = f'tmux-session{session_s}-{ts}'
            else:
                target = target_s or _default_target()
                text = snapshot_pane_text(target, lines=lines)
                base_default = f'tmux-pane{target}-{ts}'

            base = _sanitize_basename(str(args.basename or '').strip() or base_default)
            txt_path = out_dir / f'{base}.txt'
            txt_path.write_text(text, encoding='utf-8')

            png_path: Path | None = None
            if not bool(args.no_png):
                try:
                    png_path = out_dir / f'{base}.png'
                    render_text_to_png(text, png_path, pointsize=14)
                except Exception as e:
                    png_path = None
                    print(f'WARN: failed to render PNG: {e}', file=sys.stderr)

            print(str(txt_path))
            if png_path is not None:
                print(str(png_path))

            if args.send:
                cid, tid = _tg_params(args.chat_id, args.thread_id if int(args.thread_id) > 0 else None)
                caption = str(args.caption or '').strip() or base
                send_path = png_path or txt_path
                _send_document(path=send_path, caption=caption, chat_id=cid, message_thread_id=tid)
            return 0

        if args.cmd == 'send-keys':
            if not bool(args.dangerous):
                raise RuntimeError('Refusing to send keys without --dangerous')
            target = args.target.strip() or _default_target()
            keys: list[str] = []
            if str(args.text or ''):
                keys.append(str(args.text))
            for k in list(args.keys or []):
                ks = str(k).strip()
                if ks:
                    keys.append(ks)
            if bool(args.enter):
                keys.append('Enter')
            if not keys:
                raise RuntimeError('Nothing to send: provide --text and/or key names')

            cmd = ['tmux', 'send-keys', '-t', target, *keys]
            if args.dry_run:
                print(' '.join(cmd))
                return 0
            subprocess.run(cmd, check=True)
            return 0

    except BrokenPipeError:
        return 0
    except Exception as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 1

    return 1


if __name__ == '__main__':
    raise SystemExit(main())
