from __future__ import annotations

import json
import os
import re
import selectors
import shutil
import signal
import subprocess
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Lock
from typing import Any


@dataclass(frozen=True)
class CodexProfile:
    name: str
    codex_home: Path
    sandbox: str | None = None
    full_auto: bool = False


class CodexRunner:
    def __init__(
        self,
        *,
        codex_bin: str,
        repo_root: Path,
        model: str | None,
        timeout_seconds: int,
        chat_profile: CodexProfile,
        auto_profile: CodexProfile,
        router_profile: CodexProfile,
        danger_profile: CodexProfile | None = None,
        log_path: Path,
        resume_cache_path: Path | None = None,
    ) -> None:
        self.codex_bin = codex_bin
        self.repo_root = repo_root
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.chat_profile = chat_profile
        self.auto_profile = auto_profile
        self.router_profile = router_profile
        self.danger_profile = danger_profile
        self.log_path = log_path
        self.default_resume_cache_path = resume_cache_path or (
            self.repo_root / 'logs' / 'tg-bot' / 'codex-resume-cache.json'
        )
        self._resume_cache_by_path: dict[str, dict[str, dict[str, str]]] = {}
        # NOTE: `_lock` protects shared state (resume cache, reset) but MUST NOT serialize full runs.
        self._lock = Lock()
        self._proc_lock = Lock()
        self._cancel_requested_by_session: dict[str, Event] = {}
        self._current_procs_by_session: dict[str, subprocess.Popen[str]] = {}
        self._current_meta_by_session: dict[str, dict[str, object]] = {}

    _UUID_RE = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

    def _log(self, line: str) -> None:
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open('a', encoding='utf-8') as f:
            f.write(f'[{ts}] {line}\n')

    def log_note(self, line: str) -> None:
        """Append an arbitrary note to the Codex log (tg-bot runtime)."""
        try:
            self._log(f'NOTE: {line}')
        except Exception:
            pass

    def _atomic_write(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + '.tmp')
        tmp.write_text(content, encoding='utf-8')
        os.replace(tmp, path)

    def _default_global_home(self) -> Path:
        # Best-effort: where Codex usually stores auth/config.
        # We intentionally do NOT honor parent process CODEX_HOME here; that's for the bot itself.
        return Path.home() / '.codex'

    def _ensure_codex_home(self, home: Path) -> None:
        home.mkdir(parents=True, exist_ok=True)

        global_home = self._default_global_home()

        # Copy/symlink auth.json so the bot doesn't require re-login.
        # (If user prefers, they can manage auth/config in this custom CODEX_HOME themselves.)
        for fname in ('auth.json', 'config.toml', 'AGENTS.md'):
            target = home / fname
            if target.exists():
                continue
            src = global_home / fname
            if not src.exists():
                continue
            try:
                # Symlink if possible, fallback to copy.
                target.symlink_to(src)
            except Exception:
                try:
                    shutil.copy2(src, target)
                except Exception:
                    pass

        # Also keep global skills available (optional): link ~/.codex/skills -> CODEX_HOME/skills if missing.
        skills_src = global_home / 'skills'
        skills_dst = home / 'skills'
        if not skills_dst.exists() and skills_src.exists():
            try:
                skills_dst.symlink_to(skills_src, target_is_directory=True)
            except Exception:
                # Don't copy potentially large folders by default.
                pass

    def _resume_cache_path_for_repo_root(self, repo_root: Path) -> Path:
        rr = repo_root.resolve()
        try:
            if rr == self.repo_root.resolve():
                return self.default_resume_cache_path
        except Exception:
            pass
        return rr / 'logs' / 'tg-bot' / 'codex-resume-cache.json'

    def _codex_home_for_profile(self, profile: CodexProfile, repo_root: Path) -> Path:
        rr = repo_root.resolve()
        try:
            if rr == self.repo_root.resolve():
                return profile.codex_home
        except Exception:
            pass
        name = (profile.name or '').strip() or 'chat'
        return rr / '.codex-tg' / name

    def _build_base_cmd(
        self,
        profile: CodexProfile,
        *,
        repo_root: Path,
        out_last_message: Path,
        sandbox_override: str | None = None,
        config_overrides: dict[str, object] | None = None,
    ) -> list[str]:
        return self._build_base_cmd2(
            profile,
            repo_root=repo_root,
            out_last_message=out_last_message,
            sandbox_override=sandbox_override,
            config_overrides=config_overrides,
            json_output=False,
        )

    def _build_base_cmd2(
        self,
        profile: CodexProfile,
        *,
        repo_root: Path,
        out_last_message: Path,
        sandbox_override: str | None = None,
        config_overrides: dict[str, object] | None = None,
        json_output: bool = False,
        dangerously_bypass_permission_and_sandbox: bool = False,
    ) -> list[str]:
        if shutil.which(self.codex_bin) is None and not Path(self.codex_bin).exists():
            raise RuntimeError(f'codex binary not found: {self.codex_bin}')

        def _toml_value(v: object) -> str | None:
            if v is None:
                return None
            if isinstance(v, bool):
                return 'true' if v else 'false'
            if isinstance(v, int):
                return str(v)
            if isinstance(v, float):
                return str(v)
            if isinstance(v, (list, tuple)):
                # JSON arrays are valid TOML for our use cases (strings/ints/bools).
                items: list[object] = []
                for item in v:
                    if item is None:
                        items.append('')
                    elif isinstance(item, (bool, int, float, str)):
                        items.append(item)
                    else:
                        items.append(str(item))
                return json.dumps(items, ensure_ascii=False)
            if isinstance(v, str):
                # TOML basic strings use the same escaping rules as JSON for common cases.
                return json.dumps(v)
            # Fallback: force a string literal.
            return json.dumps(str(v))

        cmd: list[str] = [self.codex_bin]

        if dangerously_bypass_permission_and_sandbox:
            cmd += ['--dangerously-bypass-approvals-and-sandbox']

        if config_overrides:
            for k in sorted(config_overrides.keys()):
                key = str(k or '').strip()
                if not key:
                    continue
                v = _toml_value(config_overrides.get(k))
                if v is None:
                    continue
                cmd += ['-c', f'{key}={v}']

        cmd += ['exec']

        # Exec flags.
        cmd += ['--cd', str(repo_root)]
        cmd += ['--color', 'never']
        cmd += ['--output-last-message', str(out_last_message)]
        if json_output:
            cmd += ['--json']

        if self.model:
            cmd += ['--model', self.model]

        # Automation preset
        sb = (sandbox_override or '').strip() or None
        if sb:
            cmd += ['--sandbox', sb]
        elif profile.full_auto and not dangerously_bypass_permission_and_sandbox:
            cmd += ['--full-auto']
        elif profile.sandbox:
            cmd += ['--sandbox', profile.sandbox]

        return cmd

    def _normalize_session_key(self, *, chat_id: int | None, session_key: str | None) -> str | None:
        sk = str(session_key or '').strip()
        if sk:
            return sk[:128]
        if chat_id is None:
            return None
        try:
            return str(int(chat_id))
        except Exception:
            return None

    def _cancel_event_for_session(self, session_key: str) -> Event:
        key = str(session_key or '').strip()
        if not key:
            return Event()
        with self._proc_lock:
            ev = self._cancel_requested_by_session.get(key)
            if ev is None:
                ev = Event()
                self._cancel_requested_by_session[key] = ev
            return ev

    def _set_current_proc(
        self,
        proc: subprocess.Popen[str],
        *,
        session_key: str,
        chat_id: int | None,
        profile_name: str,
        cmd: Sequence[str],
    ) -> None:
        sk = str(session_key or '').strip()
        if not sk:
            return
        with self._proc_lock:
            self._current_procs_by_session[sk] = proc
            self._current_meta_by_session[sk] = {
                'chat_id': int(chat_id) if chat_id is not None else None,
                'profile': str(profile_name or ''),
                'cmd': ' '.join(map(str, cmd))[:800],
                'started_ts': float(time.time()),
            }

    def _clear_current_proc(self, *, session_key: str, proc: subprocess.Popen[str]) -> None:
        sk = str(session_key or '').strip()
        if not sk:
            return
        with self._proc_lock:
            cur = self._current_procs_by_session.get(sk)
            if cur is proc:
                self._current_procs_by_session.pop(sk, None)
                self._current_meta_by_session.pop(sk, None)

    def cancel_current_run(self, *, chat_id: int | None = None, session_key: str | None = None) -> dict[str, object]:
        """Best-effort cancel a running `codex exec` subprocess (SIGTERM -> SIGKILL).

        Intended to be called from the polling thread (e.g. `/pause`) to interrupt a long Codex run.
        """
        target_key = self._normalize_session_key(chat_id=chat_id, session_key=session_key)

        with self._proc_lock:
            candidates: list[tuple[float, str, subprocess.Popen[str], dict[str, object]]] = []
            for sk, proc in self._current_procs_by_session.items():
                meta = dict(self._current_meta_by_session.get(sk) or {})
                started_raw = meta.get('started_ts')
                if isinstance(started_raw, bool):
                    started_ts = 0.0
                elif isinstance(started_raw, (int, float)):
                    started_ts = float(started_raw)
                elif isinstance(started_raw, str):
                    try:
                        started_ts = float(started_raw.strip() or 0.0)
                    except Exception:
                        started_ts = 0.0
                else:
                    started_ts = 0.0
                if target_key:
                    if sk == target_key:
                        candidates = [(started_ts, sk, proc, meta)]
                        break
                    continue
                if chat_id is not None:
                    prefix = f'{int(chat_id)}:'
                    if sk == str(int(chat_id)) or sk.startswith(prefix):
                        candidates.append((started_ts, sk, proc, meta))

            if not candidates and target_key and chat_id is not None:
                # Backward compatibility: if caller provides only chat_id but sessions are scope keys,
                # cancel the most recently started one for that chat.
                prefix = f'{int(chat_id)}:'
                for sk, proc in self._current_procs_by_session.items():
                    if sk == str(int(chat_id)) or sk.startswith(prefix):
                        meta = dict(self._current_meta_by_session.get(sk) or {})
                        started_raw = meta.get('started_ts')
                        if isinstance(started_raw, bool):
                            started_ts = 0.0
                        elif isinstance(started_raw, (int, float)):
                            started_ts = float(started_raw)
                        elif isinstance(started_raw, str):
                            try:
                                started_ts = float(started_raw.strip() or 0.0)
                            except Exception:
                                started_ts = 0.0
                        else:
                            started_ts = 0.0
                        candidates.append((started_ts, sk, proc, meta))

            if not candidates:
                return {'ok': False, 'reason': 'no_active_process'}

            # Choose the most recently started process (best-effort).
            candidates.sort(key=lambda t: t[0], reverse=True)
            _, sk, proc, meta = candidates[0]

        if proc.poll() is not None:
            return {'ok': False, 'reason': 'already_finished', 'session_key': sk}

        self._cancel_event_for_session(sk).set()

        pid = int(getattr(proc, 'pid', 0) or 0)
        terminated = False
        try:
            # We spawn Codex with start_new_session=True, so proc.pid is also the process group id.
            if pid > 0 and hasattr(os, 'killpg'):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except Exception:
                    proc.terminate()
            else:
                proc.terminate()
            terminated = True
        except Exception:
            terminated = False

        if terminated:
            try:
                proc.wait(timeout=1.5)
            except subprocess.TimeoutExpired:
                try:
                    if pid > 0 and hasattr(os, 'killpg'):
                        os.killpg(pid, signal.SIGKILL)
                    else:
                        proc.kill()
                except Exception:
                    pass
            except Exception:
                pass

        profile_name = str(meta.get('profile') or '')
        cmd_s = str(meta.get('cmd') or '')
        try:
            self._log(f'CANCEL pid={pid} session_key={sk} chat_id={chat_id or 0} profile={profile_name} cmd={cmd_s}')
        except Exception:
            pass

        return {
            'ok': True,
            'pid': pid,
            'session_key': sk,
            'chat_id': int(chat_id or 0),
            'profile': profile_name,
        }

    def _run_cmd(
        self,
        cmd: Sequence[str],
        *,
        prompt: str,
        env: dict[str, str],
        cwd: Path,
        chat_id: int | None,
        session_key: str | None,
        profile_name: str,
        cancel_event: Event | None = None,
    ) -> tuple[int, str, str]:
        self._log(f'RUN: {" ".join(map(str, cmd))}')
        try:
            proc = subprocess.Popen(
                list(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                cwd=str(cwd),
                stdin=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                start_new_session=True,
            )
            sk = self._normalize_session_key(chat_id=chat_id, session_key=session_key) or ''
            if sk:
                self._set_current_proc(proc, session_key=sk, chat_id=chat_id, profile_name=profile_name, cmd=cmd)
        except FileNotFoundError as e:
            return (127, '', str(e))
        except Exception as e:
            return (1, '', repr(e))

        try:
            try:
                out, err = proc.communicate(input=prompt, timeout=float(self.timeout_seconds))
            except subprocess.TimeoutExpired:
                pid = int(getattr(proc, 'pid', 0) or 0)
                try:
                    if pid > 0 and hasattr(os, 'killpg'):
                        os.killpg(pid, signal.SIGKILL)
                    else:
                        proc.kill()
                except Exception:
                    pass
                try:
                    out2, err2 = proc.communicate(timeout=2.0)
                except Exception:
                    out2, err2 = ('', '')
                out = (out2 or '') + '\n'
                err = (err2 or '') + f'\nTimeout after {self.timeout_seconds}s\n'
                return (124, out, err)
            except Exception as e:
                return (1, '', repr(e))

            return (int(proc.returncode or 0), out or '', err or '')
        finally:
            sk = self._normalize_session_key(chat_id=chat_id, session_key=session_key) or ''
            if sk:
                self._clear_current_proc(session_key=sk, proc=proc)

    def _run_cmd_stream_json(
        self,
        cmd: Sequence[str],
        *,
        prompt: str,
        env: dict[str, str],
        cwd: Path,
        chat_id: int | None,
        session_key: str | None,
        profile_name: str,
        on_event: Callable[[dict[str, Any]], None] | None,
        cancel_event: Event | None = None,
    ) -> tuple[int, str]:
        """Run command and stream stdout as JSONL events (best-effort).

        Returns (exit_code, combined_output).
        """
        self._log(f'RUN(stream): {" ".join(map(str, cmd))}')

        try:
            proc = subprocess.Popen(
                list(cmd),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                cwd=str(cwd),
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,
                start_new_session=True,
            )
            sk = self._normalize_session_key(chat_id=chat_id, session_key=session_key) or ''
            if sk:
                self._set_current_proc(proc, session_key=sk, chat_id=chat_id, profile_name=profile_name, cmd=cmd)
        except FileNotFoundError as e:
            return (127, str(e))
        except Exception as e:
            return (1, repr(e))

        try:
            if proc.stdin is not None:
                proc.stdin.write(prompt)
                proc.stdin.close()
        except Exception:
            pass

        if proc.stdout is None:
            try:
                try:
                    code = proc.wait(timeout=float(self.timeout_seconds))
                except subprocess.TimeoutExpired:
                    return (124, f'Timeout after {self.timeout_seconds}s')
                return (int(code), '')
            finally:
                if sk:
                    self._clear_current_proc(session_key=sk, proc=proc)

        sel: selectors.BaseSelector | None = selectors.DefaultSelector()
        assert sel is not None
        try:
            sel.register(proc.stdout, selectors.EVENT_READ)
        except Exception:
            sel = None

        out_chunks: list[str] = []
        start_ts = time.time()
        last_line_ts = 0.0

        try:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    try:
                        pid = int(getattr(proc, 'pid', 0) or 0)
                        if pid > 0 and hasattr(os, 'killpg'):
                            os.killpg(pid, signal.SIGTERM)
                        else:
                            proc.terminate()
                    except Exception:
                        pass

                # Timeout guard (subprocess.run-like behavior).
                if (time.time() - start_ts) > float(self.timeout_seconds):
                    try:
                        pid = int(getattr(proc, 'pid', 0) or 0)
                        if pid > 0 and hasattr(os, 'killpg'):
                            os.killpg(pid, signal.SIGKILL)
                        else:
                            proc.kill()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=2.0)
                    except Exception:
                        pass
                    out_chunks.append(f'\nTimeout after {self.timeout_seconds}s\n')
                    return (124, ''.join(out_chunks))

                if proc.poll() is not None:
                    # Drain any remaining output.
                    try:
                        rest = proc.stdout.read()
                        if rest:
                            out_chunks.append(rest)
                    except Exception:
                        pass
                    break

                try:
                    if sel is None:
                        line = proc.stdout.readline()
                        if not line:
                            time.sleep(0.05)
                            continue
                        out_chunks.append(line)
                    else:
                        ready = sel.select(timeout=0.5)
                        if not ready:
                            continue
                        line = proc.stdout.readline()
                        if not line:
                            continue
                        out_chunks.append(line)
                except Exception:
                    time.sleep(0.05)
                    continue

                if on_event is None:
                    continue

                # Best-effort: parse only JSON lines; ignore noisy logs.
                raw = line.strip()
                if not raw.startswith('{'):
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue

                # Rate limit callbacks to avoid spamming Telegram edits.
                now_ts = time.time()
                if now_ts - last_line_ts < 0.25:
                    continue
                last_line_ts = now_ts
                try:
                    on_event(obj)
                except Exception:
                    pass

            try:
                code = int(proc.wait(timeout=2.0))
            except Exception:
                code = int(proc.returncode or 0)

            return (code, ''.join(out_chunks))
        finally:
            sk = self._normalize_session_key(chat_id=chat_id, session_key=session_key) or ''
            if sk:
                self._clear_current_proc(session_key=sk, proc=proc)

    def _read_last_message_file(self, path: Path) -> str | None:
        try:
            if not path.exists():
                return None
            txt = path.read_text(encoding='utf-8', errors='replace').strip()
            return txt or None
        except Exception:
            return None

    def _env_for_profile(self, *, codex_home: Path, env_policy: str, repo_root: Path) -> dict[str, str]:
        env = dict(os.environ)
        if (env_policy or '').strip().lower() in {'restricted', 'public'}:
            env = self._restrict_env(env)
        env['CODEX_HOME'] = str(codex_home)
        try:
            rr = repo_root.resolve()
            mem_root = rr / '.mcp'
            mem_root.mkdir(parents=True, exist_ok=True)
            desired_path = mem_root / 'server-memory.jsonl'
            old_default_path = rr / '.codex-tg' / 'mcp-memory.jsonl'

            current = (env.get('MEMORY_FILE_PATH') or '').strip()
            if not current:
                env['MEMORY_FILE_PATH'] = str(desired_path)
            else:
                try:
                    if Path(current).resolve() == old_default_path.resolve():
                        env['MEMORY_FILE_PATH'] = str(desired_path)
                except Exception:
                    pass
        except Exception:
            pass
        return env

    def _restrict_env(self, env: dict[str, str]) -> dict[str, str]:
        """Best-effort secret scrubbing for multi-tenant chats."""
        out = dict(env)
        for k in list(out.keys()):
            if k.startswith('JIRA_'):
                out.pop(k, None)
                continue
            if k.startswith('TG_'):
                out.pop(k, None)
                continue
            if k.startswith('AZURE_OPENAI_'):
                out.pop(k, None)
                continue
            if k in {
                'OPENAI_API_KEY',
                'OPENAI_ORG_ID',
                'OPENAI_PROJECT',
                'ANTHROPIC_API_KEY',
                'GITHUB_TOKEN',
            }:
                out.pop(k, None)
        return out

    def _resume_cache_key(self, resume_cache_path: Path) -> str:
        try:
            return str(resume_cache_path.resolve())
        except Exception:
            return str(resume_cache_path)

    def _load_resume_cache_locked(self, *, resume_cache_path: Path) -> dict[str, dict[str, str]]:
        """Load per-session-key session_id cache.

        Format (v1):
          {"version": 1, "profiles": {"chat": {"<session_key>": "<uuid>"}, "auto": {...}}}
        """
        key = self._resume_cache_key(resume_cache_path)
        existing = self._resume_cache_by_path.get(key)
        if existing is not None:
            return existing

        cache: dict[str, dict[str, str]] = {}
        try:
            if not resume_cache_path.exists():
                self._resume_cache_by_path[key] = {}
                return self._resume_cache_by_path[key]
            raw = resume_cache_path.read_text(encoding='utf-8', errors='replace')
            data = json.loads(raw or '{}')
        except Exception:
            self._resume_cache_by_path[key] = {}
            return self._resume_cache_by_path[key]

        profiles = None
        if isinstance(data, dict):
            v_profiles = data.get('profiles')
            profiles = v_profiles if isinstance(v_profiles, dict) else data

        if not isinstance(profiles, dict):
            self._resume_cache_by_path[key] = {}
            return self._resume_cache_by_path[key]

        for profile_name, mapping in profiles.items():
            if not isinstance(profile_name, str) or not isinstance(mapping, dict):
                continue
            out: dict[str, str] = {}
            for raw_key, session_id in mapping.items():
                if not isinstance(raw_key, str) or not raw_key.strip():
                    continue
                if not isinstance(session_id, str):
                    continue
                sid = session_id.strip()
                if not sid:
                    continue
                if not self._UUID_RE.fullmatch(sid):
                    # Codex session ids are UUIDs; ignore invalid entries.
                    continue
                sk = raw_key.strip()
                # Keep numeric keys stable across loads (best-effort).
                if sk.lstrip('-').isdigit():
                    try:
                        sk = str(int(sk))
                    except Exception:
                        continue
                else:
                    sk = sk[:128]
                out[sk] = sid
            if out:
                cache[profile_name] = out

        self._resume_cache_by_path[key] = cache
        return self._resume_cache_by_path[key]

    def _save_resume_cache_locked(self, *, resume_cache_path: Path) -> None:
        key = self._resume_cache_key(resume_cache_path)
        cache = self._resume_cache_by_path.get(key) or {}
        payload = {'version': 1, 'profiles': cache}
        try:
            self._atomic_write(resume_cache_path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        except Exception:
            pass

    def _get_session_id_locked(self, *, profile_name: str, session_key: str, resume_cache_path: Path) -> str | None:
        cache = self._load_resume_cache_locked(resume_cache_path=resume_cache_path)
        sk = str(session_key or '').strip()
        if not sk:
            return None
        return (cache.get(profile_name) or {}).get(sk)

    def _set_session_id_locked(
        self,
        *,
        profile_name: str,
        session_key: str,
        session_id: str,
        resume_cache_path: Path,
    ) -> None:
        sid = (session_id or '').strip()
        if not self._UUID_RE.fullmatch(sid):
            return
        sk = str(session_key or '').strip()
        if not sk:
            return
        cache = self._load_resume_cache_locked(resume_cache_path=resume_cache_path)
        bucket = cache.get(profile_name)
        if bucket is None:
            bucket = {}
            cache[profile_name] = bucket
        if bucket.get(sk) == sid:
            return
        bucket[sk] = sid
        self._resume_cache_by_path[self._resume_cache_key(resume_cache_path)] = cache
        self._save_resume_cache_locked(resume_cache_path=resume_cache_path)

    def _clear_resume_cache_locked(self, *, resume_cache_path: Path | None = None) -> None:
        if resume_cache_path is None:
            self._resume_cache_by_path.clear()
            resume_cache_path = self.default_resume_cache_path
        else:
            self._resume_cache_by_path.pop(self._resume_cache_key(resume_cache_path), None)
        try:
            if resume_cache_path.exists():
                resume_cache_path.unlink()
        except Exception:
            self._save_resume_cache_locked(resume_cache_path=resume_cache_path)

    def _best_effort_latest_session_id(self, codex_home: Path) -> str | None:
        """Best-effort: find the most recently modified rollout file id in CODEX_HOME/sessions."""
        sessions_dir = codex_home / 'sessions'
        if not sessions_dir.exists():
            return None

        latest: Path | None = None
        latest_mtime = -1.0
        try:
            candidates: Iterable[Path] = sessions_dir.rglob('rollout-*.jsonl')
        except Exception:
            candidates = []

        for p in candidates:
            try:
                st = p.stat()
            except Exception:
                continue
            if st.st_mtime > latest_mtime:
                latest_mtime = float(st.st_mtime)
                latest = p

        if latest is None:
            return None

        m = re.search(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})', latest.name)
        sid = (m.group(1) if m else '').strip()
        return sid if self._UUID_RE.fullmatch(sid) else None

    def _error_tail(self, *errs: str) -> str:
        tail_lines: list[str] = []
        for e in errs:
            if not e:
                continue
            tail_lines.extend(str(e).strip().splitlines())
        tail = '\n'.join(tail_lines[-20:]).strip()
        return tail or 'unknown error'

    def reset(self) -> None:
        """Reset telegram-only Codex sessions (clears CODEX_HOME folders for bot profiles)."""

        with self._lock:
            homes = [self.chat_profile.codex_home, self.auto_profile.codex_home, self.router_profile.codex_home]
            if self.danger_profile is not None:
                homes.append(self.danger_profile.codex_home)
            for p in homes:
                try:
                    if p.exists():
                        shutil.rmtree(p)
                except Exception:
                    pass
            self._clear_resume_cache_locked()

    def reset_session(
        self,
        *,
        chat_id: int | None = None,
        session_key: str | None = None,
        repo_root: Path | None = None,
    ) -> dict[str, object]:
        """Reset one per-scope Codex session (clears resume cache entries only; keeps CODEX_HOME)."""
        sk = self._normalize_session_key(chat_id=chat_id, session_key=session_key)
        if not sk:
            return {'ok': False, 'reason': 'missing_session_key'}

        rr = (repo_root or self.repo_root).resolve()
        resume_cache_path = self._resume_cache_path_for_repo_root(rr)

        removed_by_profile: dict[str, str] = {}
        with self._lock:
            cache = self._load_resume_cache_locked(resume_cache_path=resume_cache_path)
            for profile_name in list(cache.keys()):
                bucket = cache.get(profile_name)
                if not isinstance(bucket, dict):
                    continue
                sid = bucket.pop(sk, None)
                if isinstance(sid, str) and sid.strip():
                    removed_by_profile[profile_name] = sid.strip()
                if not bucket:
                    cache.pop(profile_name, None)
            self._resume_cache_by_path[self._resume_cache_key(resume_cache_path)] = cache
            self._save_resume_cache_locked(resume_cache_path=resume_cache_path)

        try:
            self._cancel_event_for_session(sk).clear()
        except Exception:
            pass

        return {
            'ok': True,
            'session_key': sk,
            'removed_profiles': sorted(removed_by_profile.keys()),
            'resume_cache_path': str(resume_cache_path),
        }

    def run(
        self,
        *,
        prompt: str,
        automation: bool,
        chat_id: int | None = None,
        session_key: str | None = None,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        """Run Codex with per-scope resume when possible; fallback to a fresh session."""
        return self.run_with_progress(
            prompt=prompt,
            automation=automation,
            chat_id=chat_id,
            session_key=session_key,
            on_event=None,
            repo_root=repo_root,
            env_policy=env_policy,
            config_overrides=config_overrides,
        )

    def run_dangerous(
        self,
        *,
        prompt: str,
        chat_id: int | None = None,
        session_key: str | None = None,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        """Run Codex with --dangerously-bypass-approvals-and-sandbox (separate CODEX_HOME when configured)."""
        return self.run_dangerous_with_progress(
            prompt=prompt,
            chat_id=chat_id,
            session_key=session_key,
            on_event=None,
            repo_root=repo_root,
            env_policy=env_policy,
            config_overrides=config_overrides,
        )

    def run_with_progress(
        self,
        *,
        prompt: str,
        automation: bool,
        chat_id: int | None,
        session_key: str | None = None,
        on_event: Callable[[dict[str, Any]], None] | None,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        env_overrides: dict[str, str | None] | None = None,
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        """Run Codex and optionally stream JSON events via callback."""

        profile = self.auto_profile if automation else self.chat_profile
        return self._run_profile_with_progress(
            profile=profile,
            prompt=prompt,
            chat_id=chat_id,
            session_key=session_key,
            on_event=on_event,
            sandbox_override=None,
            dangerously_bypass_permission_and_sandbox=False,
            repo_root=repo_root or self.repo_root,
            env_policy=env_policy,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )

    def run_dangerous_with_progress(
        self,
        *,
        prompt: str,
        chat_id: int | None,
        session_key: str | None = None,
        on_event: Callable[[dict[str, Any]], None] | None,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        env_overrides: dict[str, str | None] | None = None,
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        profile = self.danger_profile or self.auto_profile
        return self._run_profile_with_progress(
            profile=profile,
            prompt=prompt,
            chat_id=chat_id,
            session_key=session_key,
            on_event=on_event,
            sandbox_override=None,
            dangerously_bypass_permission_and_sandbox=True,
            repo_root=repo_root or self.repo_root,
            env_policy=env_policy,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )

    def _run_profile_with_progress(
        self,
        *,
        profile: CodexProfile,
        prompt: str,
        chat_id: int | None,
        session_key: str | None = None,
        on_event: Callable[[dict[str, Any]], None] | None,
        sandbox_override: str | None,
        dangerously_bypass_permission_and_sandbox: bool,
        repo_root: Path,
        env_policy: str,
        env_overrides: dict[str, str | None] | None,
        config_overrides: dict[str, object] | None,
    ) -> str:
        """Run Codex under a given profile, with optional resume and optional JSON progress streaming."""
        sk = self._normalize_session_key(chat_id=chat_id, session_key=session_key)
        cancel_event = self._cancel_event_for_session(sk) if sk else None
        if cancel_event is not None:
            cancel_event.clear()

        repo_root = repo_root.resolve()
        profile_home = self._codex_home_for_profile(profile, repo_root)
        resume_cache_path = self._resume_cache_path_for_repo_root(repo_root)

        out_dir = repo_root / 'logs' / 'tg-bot'
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_sk = ''
        if sk:
            safe_sk = re.sub(r'[^A-Za-z0-9._-]+', '_', sk)[:80]
        out_last = out_dir / (
            f'codex-last-{profile.name}-{safe_sk}.txt' if safe_sk else f'codex-last-{profile.name}.txt'
        )

        # Ensure CODEX_HOME and load resume token under lock (but do not serialize the whole run).
        with self._lock:
            self._ensure_codex_home(profile_home)
            cached_session_id = (
                self._get_session_id_locked(
                    profile_name=profile.name, session_key=sk, resume_cache_path=resume_cache_path
                )
                if sk
                else None
            )

        env = self._env_for_profile(codex_home=profile_home, env_policy=env_policy, repo_root=repo_root)
        if env_overrides:
            for k, v in env_overrides.items():
                key = str(k or '').strip()
                if not key:
                    continue
                if v is None:
                    env.pop(key, None)
                else:
                    env[key] = str(v)
        json_output = on_event is not None

        resume_code = -1
        resume_stdout = ''
        resume_stderr = ''

        if cached_session_id:
            cmd_resume = self._build_base_cmd2(
                profile,
                repo_root=repo_root,
                out_last_message=out_last,
                sandbox_override=sandbox_override,
                config_overrides=config_overrides,
                json_output=json_output,
                dangerously_bypass_permission_and_sandbox=dangerously_bypass_permission_and_sandbox,
            ) + ['resume', cached_session_id, '-']
            if json_output:
                resume_code, combined = self._run_cmd_stream_json(
                    cmd_resume,
                    prompt=prompt,
                    env=env,
                    cwd=repo_root,
                    chat_id=chat_id,
                    session_key=sk,
                    profile_name=profile.name,
                    on_event=on_event,
                    cancel_event=cancel_event,
                )
                msg = self._read_last_message_file(out_last)
                resume_stdout, resume_stderr = (combined, '')
            else:
                resume_code, resume_stdout, resume_stderr = self._run_cmd(
                    cmd_resume,
                    prompt=prompt,
                    env=env,
                    cwd=repo_root,
                    chat_id=chat_id,
                    session_key=sk,
                    profile_name=profile.name,
                    cancel_event=cancel_event,
                )
                msg = self._read_last_message_file(out_last) or resume_stdout.strip()

            if cancel_event is not None and cancel_event.is_set():
                self._log(f'CANCELLED resume profile={profile.name} session_key={sk or ""}')
                return '⏸️ Остановил текущий запуск Codex (/pause).'

            if resume_code == 0 and msg:
                if sk:
                    latest = self._best_effort_latest_session_id(profile_home) or cached_session_id
                    if latest:
                        with self._lock:
                            self._set_session_id_locked(
                                profile_name=profile.name,
                                session_key=sk,
                                session_id=latest,
                                resume_cache_path=resume_cache_path,
                            )
                return msg

        # Fallback: start a brand new session with this prompt.
        cmd_fresh = self._build_base_cmd2(
            profile,
            repo_root=repo_root,
            out_last_message=out_last,
            sandbox_override=sandbox_override,
            config_overrides=config_overrides,
            json_output=json_output,
            dangerously_bypass_permission_and_sandbox=dangerously_bypass_permission_and_sandbox,
        ) + ['-']
        if json_output:
            code2, combined2 = self._run_cmd_stream_json(
                cmd_fresh,
                prompt=prompt,
                env=env,
                cwd=repo_root,
                chat_id=chat_id,
                session_key=sk,
                profile_name=profile.name,
                on_event=on_event,
                cancel_event=cancel_event,
            )
            msg2 = self._read_last_message_file(out_last)
            stdout2, stderr2 = (combined2, '')
        else:
            code2, stdout2, stderr2 = self._run_cmd(
                cmd_fresh,
                prompt=prompt,
                env=env,
                cwd=repo_root,
                chat_id=chat_id,
                session_key=sk,
                profile_name=profile.name,
                cancel_event=cancel_event,
            )
            msg2 = self._read_last_message_file(out_last) or stdout2.strip()

        if cancel_event is not None and cancel_event.is_set():
            self._log(f'CANCELLED fresh profile={profile.name} session_key={sk or ""}')
            return '⏸️ Остановил текущий запуск Codex (/pause).'

        if code2 == 0 and msg2:
            if sk:
                latest_session_id = self._best_effort_latest_session_id(profile_home)
                if latest_session_id:
                    with self._lock:
                        self._set_session_id_locked(
                            profile_name=profile.name,
                            session_key=sk,
                            session_id=latest_session_id,
                            resume_cache_path=resume_cache_path,
                        )
            return msg2

        tail = self._error_tail(stderr2, stdout2, resume_stderr, resume_stdout)
        self._log(f'FAIL code={resume_code}/{code2} tail={tail[:400]}')
        return f'[codex error]\n{tail}'

    def run_followup(
        self,
        *,
        prompt: str,
        automation: bool,
        chat_id: int | None,
        session_key: str | None = None,
        sandbox_override: str,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        env_overrides: dict[str, str | None] | None = None,
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        """Run Codex follow-up inside the SAME CODEX_HOME, but force a safe sandbox.

        This is used for inline buttons under Codex answers ("короче", "план", etc.),
        where we want to reuse the session context but avoid accidental repo modifications.
        """

        profile = self.auto_profile if automation else self.chat_profile

        return self._run_profile_with_progress(
            profile=profile,
            prompt=prompt,
            chat_id=chat_id,
            session_key=session_key,
            on_event=None,
            sandbox_override=sandbox_override,
            dangerously_bypass_permission_and_sandbox=False,
            repo_root=repo_root or self.repo_root,
            env_policy=env_policy,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )

    def run_followup_by_profile_name(
        self,
        *,
        prompt: str,
        profile_name: str,
        chat_id: int | None,
        session_key: str | None = None,
        sandbox_override: str,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        env_overrides: dict[str, str | None] | None = None,
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        """Run a safe follow-up using the last profile's CODEX_HOME (best-effort)."""

        profile = self.profile_by_name(profile_name)
        if profile is None:
            profile = self.auto_profile if (profile_name or '').strip() in {'auto', 'danger'} else self.chat_profile

        return self._run_profile_with_progress(
            profile=profile,
            prompt=prompt,
            chat_id=chat_id,
            session_key=session_key,
            on_event=None,
            sandbox_override=sandbox_override,
            dangerously_bypass_permission_and_sandbox=False,
            repo_root=repo_root or self.repo_root,
            env_policy=env_policy,
            env_overrides=env_overrides,
            config_overrides=config_overrides,
        )

    def profile_by_name(self, profile_name: str) -> CodexProfile | None:
        name = (profile_name or '').strip()
        if not name:
            return None
        if name == self.chat_profile.name:
            return self.chat_profile
        if name == self.auto_profile.name:
            return self.auto_profile
        if name == self.router_profile.name:
            return self.router_profile
        if self.danger_profile is not None and name == self.danger_profile.name:
            return self.danger_profile
        return None

    def classify(
        self,
        *,
        prompt: str,
        session_key: str | None = None,
        repo_root: Path | None = None,
        env_policy: str = 'full',
        config_overrides: dict[str, object] | None = None,
    ) -> str:
        """Run Codex in read-only mode to classify routing/permissions.

        This uses a separate CODEX_HOME (router_profile) to avoid polluting chat sessions.

        By default, router classification runs with a lightweight reasoning effort:
        `-c model_reasoning_effort="low"`. Callers can override via `config_overrides`.
        """

        profile = self.router_profile
        rr = (repo_root or self.repo_root).resolve()
        profile_home = self._codex_home_for_profile(profile, rr)
        with self._lock:
            self._ensure_codex_home(profile_home)

        out_dir = rr / 'logs' / 'tg-bot'
        out_dir.mkdir(parents=True, exist_ok=True)
        out_last = out_dir / f'codex-last-{profile.name}.txt'

        env = self._env_for_profile(codex_home=profile_home, env_policy=env_policy, repo_root=rr)

        effective_overrides: dict[str, object] = {'model_reasoning_effort': 'low'}
        if config_overrides:
            effective_overrides.update(config_overrides)

        cmd = self._build_base_cmd(
            profile, repo_root=rr, out_last_message=out_last, config_overrides=effective_overrides
        ) + ['-']

        sk = self._normalize_session_key(chat_id=None, session_key=session_key)
        cancel_event = self._cancel_event_for_session(sk) if sk else None
        if cancel_event is not None:
            cancel_event.clear()

        code, stdout, stderr = self._run_cmd(
            cmd,
            prompt=prompt,
            env=env,
            cwd=rr,
            chat_id=None,
            session_key=sk,
            profile_name=profile.name,
            cancel_event=cancel_event,
        )
        msg = self._read_last_message_file(out_last) or stdout.strip()

        if cancel_event is not None and cancel_event.is_set():
            self._log('CANCELLED router classify')
            return '⏸️ Остановил текущий запуск Codex (/pause).'

        if code == 0 and msg:
            return msg

        tail = self._error_tail(stderr)
        self._log(f'ROUTER FAIL code={code} tail={tail[:400]}')
        return f'[codex error]\n{tail}'
