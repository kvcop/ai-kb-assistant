import json
import tempfile
import unittest
from pathlib import Path

from tg_bot.codex_runner import CodexProfile, CodexRunner


class TestCodexRunnerCmd(unittest.TestCase):
    def _mk_runner(self, *, root: Path, model: str | None = 'gpt-test') -> CodexRunner:
        chat = CodexProfile(name='chat', codex_home=root / '.codex-tg' / 'chat', sandbox='read-only', full_auto=False)
        auto = CodexProfile(name='auto', codex_home=root / '.codex-tg' / 'auto', sandbox=None, full_auto=True)
        router = CodexProfile(
            name='router', codex_home=root / '.codex-tg' / 'router', sandbox='read-only', full_auto=False
        )
        danger = CodexProfile(
            name='danger', codex_home=root / '.codex-tg' / 'danger', sandbox='danger-full-access', full_auto=False
        )
        return CodexRunner(
            codex_bin='python3',
            repo_root=root,
            model=model,
            timeout_seconds=60,
            chat_profile=chat,
            auto_profile=auto,
            router_profile=router,
            danger_profile=danger,
            log_path=root / 'codex.log',
            resume_cache_path=root / 'resume-cache.json',
        )

    def test_build_base_cmd2_full_auto_vs_sandbox_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runner = self._mk_runner(root=root)
            profile = CodexProfile(name='auto', codex_home=root / '.codex-tg' / 'auto', sandbox=None, full_auto=True)

            cmd = runner._build_base_cmd2(profile, repo_root=root, out_last_message=root / 'last.txt', json_output=True)
            self.assertIn('--full-auto', cmd)
            self.assertIn('--json', cmd)

            cmd2 = runner._build_base_cmd2(
                profile,
                repo_root=root,
                out_last_message=root / 'last.txt',
                sandbox_override='read-only',
                json_output=True,
            )
            self.assertIn('--sandbox', cmd2)
            self.assertIn('read-only', cmd2)
            self.assertNotIn('--full-auto', cmd2)

    def test_build_base_cmd2_dangerous_bypass_adds_flag_and_skips_full_auto(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runner = self._mk_runner(root=root)
            profile = CodexProfile(name='auto', codex_home=root / '.codex-tg' / 'auto', sandbox=None, full_auto=True)

            cmd = runner._build_base_cmd2(
                profile,
                repo_root=root,
                out_last_message=root / 'last.txt',
                dangerously_bypass_permission_and_sandbox=True,
            )
            self.assertIn('--dangerously-bypass-approvals-and-sandbox', cmd)
            self.assertNotIn('--full-auto', cmd)

            cmd2 = runner._build_base_cmd2(
                profile,
                repo_root=root,
                out_last_message=root / 'last.txt',
                sandbox_override='danger-full-access',
                dangerously_bypass_permission_and_sandbox=True,
            )
            self.assertIn('--dangerously-bypass-approvals-and-sandbox', cmd2)
            self.assertIn('--sandbox', cmd2)
            self.assertIn('danger-full-access', cmd2)

    def test_build_base_cmd2_config_overrides_are_sorted_and_quoted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runner = self._mk_runner(root=root, model='gpt-x')
            profile = CodexProfile(
                name='chat', codex_home=root / '.codex-tg' / 'chat', sandbox='read-only', full_auto=False
            )

            overrides = {
                'b': 1,
                'a': 'x',
                'e': 'a"b',
                'c': True,
                'z': None,
            }
            cmd = runner._build_base_cmd2(
                profile,
                repo_root=root,
                out_last_message=root / 'last.txt',
                config_overrides=overrides,
                json_output=True,
            )

            self.assertIn('--model', cmd)
            self.assertIn('gpt-x', cmd)

            pairs: list[str] = []
            for i, tok in enumerate(cmd):
                if tok == '-c' and (i + 1) < len(cmd):
                    pairs.append(str(cmd[i + 1]))

            expected_e = f'e={json.dumps(str(overrides["e"]))}'
            self.assertEqual(
                pairs,
                [
                    f'a={json.dumps("x")}',
                    'b=1',
                    'c=true',
                    expected_e,
                ],
            )

    def test_normalize_session_key_prefers_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runner = self._mk_runner(root=root)

            self.assertEqual(runner._normalize_session_key(chat_id=123, session_key=' abc '), 'abc')
            self.assertEqual(runner._normalize_session_key(chat_id=123, session_key=''), '123')
            self.assertIsNone(runner._normalize_session_key(chat_id=None, session_key=''))
