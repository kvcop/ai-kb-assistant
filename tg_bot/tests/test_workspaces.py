import json
import tempfile
import unittest
from pathlib import Path

from tg_bot.workspaces import WorkspaceManager


class TestWorkspaceManager(unittest.TestCase):
    def test_single_tenant_uses_main_repo_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'tg_uploads').mkdir(parents=True, exist_ok=True)
            workspaces_dir = root / 'workspaces'

            wm = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=0,
                workspaces_dir=workspaces_dir,
                owner_uploads_dir=root / 'tg_uploads',
            )

            self.assertFalse(wm.is_multi_tenant())
            self.assertFalse(wm.is_owner_chat(123))
            self.assertEqual(wm.repo_root_for(123), root)
            self.assertEqual(wm.uploads_root_for(123), root / 'tg_uploads')

            paths = wm.ensure_workspace(123)
            self.assertEqual(paths.repo_root, root)
            self.assertEqual(paths.uploads_root, root / 'tg_uploads')
            self.assertFalse((workspaces_dir / 'chat_123').exists())

    def test_non_owner_workspace_is_isolated_and_bootstrapped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'tg_uploads').mkdir(parents=True, exist_ok=True)
            (root / 'templates').mkdir(parents=True, exist_ok=True)
            (root / 'templates' / 't.txt').write_text('hi', encoding='utf-8')

            wm = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=1,
                workspaces_dir=root / 'workspaces',
                owner_uploads_dir=root / 'tg_uploads',
            )

            self.assertTrue(wm.is_multi_tenant())
            self.assertTrue(wm.is_owner_chat(1))
            self.assertFalse(wm.is_owner_chat(2))

            paths = wm.ensure_workspace(2)
            self.assertEqual(paths.repo_root, root / 'workspaces' / 'chat_2')
            self.assertEqual(paths.uploads_root, root / 'workspaces' / 'chat_2' / 'tg_uploads')

            marker = paths.repo_root / '.tg_workspace.json'
            self.assertTrue(marker.exists())
            payload = json.loads(marker.read_text(encoding='utf-8'))
            self.assertEqual(int(payload.get('version') or 0), 1)
            self.assertEqual(int(payload.get('chat_id') or 0), 2)

            # Minimal KB skeleton
            self.assertTrue((paths.repo_root / 'notes' / 'work').exists())
            self.assertTrue((paths.repo_root / 'notes' / 'meetings' / 'artifacts').exists())
            self.assertTrue((paths.repo_root / 'notes' / 'technical').exists())
            self.assertTrue((paths.repo_root / 'notes' / 'daily-logs').exists())
            self.assertTrue((paths.repo_root / 'tmp').exists())
            self.assertTrue(paths.uploads_root.exists())

            # Templates are copied best-effort.
            self.assertTrue((paths.repo_root / 'templates' / 't.txt').exists())

    def test_ensure_workspace_is_idempotent_after_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / 'tg_uploads').mkdir(parents=True, exist_ok=True)
            (root / 'templates').mkdir(parents=True, exist_ok=True)
            (root / 'templates' / 't1.txt').write_text('one', encoding='utf-8')

            wm = WorkspaceManager(
                main_repo_root=root,
                owner_chat_id=1,
                workspaces_dir=root / 'workspaces',
                owner_uploads_dir=root / 'tg_uploads',
            )

            paths = wm.ensure_workspace(2)
            self.assertTrue((paths.repo_root / 'templates' / 't1.txt').exists())

            # After the marker exists, the second call should not re-bootstrap.
            (root / 'templates' / 't2.txt').write_text('two', encoding='utf-8')
            wm.ensure_workspace(2)
            self.assertFalse((paths.repo_root / 'templates' / 't2.txt').exists())
