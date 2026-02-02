from __future__ import annotations

import dataclasses
import json
import shutil
import time
from pathlib import Path


@dataclasses.dataclass(frozen=True)
class WorkspacePaths:
    repo_root: Path
    uploads_root: Path


class WorkspaceManager:
    """Multi-chat workspace isolation.

    - Owner chat uses the main repo root (full KB).
    - Any other allowed chat gets an isolated "mini-KB" workspace under `workspaces_dir`.
    """

    def __init__(
        self,
        *,
        main_repo_root: Path,
        owner_chat_id: int,
        workspaces_dir: Path,
        owner_uploads_dir: Path,
    ) -> None:
        self.main_repo_root = main_repo_root
        self.owner_chat_id = int(owner_chat_id or 0)
        self.workspaces_dir = workspaces_dir
        self.owner_uploads_dir = owner_uploads_dir

    def is_multi_tenant(self) -> bool:
        return int(self.owner_chat_id) != 0

    def is_owner_chat(self, chat_id: int) -> bool:
        return self.is_multi_tenant() and int(chat_id) == int(self.owner_chat_id)

    def repo_root_for(self, chat_id: int) -> Path:
        if not self.is_multi_tenant() or self.is_owner_chat(chat_id):
            return self.main_repo_root
        return self.workspaces_dir / f'chat_{int(chat_id)}'

    def uploads_root_for(self, chat_id: int) -> Path:
        repo_root = self.repo_root_for(chat_id)
        if repo_root == self.main_repo_root:
            return self.owner_uploads_dir
        return repo_root / 'tg_uploads'

    def paths_for(self, chat_id: int) -> WorkspacePaths:
        repo_root = self.repo_root_for(chat_id)
        uploads_root = self.uploads_root_for(chat_id)
        return WorkspacePaths(repo_root=repo_root, uploads_root=uploads_root)

    def ensure_workspace(self, chat_id: int) -> WorkspacePaths:
        """Create an isolated workspace for chat_id if needed (best-effort)."""
        paths = self.paths_for(chat_id)
        if paths.repo_root == self.main_repo_root:
            return paths

        root = paths.repo_root
        marker = root / '.tg_workspace.json'
        if marker.exists():
            return paths

        root.mkdir(parents=True, exist_ok=True)

        # Minimal KB skeleton (no personal notes, no Jira config by default).
        (root / 'notes' / 'work').mkdir(parents=True, exist_ok=True)
        (root / 'notes' / 'meetings' / 'artifacts').mkdir(parents=True, exist_ok=True)
        (root / 'notes' / 'technical').mkdir(parents=True, exist_ok=True)
        (root / 'notes' / 'daily-logs').mkdir(parents=True, exist_ok=True)
        (root / 'tmp').mkdir(parents=True, exist_ok=True)
        paths.uploads_root.mkdir(parents=True, exist_ok=True)

        # Copy templates as a starting point (safe, generic).
        templates_src = self.main_repo_root / 'templates'
        templates_dst = root / 'templates'
        try:
            if templates_src.exists():
                shutil.copytree(templates_src, templates_dst, dirs_exist_ok=True)
        except Exception:
            pass

        readme = root / 'README.md'
        if not readme.exists():
            try:
                readme.write_text(
                    (
                        '# KB workspace (Telegram chat)\n\n'
                        'This folder is an isolated knowledge base used by the Telegram bot for a non-owner chat.\n'
                        "It is created automatically to avoid leaking the owner's personal KB into shared chats.\n"
                    ),
                    encoding='utf-8',
                )
            except Exception:
                pass

        try:
            marker.write_text(
                json.dumps(
                    {
                        'version': 1,
                        'chat_id': int(chat_id),
                        'created_ts': float(time.time()),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + '\n',
                encoding='utf-8',
            )
        except Exception:
            pass

        return paths
