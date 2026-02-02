from __future__ import annotations

import sys
from pathlib import Path


def _main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    from tg_bot.tmux_tools import main  # noqa: PLC0415

    return main()


if __name__ == '__main__':
    raise SystemExit(_main())
