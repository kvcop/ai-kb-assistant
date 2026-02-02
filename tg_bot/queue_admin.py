from __future__ import annotations

import queue
from typing import Any


def mutate_queue(q: queue.Queue[Any], *, action: str, index: int) -> dict[str, object]:
    """Apply a small mutation to a queue.Queue in-place (best-effort, thread-safe).

    Supports:
    - action=del/delete/rm: delete item at index
    - action=up: swap item with previous (if possible)
    - action=down/dn: swap item with next (if possible)

    Returns a compact dict: {ok, changed, n, error?}.
    """
    a = str(action or '').strip().lower()
    try:
        i = int(index)
    except Exception:
        return {'ok': False, 'error': 'bad_index'}

    with q.mutex:
        try:
            items = list(q.queue)
        except Exception:
            items = []
        n = len(items)
        if i < 0 or i >= n:
            return {'ok': False, 'error': 'out_of_range', 'n': int(n)}

        changed = False
        if a in {'del', 'delete', 'rm'}:
            del items[i]
            changed = True
        elif a == 'up':
            if i > 0:
                items[i - 1], items[i] = items[i], items[i - 1]
                changed = True
        elif a in {'down', 'dn'}:
            if i + 1 < n:
                items[i + 1], items[i] = items[i], items[i + 1]
                changed = True
        else:
            return {'ok': False, 'error': 'bad_action'}

        try:
            q.queue.clear()
            q.queue.extend(items)
            try:
                q.not_empty.notify_all()
            except Exception:
                pass
            try:
                q.not_full.notify_all()
            except Exception:
                pass
        except Exception:
            return {'ok': False, 'error': 'apply_failed'}

    return {'ok': True, 'changed': bool(changed), 'n': len(items)}
