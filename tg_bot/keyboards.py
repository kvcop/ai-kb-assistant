from __future__ import annotations

from typing import Any

# Callback data codes (must be <= 64 bytes)
CB_ACK = 'ack'
CB_BACK = 'back'
CB_LUNCH_60 = 'lunch60'

CB_MUTE_30M = 'mute30m'
CB_MUTE_1H = 'mute1h'
CB_MUTE_2H = 'mute2h'
CB_MUTE_1D = 'mute1d'

CB_STATUS = 'status'
CB_SUMMARY = 'summary'
CB_TEMPLATE_STATUS = 'tmpl_status'
CB_EOD = 'eod'
CB_RESET = 'reset'

# Simple one-off UI helpers
CB_DISMISS = 'dismiss'

# Gentle mode ("щадящий режим")
CB_GENTLE_TOGGLE = 'gentle'

# Codex answer follow-ups
CB_CX_SHORTER = 'cx_short'
CB_CX_PLAN3 = 'cx_plan3'
CB_CX_STATUS1 = 'cx_status1'
CB_CX_NEXT = 'cx_next'

# Settings (owner chat only)
CB_SETTINGS = 'settings'
CB_SETTINGS_DELIVERY_EDIT = 'settings_delivery_edit'
CB_SETTINGS_DELIVERY_NEW = 'settings_delivery_new'
CB_SETTINGS_DONE_TOGGLE = 'settings_done_toggle'
CB_SETTINGS_DONE_TTL_CYCLE = 'settings_done_ttl'
CB_SETTINGS_BOT_INITIATIVES_TOGGLE = 'settings_bot_inits'
CB_SETTINGS_LIVE_CHATTER_TOGGLE = 'settings_chatter'
CB_SETTINGS_MCP_LIVE_TOGGLE = 'settings_mcp'
CB_SETTINGS_USER_IN_LOOP_TOGGLE = 'settings_uil'

# Queue UI (owner chat only)
CB_QUEUE_PAGE_PREFIX = 'queue:'
CB_QUEUE_EDIT_PREFIX = 'queue_edit:'
CB_QUEUE_DONE_PREFIX = 'queue_done:'
CB_QUEUE_CLEAR_PREFIX = 'queue_clear:'
CB_QUEUE_ITEM_PREFIX = 'queue_item:'
CB_QUEUE_ACT_PREFIX = 'queue_act:'

# Admin menu (owner chat only)
CB_ADMIN = 'admin'
CB_ADMIN_DOCTOR = 'admin_doctor'
CB_ADMIN_STATS = 'admin_stats'
CB_ADMIN_DROP_QUEUE = 'admin_drop_queue'
CB_ADMIN_DROP_ALL = 'admin_drop_all'

# Dangerous override confirmations (dynamic callback_data: prefix + request_id)
CB_DANGER_ALLOW_PREFIX = 'danger_yes:'
CB_DANGER_DENY_PREFIX = 'danger_no:'

# Voice routing choice for auto-transcribed messages (dynamic callback_data: prefix + voice_message_id + ":" + mode)
# Mode codes:
# - r: read (?)
# - w: write (!)
# - d: dangerous (∆)
# - n: no prefix
CB_VOICE_ROUTE_PREFIX = 'vr:'

# user-in-the-loop: answer buttons for `ask_user` blocking questions.
# callback_data: prefix + ("def" | <1-based option index>)
CB_ASK_USER_PREFIX = 'asku:'

# Control-plane callbacks are handled immediately (not spooled to disk),
# so they should be safe/fast and not trigger Codex.
CONTROL_PLANE_CALLBACK_DATA: frozenset[str] = frozenset(
    {
        CB_DISMISS,
        CB_ACK,
        CB_BACK,
        CB_LUNCH_60,
        CB_MUTE_30M,
        CB_MUTE_1H,
        CB_MUTE_2H,
        CB_MUTE_1D,
        CB_STATUS,
        CB_TEMPLATE_STATUS,
        CB_GENTLE_TOGGLE,
        CB_SETTINGS,
        CB_SETTINGS_DELIVERY_EDIT,
        CB_SETTINGS_DELIVERY_NEW,
        CB_SETTINGS_DONE_TOGGLE,
        CB_SETTINGS_DONE_TTL_CYCLE,
        CB_SETTINGS_BOT_INITIATIVES_TOGGLE,
        CB_SETTINGS_LIVE_CHATTER_TOGGLE,
        CB_SETTINGS_MCP_LIVE_TOGGLE,
        CB_SETTINGS_USER_IN_LOOP_TOGGLE,
        CB_ADMIN,
        CB_ADMIN_DOCTOR,
        CB_ADMIN_STATS,
        CB_ADMIN_DROP_QUEUE,
        CB_ADMIN_DROP_ALL,
    }
)


def inline_keyboard(rows: list[list[tuple[str, str]]]) -> dict[str, Any]:
    """Build Telegram InlineKeyboardMarkup.

    rows: list of rows; each row is list of (text, callback_data)
    """
    kb: list[list[dict[str, Any]]] = []
    for row in rows:
        kb_row: list[dict[str, Any]] = []
        for text, data in row:
            kb_row.append({'text': text, 'callback_data': data})
        if kb_row:
            kb.append(kb_row)
    return {'inline_keyboard': kb}


def _gentle_button(gentle_active: bool) -> tuple[str, str]:
    return ('▶️ Обычный режим', CB_GENTLE_TOGGLE) if gentle_active else ('🫶 Щадящий режим', CB_GENTLE_TOGGLE)


def idle_stage(stage: int, *, gentle_active: bool = False) -> dict[str, Any]:
    """Buttons used for staged inactivity pings."""
    gentle_btn = _gentle_button(gentle_active)

    if stage <= 1:
        return inline_keyboard(
            [
                [('✅ Я здесь', CB_ACK), ('🍽️ Обед 60м', CB_LUNCH_60)],
                [('🔕 30м', CB_MUTE_30M), ('🔕 1ч', CB_MUTE_1H), ('🔕 2ч', CB_MUTE_2H)],
                [('📌 Статус', CB_STATUS), gentle_btn],
            ]
        )

    if stage == 2:
        return inline_keyboard(
            [
                [('✍️ Статус-шаблон', CB_TEMPLATE_STATUS), ('📌 Статус', CB_STATUS)],
                [('✅ Я здесь', CB_ACK), ('🍽️ Обед 60м', CB_LUNCH_60), gentle_btn],
                [('🔕 30м', CB_MUTE_30M), ('🔕 1ч', CB_MUTE_1H), ('🔕 2ч', CB_MUTE_2H)],
            ]
        )

    if stage == 3:
        return inline_keyboard(
            [
                [('🧠 Сводка', CB_SUMMARY), ('✍️ Статус-шаблон', CB_TEMPLATE_STATUS)],
                [('✅ Я здесь', CB_ACK), gentle_btn],
                [('🍽️ Обед 60м', CB_LUNCH_60), ('🔕 2ч', CB_MUTE_2H), ('🔕 1д', CB_MUTE_1D)],
            ]
        )

    if stage == 4:
        return inline_keyboard(
            [
                [('🔚 Закончить день', CB_EOD), ('🧠 Сводка', CB_SUMMARY)],
                [('✅ Я здесь', CB_ACK), gentle_btn],
                [('🔕 2ч', CB_MUTE_2H), ('🔕 1д', CB_MUTE_1D)],
            ]
        )

    # stage 5+
    return inline_keyboard(
        [
            [('🔚 Закончить день', CB_EOD), ('🧠 Сводка', CB_SUMMARY)],
            [('✅ Я здесь', CB_ACK), gentle_btn],
            [('🔕 1д', CB_MUTE_1D)],
        ]
    )


def lunch_expired(*, gentle_active: bool = False) -> dict[str, Any]:
    gentle_btn = _gentle_button(gentle_active)
    return inline_keyboard(
        [
            [('✅ Вернулся', CB_BACK), ('🍽️ ещё 60м', CB_LUNCH_60)],
            [('🔕 30м', CB_MUTE_30M), ('🔕 1ч', CB_MUTE_1H), gentle_btn],
        ]
    )


def help_menu(*, gentle_active: bool = False) -> dict[str, Any]:
    gentle_btn = _gentle_button(gentle_active)
    return inline_keyboard(
        [
            [('📌 Статус', CB_STATUS), ('🍽️ Обед', CB_LUNCH_60), gentle_btn],
            [('🔕 30м', CB_MUTE_30M), ('🔕 2ч', CB_MUTE_2H), ('🔕 1д', CB_MUTE_1D)],
            [('🧠 Сводка', CB_SUMMARY), ('✍️ Статус-шаблон', CB_TEMPLATE_STATUS)],
            [('🔚 Закончить день', CB_EOD), ('♻️ Reset', CB_RESET)],
        ]
    )


def codex_answer_menu(*, gentle_active: bool = False) -> dict[str, Any]:
    """Inline buttons shown under Codex answers."""
    gentle_btn = _gentle_button(gentle_active)
    return inline_keyboard(
        [
            [('✂️ Короче', CB_CX_SHORTER), ('🧾 План 3 шага', CB_CX_PLAN3), ('🧩 След. шаг', CB_CX_NEXT)],
            [('🗣 Статус 1 строка', CB_CX_STATUS1), ('🔚 Закончить день', CB_EOD), gentle_btn],
        ]
    )


def codex_answer_menu_public() -> dict[str, Any]:
    """Inline buttons for non-owner chats (no global state actions)."""
    return inline_keyboard(
        [
            [('✂️ Короче', CB_CX_SHORTER), ('🧾 План 3 шага', CB_CX_PLAN3), ('🧩 След. шаг', CB_CX_NEXT)],
            [('🗣 Статус 1 строка', CB_CX_STATUS1)],
        ]
    )


def dangerous_confirm_menu(request_id: str) -> dict[str, Any]:
    """Inline buttons shown when a request needs an explicit dangerous override."""
    rid = (request_id or '').strip()[:32]
    return inline_keyboard(
        [
            [('⚠️ Да (dangerous)', f'{CB_DANGER_ALLOW_PREFIX}{rid}'), ('❌ Нет', f'{CB_DANGER_DENY_PREFIX}{rid}')],
        ]
    )


def voice_route_menu(*, voice_message_id: int, selected: str | None = None) -> dict[str, Any]:
    """Inline buttons shown under "voice accepted" message.

    Lets the user choose a forced routing prefix for the transcribed request.
    """
    try:
        mid = int(voice_message_id or 0)
    except Exception:
        mid = 0
    mid_s = str(mid if mid > 0 else 0)
    sel = str(selected or '').strip().lower()

    def _btn(label: str, *, mode: str, is_selected: bool) -> tuple[str, str]:
        txt = f'✅ {label}' if is_selected else label
        return (txt, f'{CB_VOICE_ROUTE_PREFIX}{mid_s}:{mode}')

    return inline_keyboard(
        [
            [
                _btn('? read', mode='r', is_selected=(sel == 'read')),
                _btn('! write', mode='w', is_selected=(sel == 'write')),
            ],
            [
                _btn('∆ danger', mode='d', is_selected=(sel == 'danger')),
                _btn('∅ none', mode='n', is_selected=(sel == 'none')),
            ],
        ]
    )


def ask_user_menu(*, options: list[str], default: str = '') -> dict[str, Any] | None:
    opts = [str(x or '').strip() for x in (options or []) if isinstance(x, str) and str(x or '').strip()]
    opts = opts[:5]
    default_s = (default or '').strip()

    rows: list[list[tuple[str, str]]] = []
    row: list[tuple[str, str]] = []
    for i, opt in enumerate(opts, start=1):
        label = opt
        if len(label) > 24:
            label = label[:23] + '…'
        row.append((label, f'{CB_ASK_USER_PREFIX}{i}'))
        if len(row) >= 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if default_s:
        rows.append([('Дефолт', f'{CB_ASK_USER_PREFIX}def')])
    return inline_keyboard(rows) if rows else None


def describe_callback_data(data: str) -> str:
    """Human-friendly label for callback_data (best-effort).

    Used for queue previews and activity history so queued callbacks are understandable.
    """
    d = str(data or '').strip()
    if not d:
        return ''

    if d.startswith(CB_DANGER_ALLOW_PREFIX):
        return '⚠️ Dangerous override: YES'
    if d.startswith(CB_DANGER_DENY_PREFIX):
        return '❌ Dangerous override: NO'
    if d.startswith(CB_VOICE_ROUTE_PREFIX):
        rest = d[len(CB_VOICE_ROUTE_PREFIX) :].strip()
        parts = rest.split(':')
        mode = str(parts[1] or '').strip().lower() if len(parts) == 2 else ''
        suffix = {'r': 'read (?)', 'w': 'write (!)', 'd': 'danger (∆)', 'n': 'none'}.get(mode, '')
        return f'🎙️ Voice route: {suffix}'.strip(': ').strip()

    if d.startswith(CB_ASK_USER_PREFIX):
        rest = d[len(CB_ASK_USER_PREFIX) :].strip()
        if rest == 'def':
            return '❓ Answer: default'
        if rest.isdigit():
            return f'❓ Answer: option {rest}'
        return '❓ Answer'

    if d.startswith(CB_QUEUE_ACT_PREFIX):
        rest = d[len(CB_QUEUE_ACT_PREFIX) :].strip()
        parts = rest.split(':')
        if len(parts) >= 3:
            bucket = str(parts[0] or '').strip().lower()
            act = str(parts[2] or '').strip().lower()
            act_lbl = {'up': '⬆️', 'down': '⬇️', 'del': '🗑 Delete'}.get(act, act)
            return f'🧾 Queue: {act_lbl} ({bucket})'
        return '🧾 Queue: action'
    if d.startswith(CB_QUEUE_ITEM_PREFIX):
        return '🧾 Queue: item'
    if d.startswith(CB_QUEUE_CLEAR_PREFIX):
        return '🧾 Queue: clear'
    if d.startswith(CB_QUEUE_EDIT_PREFIX):
        return '🧾 Queue: edit mode ON'
    if d.startswith(CB_QUEUE_DONE_PREFIX):
        return '🧾 Queue: edit mode OFF'
    if d.startswith(CB_QUEUE_PAGE_PREFIX):
        return '🧾 Queue: page'

    label_map: dict[str, str] = {
        CB_ACK: '✅ Я здесь',
        CB_BACK: '✅ Вернулся',
        CB_LUNCH_60: '🍽️ Обед 60м',
        CB_MUTE_30M: '🔕 30м',
        CB_MUTE_1H: '🔕 1ч',
        CB_MUTE_2H: '🔕 2ч',
        CB_MUTE_1D: '🔕 1д',
        CB_STATUS: '📌 Статус',
        CB_SUMMARY: '🧠 Сводка',
        CB_TEMPLATE_STATUS: '✍️ Статус-шаблон',
        CB_EOD: '🔚 Закончить день',
        CB_RESET: '♻️ Reset',
        CB_DISMISS: '🗑 Убрать',
        CB_GENTLE_TOGGLE: '🫶 Щадящий режим (toggle)',
        CB_CX_SHORTER: '✂️ Короче',
        CB_CX_PLAN3: '🧾 План 3 шага',
        CB_CX_NEXT: '🧩 Следующий шаг',
        CB_CX_STATUS1: '🗣 Статус 1 строка',
        CB_SETTINGS: '⚙️ Settings',
        CB_SETTINGS_DELIVERY_EDIT: '⚙️ Settings: delivery=edit',
        CB_SETTINGS_DELIVERY_NEW: '⚙️ Settings: delivery=new',
        CB_SETTINGS_DONE_TOGGLE: '⚙️ Settings: done toggle',
        CB_SETTINGS_DONE_TTL_CYCLE: '⚙️ Settings: done ttl',
        CB_SETTINGS_BOT_INITIATIVES_TOGGLE: '⚙️ Settings: bot initiatives toggle',
        CB_ADMIN: '🛠 Admin',
        CB_ADMIN_DOCTOR: '🩺 Doctor',
        CB_ADMIN_STATS: '📊 Stats',
        CB_ADMIN_DROP_QUEUE: '🧹 Drop queue',
        CB_ADMIN_DROP_ALL: '🧹 Drop all',
    }
    return label_map.get(d, d)


def dismiss_menu(*, label: str = '🗑 Убрать') -> dict[str, Any]:
    """Single-button menu to remove a bot message."""
    lbl = (label or '').strip() or '🗑 Убрать'
    if len(lbl) > 60:
        lbl = lbl[:60]
    return inline_keyboard([[((lbl), CB_DISMISS)]])


def settings_menu(
    *,
    prefer_edit_delivery: bool,
    done_notice_enabled: bool,
    done_notice_delete_seconds: int,
    bot_initiatives_enabled: bool,
    live_chatter_enabled: bool,
    mcp_live_enabled: bool,
    user_in_loop_enabled: bool,
) -> dict[str, Any]:
    def _ttl_label(seconds: int) -> str:
        s = max(0, int(seconds))
        if s <= 0:
            return 'не удалять'
        if s % 3600 == 0:
            return f'{s // 3600}ч'
        if s % 60 == 0:
            return f'{s // 60}м'
        return f'{s}с'

    edit_lbl = '✏️ Edit ✅' if prefer_edit_delivery else '✏️ Edit'
    new_lbl = '📨 New ✅' if not prefer_edit_delivery else '📨 New'
    done_lbl = '✅ Done: ON' if done_notice_enabled else '✅ Done: OFF'
    ttl_lbl = f'🗑 Done: {_ttl_label(done_notice_delete_seconds)}'
    bot_lbl = '🔔 Bot: ON' if bot_initiatives_enabled else '🔕 Bot: OFF'
    chatter_lbl = '💬 Chatter: ON' if live_chatter_enabled else '💬 Chatter: OFF'
    mcp_lbl = '📡 Followups: ON' if mcp_live_enabled else '📡 Followups: OFF'
    uil_lbl = '❓ Ask: ON' if user_in_loop_enabled else '❓ Ask: OFF'

    return inline_keyboard(
        [
            [(edit_lbl, CB_SETTINGS_DELIVERY_EDIT), (new_lbl, CB_SETTINGS_DELIVERY_NEW)],
            [(done_lbl, CB_SETTINGS_DONE_TOGGLE), (ttl_lbl, CB_SETTINGS_DONE_TTL_CYCLE)],
            [(bot_lbl, CB_SETTINGS_BOT_INITIATIVES_TOGGLE), (chatter_lbl, CB_SETTINGS_LIVE_CHATTER_TOGGLE)],
            [(mcp_lbl, CB_SETTINGS_MCP_LIVE_TOGGLE), (uil_lbl, CB_SETTINGS_USER_IN_LOOP_TOGGLE)],
            [('🗑 Убрать', CB_DISMISS)],
        ]
    )


def queue_menu(
    *,
    page: int,
    pages: int,
    edit_active: bool = False,
    item_buttons: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    p = max(0, int(page))
    n = max(1, int(pages))
    if p >= n:
        p = n - 1

    prev_p = p - 1 if p > 0 else 0
    next_p = p + 1 if p + 1 < n else (n - 1)

    toggle_label = '✅ Done' if edit_active else '✏️ Edit'
    toggle_cb = f'{CB_QUEUE_DONE_PREFIX}{p}' if edit_active else f'{CB_QUEUE_EDIT_PREFIX}{p}'

    rows: list[list[tuple[str, str]]] = [
        [
            ('◀︎ Prev', f'{CB_QUEUE_PAGE_PREFIX}{prev_p}'),
            (f'{p + 1}/{n}', f'{CB_QUEUE_PAGE_PREFIX}{p}'),
            ('Next ▶︎', f'{CB_QUEUE_PAGE_PREFIX}{next_p}'),
        ],
        [
            (toggle_label, toggle_cb),
            ('🧹 Clear', f'{CB_QUEUE_CLEAR_PREFIX}{p}'),
            ('🔄 Refresh', f'{CB_QUEUE_PAGE_PREFIX}{p}'),
        ],
    ]
    if edit_active and item_buttons:
        rows.append(item_buttons)
    rows.append([('🗑 Убрать', CB_DISMISS)])
    return inline_keyboard(rows)


def queue_item_menu(*, bucket: str, index: int, page: int, edit_active: bool) -> dict[str, Any]:
    b = str(bucket or '').strip().lower()
    i = max(0, int(index))
    p = max(0, int(page))

    toggle_label = '✅ Done' if edit_active else '✏️ Edit'
    toggle_cb = f'{CB_QUEUE_DONE_PREFIX}{p}' if edit_active else f'{CB_QUEUE_EDIT_PREFIX}{p}'

    rows: list[list[tuple[str, str]]] = [
        [('⬅️ Back', f'{CB_QUEUE_PAGE_PREFIX}{p}'), (toggle_label, toggle_cb)],
    ]
    if edit_active and b == 'main':
        rows.append(
            [
                ('⬆️', f'{CB_QUEUE_ACT_PREFIX}{b}:{i}:up:{p}'),
                ('🗑 Delete', f'{CB_QUEUE_ACT_PREFIX}{b}:{i}:del:{p}'),
                ('⬇️', f'{CB_QUEUE_ACT_PREFIX}{b}:{i}:down:{p}'),
            ]
        )
    elif edit_active and b == 'spool':
        rows.append(
            [
                ('🗑 Delete', f'{CB_QUEUE_ACT_PREFIX}{b}:{i}:del:{p}'),
            ]
        )
    rows.append([('🧹 Clear', f'{CB_QUEUE_CLEAR_PREFIX}{p}'), ('🗑 Убрать', CB_DISMISS)])
    return inline_keyboard(rows)


def admin_menu(*, queue_page: int = 0) -> dict[str, Any]:
    p = max(0, int(queue_page))
    return inline_keyboard(
        [
            [('🧾 Queue', f'{CB_QUEUE_PAGE_PREFIX}{p}'), ('⚙️ Settings', CB_SETTINGS)],
            [('🩺 Doctor', CB_ADMIN_DOCTOR), ('📊 Stats', CB_ADMIN_STATS)],
            [('🧹 Drop queue', CB_ADMIN_DROP_QUEUE), ('🧹 Drop all', CB_ADMIN_DROP_ALL)],
            [('🗑 Убрать', CB_DISMISS)],
        ]
    )
