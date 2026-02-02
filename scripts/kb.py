#!/usr/bin/env python3
"""
Knowledge Base maintenance helper.

Host requirements:
- Python 3.10+ (3.11+ recommended for built-in TOML parser)
- Optional: Jira access via `JIRA_URL` + `JIRA_TOKEN` (+ `JIRA_USERNAME` for Basic auth)
- Optional: taxonomy + classification cache (configure in `configs/kb.toml`):
  - taxonomy YAML: `taxonomy.path`
  - sqlite cache: `taxonomy.classification_db`

Typical usage from repository root:
  python scripts/kb.py doctor
  python scripts/kb.py day-start
  python scripts/kb.py end-day
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import difflib
import json
import os
import re
import sqlite3
import sys
import textwrap
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / 'configs' / 'kb.toml'
SNAPSHOT_DIR = REPO_ROOT / 'logs' / 'jira-snapshots'
BRIEF_PATH = REPO_ROOT / 'notes' / 'work' / 'daily-brief.md'
EOD_PATH = REPO_ROOT / 'notes' / 'work' / 'end-of-day.md'
JIRA_DOC_PATH = REPO_ROOT / 'notes' / 'work' / 'jira.md'
DAILY_LOGS_DIR = REPO_ROOT / 'notes' / 'daily-logs'
DOTENV_PATH = REPO_ROOT / '.env'
OPEN_QUESTIONS_PATH = REPO_ROOT / 'notes' / 'work' / 'open-questions.md'
TIME_BUCKETS_PATH = REPO_ROOT / 'notes' / 'work' / 'time-buckets.md'
TYPOS_PATH = REPO_ROOT / 'notes' / 'work' / 'typos.md'
REMINDERS_PATH = REPO_ROOT / 'notes' / 'work' / 'reminders.md'
PROJECT_TODOS_PATH = REPO_ROOT / 'notes' / 'work' / 'todos.md'


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).astimezone()


def _default_work_date(now: dt.datetime) -> dt.date:
    return now.date() - dt.timedelta(days=1) if now.hour < 6 else now.date()


def _read_text(path: Path) -> str:
    return path.read_text(encoding='utf-8')


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')


def _load_typos_db(path: Path = TYPOS_PATH) -> dict[str, str]:
    try:
        if not path.exists():
            return {}
        content = _read_text(path)
    except OSError:
        return {}

    out: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.rstrip('\n')
        if not line.strip():
            continue
        if line.lstrip().startswith('#'):
            continue
        if '\t' not in line:
            continue
        typo, fix = line.split('\t', 1)
        typo = typo.strip()
        fix = fix.strip()
        if not typo or not fix:
            continue
        out[typo] = fix
    return out


def _render_typos_db(*, entries: Mapping[str, str], updated_at: dt.datetime) -> str:
    lines: list[str] = []
    lines.append('# Опечатки и исправления (транскрипции/термины)')
    lines.append('')
    lines.append(f'**Updated**: {updated_at.date().isoformat()}  ')
    lines.append(
        '**Format**: `typo<TAB>fix` (1 запись на строку; файл поддерживается в отсортированном виде по `typo`).'
    )
    lines.append('')
    for typo, fix in sorted(entries.items(), key=lambda kv: kv[0].casefold()):
        lines.append(f'{typo}\t{fix}')
    lines.append('')
    return _compact_lines('\n'.join(lines))


def _write_typos_db(
    *, entries: Mapping[str, str], path: Path = TYPOS_PATH, updated_at: dt.datetime | None = None
) -> None:
    md = _render_typos_db(entries=entries, updated_at=updated_at or _now())
    _write_text(path, md)


def _split_alnum_words(text: str) -> list[str]:
    words: list[str] = []
    buffer: list[str] = []
    for ch in text.casefold():
        if ch.isalnum():
            buffer.append(ch)
            continue
        if buffer:
            words.append(''.join(buffer))
            buffer = []
    if buffer:
        words.append(''.join(buffer))
    return words


def _is_subsequence(needle: str, haystack: str) -> bool:
    if not needle or not haystack:
        return False
    it = iter(haystack)
    return all(ch in it for ch in needle)


def _typos_fuzzy_matches(query: str, entries: Mapping[str, str]) -> list[tuple[str, str]]:
    tokens = _split_alnum_words(query)
    if not tokens:
        return []
    query_norm = ''.join(tokens)
    if len(query_norm) < 3:
        return []

    hits: list[tuple[str, str, float]] = []
    for typo, fix in entries.items():
        best = 0.0
        for text in (typo, fix):
            for word in _split_alnum_words(text):
                if not word:
                    continue
                if len(tokens) > 1 and not all(_is_subsequence(token, word) for token in tokens):
                    continue
                ratio = difflib.SequenceMatcher(None, query_norm, word).ratio()
                min_ratio = 0.45 if len(tokens) > 1 else 0.5
                if ratio < min_ratio:
                    continue
                if ratio > best:
                    best = ratio
        if best:
            hits.append((typo, fix, best))

    hits.sort(key=lambda item: (-item[2], item[0].casefold()))
    return [(typo, fix) for typo, fix, _ in hits]


def cmd_typos(args: argparse.Namespace) -> int:
    """
    Maintain a lightweight typo->fix glossary (for transcripts/protocols).
    """

    entries = _load_typos_db()

    additions: list[tuple[str, str]] = []
    for pair in getattr(args, 'add', None) or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        typo, fix = str(pair[0]).strip(), str(pair[1]).strip()
        if typo and fix:
            additions.append((typo, fix))

    if additions:
        for typo, fix in additions:
            entries[typo] = fix
        _write_typos_db(entries=entries)

    query = getattr(args, 'query', None)
    if query:
        q = str(query).strip()
        if not q:
            return 0
        q_cf = q.casefold()

        matches = [(t, f) for t, f in entries.items() if q_cf in t.casefold() or q_cf in f.casefold()]
        if matches:
            for typo, fix in sorted(matches, key=lambda kv: kv[0].casefold()):
                print(f'{typo}\t{fix}')
            return 0

        fuzzy_matches = _typos_fuzzy_matches(q, entries)
        if fuzzy_matches:
            for typo, fix in fuzzy_matches:
                print(f'{typo}\t{fix}')
            return 0

        keys = sorted(entries.keys(), key=str.casefold)
        suggestions = difflib.get_close_matches(q, keys, n=5, cutoff=0.6)
        if suggestions:
            print('# No direct matches. Similar entries:')
            for s in suggestions:
                print(f'{s}\t{entries[s]}')
        else:
            print('# No matches.')
        return 0

    for typo, fix in sorted(entries.items(), key=lambda kv: kv[0].casefold()):
        print(f'{typo}\t{fix}')
    return 0


@dataclasses.dataclass(frozen=True)
class ReminderEntry:
    rule: str
    text: str


@dataclasses.dataclass(frozen=True)
class ReminderRule:
    kind: str
    label: str | None
    date: dt.date | None = None
    start: dt.date | None = None
    end: dt.date | None = None
    weekdays: tuple[int, ...] = ()


def _parse_weekdays(raw: str) -> tuple[int, ...] | None:
    raw = (raw or '').strip()
    if not raw:
        return None
    tokens = [t for t in re.split(r'[,\s]+', raw) if t]
    if not tokens:
        return None

    alias: dict[str, int] = {
        '0': 0,
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        'mon': 0,
        'monday': 0,
        'пн': 0,
        'tue': 1,
        'tues': 1,
        'tuesday': 1,
        'вт': 1,
        'wed': 2,
        'wednesday': 2,
        'ср': 2,
        'thu': 3,
        'thur': 3,
        'thurs': 3,
        'thursday': 3,
        'чт': 3,
        'fri': 4,
        'friday': 4,
        'пт': 4,
        'sat': 5,
        'saturday': 5,
        'сб': 5,
        'sun': 6,
        'sunday': 6,
        'вс': 6,
    }

    days: set[int] = set()
    for tok in tokens:
        key = tok.strip().casefold()
        if not key:
            continue
        d = alias.get(key)
        if d is None:
            return None
        if 0 <= d <= 6:
            days.add(int(d))
    if not days:
        return None
    return tuple(sorted(days))


def _load_reminders_db(path: Path = REMINDERS_PATH) -> list[ReminderEntry]:
    try:
        if not path.exists():
            return []
        content = _read_text(path)
    except OSError:
        return []

    entries: list[ReminderEntry] = []
    for raw_line in content.splitlines():
        line = raw_line.rstrip('\n')
        if not line.strip():
            continue
        if line.lstrip().startswith('#'):
            continue
        if '\t' not in line:
            continue
        rule, text = line.split('\t', 1)
        rule = rule.strip()
        text = text.strip()
        if not rule or not text:
            continue
        entries.append(ReminderEntry(rule=rule, text=text))
    return entries


def _render_reminders_db(*, entries: Sequence[ReminderEntry], updated_at: dt.datetime) -> str:
    lines: list[str] = []
    lines.append('# Напоминания (work)')
    lines.append('')
    lines.append(f'**Updated**: {updated_at.date().isoformat()}  ')
    lines.append('**Format**: `rule<TAB>text` (1 запись на строку; строки с `#` и пустые игнорируются).')
    lines.append('**Rule**:')
    lines.append('- `daily` или `daily@label`')
    lines.append('- `weekly:<dow>[,<dow>...]` (опционально `@HH:MM`; например `weekly:tue,fri@11:50`)')
    lines.append('- `date:YYYY-MM-DD` (опционально `@HH:MM`)')
    lines.append('- `range:YYYY-MM-DD..YYYY-MM-DD` (опционально `@HH:MM`)')
    lines.append(
        '- (опционально) суффикс `|to=...` — куда отправлять (для TG-бота): `owner`, `broadcast`, или список `chat_id` через запятую'
    )
    lines.append('')
    for entry in entries:
        lines.append(f'{entry.rule}\t{entry.text}')
    lines.append('')
    return _compact_lines('\n'.join(lines))


def _write_reminders_db(
    *, entries: Sequence[ReminderEntry], path: Path = REMINDERS_PATH, updated_at: dt.datetime | None = None
) -> None:
    md = _render_reminders_db(entries=entries, updated_at=updated_at or _now())
    _write_text(path, md)


def _parse_reminder_rule(rule: str) -> ReminderRule | None:
    raw = rule.strip()
    if not raw:
        return None
    head = raw.split('|', 1)[0].strip()
    base_raw, sep, label_raw = head.partition('@')
    label = label_raw.strip() if sep and label_raw.strip() else None
    base = base_raw.strip().casefold()

    if base == 'daily':
        return ReminderRule(kind='daily', label=label)
    if base.startswith('weekly:'):
        value = base_raw[len('weekly:') :].strip()
        weekdays = _parse_weekdays(value)
        if not weekdays:
            return None
        return ReminderRule(kind='weekly', label=label, weekdays=weekdays)
    if base.startswith('date:'):
        date = _parse_ymd_date(base_raw[len('date:') :].strip())
        if not date:
            return None
        return ReminderRule(kind='date', label=label, date=date)
    if base.startswith('range:'):
        value = base_raw[len('range:') :].strip()
        if '..' not in value:
            return None
        start_raw, end_raw = value.split('..', 1)
        start = _parse_ymd_date(start_raw.strip())
        end = _parse_ymd_date(end_raw.strip())
        if not start or not end or end < start:
            return None
        return ReminderRule(kind='range', label=label, start=start, end=end)
    return None


def _reminder_matches_date(rule: ReminderRule, target_date: dt.date) -> bool:
    if rule.kind == 'daily':
        return True
    if rule.kind == 'weekly':
        return bool(rule.weekdays) and target_date.weekday() in rule.weekdays
    if rule.kind == 'date':
        return rule.date == target_date
    if rule.kind == 'range':
        if rule.start is None or rule.end is None:
            return False
        return rule.start <= target_date <= rule.end
    return False


def _collect_reminders_for_date(entries: Sequence[ReminderEntry], target_date: dt.date) -> list[tuple[str | None, str]]:
    matches: list[tuple[str | None, str]] = []
    for entry in entries:
        parsed = _parse_reminder_rule(entry.rule)
        if not parsed:
            continue
        if _reminder_matches_date(parsed, target_date):
            matches.append((parsed.label, entry.text))
    return matches


def _print_reminders_summary(*, target_date: dt.date, entries: Sequence[ReminderEntry]) -> None:
    matches = _collect_reminders_for_date(entries, target_date)
    print(f'Reminders ({target_date.isoformat()})')
    if not matches:
        print('- none')
        return

    def _try_parse_hhmm(label: str | None) -> int | None:
        if not label:
            return None
        m = re.fullmatch(r'(\d{1,2}):(\d{2})', label.strip())
        if not m:
            return None
        hour, minute = int(m.group(1)), int(m.group(2))
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return None
        return hour * 60 + minute

    ordered: list[tuple[int, int, int, str | None, str]] = []
    for idx, (label, text) in enumerate(matches):
        minutes = _try_parse_hhmm(label)
        group = 0 if minutes is not None else 1
        ordered.append((group, minutes or 0, idx, label, text))
    ordered.sort(key=lambda t: (t[0], t[1], t[2]))

    for _, _, _, label, text in ordered:
        if label:
            print(f'- {label}: {text}')
        else:
            print(f'- {text}')


def cmd_reminders(args: argparse.Namespace) -> int:
    entries = _load_reminders_db()

    additions: list[ReminderEntry] = []
    for pair in getattr(args, 'add', None) or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        rule, text = str(pair[0]).strip(), str(pair[1]).strip()
        if rule and text:
            additions.append(ReminderEntry(rule=rule, text=text))

    if additions:
        entries = [*entries, *additions]
        _write_reminders_db(entries=entries)

    if getattr(args, 'all', False):
        for entry in entries:
            print(f'{entry.rule}\t{entry.text}')
        return 0

    now = _now()
    target_date = _parse_ymd_date(getattr(args, 'date', None)) or _default_work_date(now)
    _print_reminders_summary(target_date=target_date, entries=entries)
    return 0


def _load_dotenv(path: Path = DOTENV_PATH) -> None:
    """
    Best-effort `.env` loader (no dependencies).

    - Only imports `JIRA_*` keys
    - Does not override already-set environment variables
    - Supports `KEY=VALUE` and `export KEY=VALUE`
    """

    try:
        if not path.exists():
            return
        content = _read_text(path)
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
        if not key or not key.startswith('JIRA_'):
            continue
        if os.environ.get(key):
            continue
        value = _parse_dotenv_value(value)
        os.environ[key] = value


def _parse_dotenv_value(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ''

    if value[0] in {"'", '"'}:
        quote = value[0]
        end = value.find(quote, 1)
        if end != -1:
            return value[1:end]
        return value

    comment = re.search(r'\s+#', value)
    if comment:
        value = value[: comment.start()].rstrip()
    return value


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        import tomllib  # pyright: ignore[reportMissingImports]
    except ModuleNotFoundError:  # pragma: no cover
        print(
            'ERROR: Python < 3.11 without tomllib. Install `tomli` or upgrade Python.',
            file=sys.stderr,
        )
        raise

    return tomllib.loads(_read_text(path))


def _resolve_repo_path(path: str | Path) -> Path:
    p = path if isinstance(path, Path) else Path(path)
    return p if p.is_absolute() else (REPO_ROOT / p)


def _compact_lines(text: str) -> str:
    return '\n'.join(line.rstrip() for line in text.splitlines()).strip() + '\n'


def _slug_timestamp(ts: dt.datetime) -> str:
    return ts.strftime('%Y-%m-%d_%H%M%S')


def _parse_ymd_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _priority_rank(priority: str | None) -> int:
    if not priority:
        return 100
    p = str(priority).strip().lower()
    mapping: dict[str, int] = {
        'highest': 0,
        'critical': 0,
        'blocker': 0,
        'high': 1,
        'major': 1,
        'medium': 2,
        'normal': 2,
        'low': 3,
        'minor': 3,
        'lowest': 4,
        'trivial': 4,
        'критический': 0,
        'высокий': 1,
        'нормальный': 2,
        'средний': 2,
        'низкий': 3,
        'самый низкий': 4,
    }
    return mapping.get(p, 50)


def _status_rank(status: str | None) -> int:
    if not status:
        return 50
    s = str(status).strip().lower()

    done = {'done', 'готово', 'сделано', 'закрыто', 'closed', 'resolved'}
    if s in done:
        return 99

    if 'progress' in s or 'в работе' in s:
        return 0
    if 'review' in s or 'ревью' in s:
        return 1

    todo = {'to do', 'open', 'backlog', 'сделать'}
    if s in todo:
        return 2

    return 10


def _is_done_status(status: str | None) -> bool:
    return _status_rank(status) >= 90


def _extract_h2_section_bodies(markdown: str) -> dict[str, list[str]]:
    """
    Extract bodies of `## <Title>` sections (without the heading line).
    Intended for preserving manual sections when regenerating files.
    """

    sections: dict[str, list[str]] = {}
    current_title: str | None = None
    buf: list[str] = []

    for line in markdown.splitlines():
        if line.startswith('## '):
            if current_title is not None:
                sections[current_title] = buf
            current_title = line[3:].strip()
            buf = []
            continue
        if current_title is not None:
            buf.append(line)

    if current_title is not None:
        sections[current_title] = buf

    return sections


def _extract_eod_date(markdown: str) -> str | None:
    for line in markdown.splitlines():
        if line.startswith('# End of Day — '):
            value = line.removeprefix('# End of Day — ').strip()
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
                return value
            return value[:10] if re.fullmatch(r'\d{4}-\d{2}-\d{2}.*', value) else None
    return None


def _extract_daily_brief_date(markdown: str) -> str | None:
    for line in markdown.splitlines():
        if line.startswith('# Daily Brief — '):
            value = line.removeprefix('# Daily Brief — ').strip()
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
                return value
            return value[:10] if re.fullmatch(r'\d{4}-\d{2}-\d{2}.*', value) else None
    return None


@dataclasses.dataclass(frozen=True)
class JiraIssue:
    key: str
    summary: str
    status: str | None
    priority: str | None
    assignee: str | None
    created: str | None
    updated: str | None
    duedate: str | None
    issuetype: str | None
    labels: list[str]


@dataclasses.dataclass(frozen=True)
class IssueClassification:
    issue_key: str
    issue_updated: str
    theme_id: str
    confidence: float
    reason: str
    locked: bool


@dataclasses.dataclass(frozen=True)
class ThemeNode:
    node_id: str
    title: str
    children: tuple[ThemeNode, ...]


class ThemeResolver:
    def __init__(self, roots: list[ThemeNode]) -> None:
        self._path_to_titles: dict[str, list[str]] = {}
        self._variant_to_path: dict[str, str] = {}
        self._leaf_to_paths: dict[str, set[str]] = {}

        for root in roots:
            self._index_node(root, parent_path=None, parent_titles=[])

        self._build_variants()

    def _index_node(self, node: ThemeNode, parent_path: str | None, parent_titles: list[str]) -> None:
        path = node.node_id if parent_path is None else f'{parent_path}.{node.node_id}'
        titles = [*parent_titles, node.title]
        self._path_to_titles[path] = titles
        self._leaf_to_paths.setdefault(node.node_id, set()).add(path)
        for child in node.children:
            self._index_node(child, parent_path=path, parent_titles=titles)

    def _build_variants(self) -> None:
        joiners = ['.', '/', '-', '_', ' > ']

        variant_to_paths: dict[str, set[str]] = {}
        for path in self._path_to_titles.keys():
            segments = path.split('.')
            for start in range(len(segments)):
                suffix = segments[start:]
                for joiner in joiners:
                    variant = joiner.join(suffix)
                    variant_to_paths.setdefault(variant, set()).add(path)

        # Keep only unambiguous variants.
        self._variant_to_path = {
            variant: next(iter(paths)) for variant, paths in variant_to_paths.items() if len(paths) == 1
        }

    def resolve_path(self, raw_theme_id: str | None) -> str | None:
        if not raw_theme_id:
            return None

        raw = raw_theme_id.strip()

        if raw in self._variant_to_path:
            return self._variant_to_path[raw]

        if raw in self._leaf_to_paths and len(self._leaf_to_paths[raw]) == 1:
            return next(iter(self._leaf_to_paths[raw]))

        return None

    def title_path(self, canonical_path: str | None) -> str | None:
        if not canonical_path:
            return None
        titles = self._path_to_titles.get(canonical_path)
        if not titles:
            return None
        return ' → '.join(titles)


def _parse_simple_taxonomy_yaml(path: Path) -> list[ThemeNode]:
    """
    Minimal YAML parser for the repo's taxonomy.yaml structure (id/title/children only).
    Does not support general YAML; intended specifically for jira-mindmap taxonomy.
    """

    def parse_scalar(value: str) -> str:
        value = value.strip()
        if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
            return value[1:-1]
        return value

    roots: list[dict[str, Any]] = []
    list_stack: list[tuple[int, list[dict[str, Any]]]] = [(0, roots)]
    current_node_by_list_indent: dict[int, dict[str, Any]] = {}

    for raw_line in _read_text(path).splitlines():
        line = raw_line.rstrip()
        if not line.strip() or line.lstrip().startswith('#'):
            continue

        indent = len(line) - len(line.lstrip(' '))
        stripped = line.strip()

        if stripped.startswith('- id:'):
            value = parse_scalar(stripped.split(':', 1)[1])
            while list_stack and list_stack[-1][0] != indent:
                if indent < list_stack[-1][0]:
                    list_stack.pop()
                else:
                    break
            if not list_stack or list_stack[-1][0] != indent:
                raise ValueError(f'Unsupported taxonomy indentation at: {raw_line}')
            node: dict[str, Any] = {'id': value, 'title': '', 'children': []}
            list_stack[-1][1].append(node)
            current_node_by_list_indent[indent] = node
            continue

        if stripped.startswith('title:'):
            value = parse_scalar(stripped.split(':', 1)[1])
            list_indent = indent - 2
            node = current_node_by_list_indent.get(list_indent)
            if not node:
                raise ValueError(f'Orphan title line at: {raw_line}')
            node['title'] = value
            continue

        if stripped.startswith('children:'):
            list_indent = indent - 2
            node = current_node_by_list_indent.get(list_indent)
            if not node:
                raise ValueError(f'Orphan children line at: {raw_line}')
            child_list_indent = indent + 2
            list_stack.append((child_list_indent, node['children']))
            continue

        raise ValueError(f'Unsupported taxonomy line: {raw_line}')

    def to_node(raw_node: Mapping[str, Any]) -> ThemeNode:
        children = tuple(to_node(child) for child in raw_node.get('children', []))
        title = raw_node.get('title', '') or raw_node.get('id', '')
        return ThemeNode(node_id=str(raw_node['id']), title=str(title), children=children)

    return [to_node(node) for node in roots]


def _load_classification(db_path: Path) -> dict[str, IssueClassification]:
    uri = f'file:{db_path}?mode=ro'
    conn = sqlite3.connect(uri, uri=True)
    try:
        rows = conn.execute(
            'SELECT issue_key, issue_updated, theme_id, confidence, reason, locked FROM issueclassification'
        ).fetchall()
    finally:
        conn.close()

    out: dict[str, IssueClassification] = {}
    for issue_key, issue_updated, theme_id, confidence, reason, locked in rows:
        out[str(issue_key)] = IssueClassification(
            issue_key=str(issue_key),
            issue_updated=str(issue_updated),
            theme_id=str(theme_id),
            confidence=float(confidence),
            reason=str(reason),
            locked=bool(locked),
        )
    return out


class JiraClient:
    def __init__(self, base_url: str, token: str, username: str | None, api_version: int) -> None:
        self._base_url = base_url.rstrip('/')
        self._token = token
        self._username = username
        self._api_version = api_version

    def _headers(self) -> dict[str, str]:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'kb.py/1.0',
        }

        if self._username:
            raw = f'{self._username}:{self._token}'.encode()
            headers['Authorization'] = 'Basic ' + base64.b64encode(raw).decode('ascii')
        else:
            headers['Authorization'] = f'Bearer {self._token}'

        return headers

    def search(self, jql: str, fields: list[str], max_results: int) -> list[JiraIssue]:
        endpoint = f'{self._base_url}/rest/api/{self._api_version}/search'
        start_at = 0
        page_size = min(100, max_results)
        out: list[JiraIssue] = []

        while True:
            payload = {
                'jql': ' '.join(line.strip() for line in jql.splitlines() if line.strip()),
                'startAt': start_at,
                'maxResults': page_size,
                'fields': fields,
            }

            req = urllib.request.Request(endpoint, data=json.dumps(payload).encode('utf-8'), headers=self._headers())
            data = _jira_request_json(req, base_url=self._base_url)

            issues = data.get('issues') or []
            if not isinstance(issues, list):
                raise RuntimeError('Unexpected Jira response: issues is not a list')

            for raw_issue in issues:
                issue = _parse_issue(raw_issue)
                out.append(issue)

            total = int(data.get('total') or 0)
            start_at += len(issues)

            if start_at >= total:
                break
            if start_at >= max_results:
                break

        return out

    def issue(self, key: str, *, all_fields: bool) -> dict[str, Any]:
        issue_key = str(key).strip()
        if not issue_key:
            raise RuntimeError('Issue key is empty')

        endpoint = f'{self._base_url}/rest/api/{self._api_version}/issue/{urllib.parse.quote(issue_key)}'
        params: dict[str, str] = {'fields': '*all'}
        if all_fields:
            params['expand'] = 'names,schema,renderedFields'
        else:
            params['expand'] = 'names'
        query = urllib.parse.urlencode(params)
        url = f'{endpoint}?{query}' if query else endpoint
        req = urllib.request.Request(url, headers=self._headers())
        data = _jira_request_json(req, base_url=self._base_url)
        if not isinstance(data, dict):
            raise RuntimeError('Unexpected Jira response: issue is not an object')
        return data


def _jira_request_json(req: urllib.request.Request, *, base_url: str, timeout_s: int = 60) -> dict[str, Any]:
    retry_delays_s = [0.0, 1.0, 2.0]
    last_http_error: urllib.error.HTTPError | None = None
    last_url_error: urllib.error.URLError | None = None

    for attempt, delay_s in enumerate(retry_delays_s, start=1):
        if delay_s:
            time.sleep(delay_s)

        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            last_http_error = e
            if e.code in {502, 503, 504} and attempt < len(retry_delays_s):
                continue
            body = e.read().decode('utf-8', errors='replace') if hasattr(e, 'read') else ''
            raise RuntimeError(f'Jira API error ({base_url}): HTTP {e.code} {e.reason}\n{body}') from e
        except urllib.error.URLError as e:
            last_url_error = e
            if attempt < len(retry_delays_s) and _is_retryable_url_error(e):
                continue
            raise RuntimeError(f'Jira API error ({base_url}): {_summarize_url_error(e)}') from e

    if last_http_error is not None:
        raise RuntimeError(
            f'Jira API error ({base_url}): HTTP {last_http_error.code} {last_http_error.reason}'
        ) from last_http_error
    if last_url_error is not None:
        raise RuntimeError(f'Jira API error ({base_url}): {_summarize_url_error(last_url_error)}') from last_url_error
    raise RuntimeError(f'Jira API error ({base_url}): unknown error')


def _summarize_url_error(error: urllib.error.URLError) -> str:
    if _is_dns_url_error(error):
        return 'DNS error'
    return str(error)


def _is_dns_url_error(error: urllib.error.URLError) -> bool:
    reason = getattr(error, 'reason', None)
    if isinstance(reason, OSError):
        errno = getattr(reason, 'errno', None)
        if errno in {-3, -2}:
            return True

    text = str(error).lower()
    dns_tokens = [
        'temporary failure in name resolution',
        'name or service not known',
        'nodename nor servname provided',
        'unknown node or service',
    ]
    return any(token in text for token in dns_tokens)


def _is_retryable_url_error(error: urllib.error.URLError) -> bool:
    reason = getattr(error, 'reason', None)
    if isinstance(reason, OSError):
        errno = getattr(reason, 'errno', None)
        if errno in {-3, -2, 101, 104, 110, 111, 113}:
            return True

    text = str(error).lower()
    retry_tokens = [
        'temporary failure in name resolution',
        'name or service not known',
        'timed out',
        'timeout',
        'connection reset',
        'network is unreachable',
        'no route to host',
        'connection refused',
    ]
    return any(token in text for token in retry_tokens)


def cmd_jira_issue(args: argparse.Namespace) -> int:
    config_path = _resolve_repo_path(args.config)
    config = _load_toml(config_path)
    client = _load_jira_client(config)
    include_comments = bool(getattr(args, 'comments', False))
    comment_limit_raw = getattr(args, 'comment_limit', 5)
    comment_limit = int(comment_limit_raw) if comment_limit_raw is not None else 5
    if comment_limit < 0:
        raise RuntimeError('--comment-limit must be >= 0')
    comment_limit = min(comment_limit, 50)

    issue = client.issue(str(args.key), all_fields=bool(getattr(args, 'all', False)))
    if getattr(args, 'all', False):
        payload = issue
    else:
        payload = _compact_issue_payload(issue=issue, include_comments=include_comments, comment_limit=comment_limit)
    print(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')
    return 0


def _compact_issue_payload(*, issue: Mapping[str, Any], include_comments: bool, comment_limit: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in ('key', 'id', 'self'):
        value = issue.get(k)
        if value:
            out[k] = value

    names_raw = issue.get('names')
    names = names_raw if isinstance(names_raw, dict) else {}

    raw_fields = issue.get('fields')
    fields = raw_fields if isinstance(raw_fields, dict) else {}

    comment_block: dict[str, Any] | None = None
    if include_comments:
        comment_block = _compact_issue_comments(fields.get('comment'), limit=comment_limit)

    drop_fields = {
        'attachment',
        'comment',
        'worklog',
        'customfield_10005',
        'customfield_10400',
        'customfield_13503',
        'customfield_13502',
        'watches',
        'votes',
        'lastViewed',
        'aggregateprogress',
        'progress',
    }
    keep_long_fields = {'description'}
    max_string_len = 800
    max_json_len = 4000
    compact_fields: dict[str, Any] = {}
    for name, value in fields.items():
        if name in drop_fields:
            continue
        cleaned = _compact_issue_value(value)
        if not _is_effectively_empty(cleaned):
            if name not in keep_long_fields:
                if isinstance(cleaned, str) and len(cleaned) > max_string_len:
                    continue
                if isinstance(cleaned, (dict, list)):
                    try:
                        size = len(json.dumps(cleaned, ensure_ascii=False, separators=(',', ':')))
                    except (TypeError, ValueError):
                        size = 0
                    if size > max_json_len:
                        continue
            compact_fields[name] = cleaned

    if comment_block and not _is_effectively_empty(comment_block):
        compact_fields['comment'] = comment_block

    field_names: dict[str, str] = {}
    for field_id in sorted(k for k in compact_fields.keys() if k.startswith('customfield_')):
        label = names.get(field_id)
        if label:
            field_names[field_id] = str(label)
    if field_names:
        out['field_names'] = field_names

    out['fields'] = compact_fields
    return out


def _compact_issue_comments(value: Any, *, limit: int) -> dict[str, Any] | None:
    if limit <= 0:
        return None
    if not isinstance(value, dict):
        return None

    raw_comments = value.get('comments') or []
    if not isinstance(raw_comments, list) or not raw_comments:
        total = value.get('total')
        return {'total': int(total)} if isinstance(total, int) else None

    comments = [c for c in raw_comments if isinstance(c, dict)]
    selected = comments[-limit:] if len(comments) > limit else comments

    max_body_len = 800
    out_comments: list[dict[str, Any]] = []
    for comment in selected:
        out_comment: dict[str, Any] = {}
        for k in ('id', 'created', 'updated'):
            v = comment.get(k)
            if v:
                out_comment[k] = v

        author = comment.get('author')
        if author is not None:
            compact_author = _compact_issue_value(author)
            if not _is_effectively_empty(compact_author):
                out_comment['author'] = compact_author

        body = comment.get('body')
        if isinstance(body, str):
            text = body.strip()
            if text:
                if len(text) > max_body_len:
                    out_comment['body'] = text[:max_body_len] + '…'
                    out_comment['body_truncated'] = True
                else:
                    out_comment['body'] = text

        if out_comment:
            out_comments.append(out_comment)

    total = value.get('total')
    out: dict[str, Any] = {
        'total': int(total) if isinstance(total, int) else len(raw_comments),
        'shown': len(out_comments),
        'comments': out_comments,
    }
    return out


def _compact_issue_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        s = value.strip('\n')
        return s if s.strip() else None

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, list):
        items: list[Any] = []
        for item in value:
            cleaned = _compact_issue_value(item)
            if _is_effectively_empty(cleaned):
                continue
            items.append(cleaned)
        return items if items else None

    if isinstance(value, dict):
        if _looks_like_jira_entity(value):
            keep = (
                'id',
                'key',
                'name',
                'value',
                'displayName',
                'accountId',
                'emailAddress',
                'active',
                'disabled',
                'subtask',
                'released',
                'releaseDate',
                'statusCategory',
                'type',
            )
            out: dict[str, Any] = {}
            for k in keep:
                if k not in value:
                    continue
                cleaned = _compact_issue_value(value.get(k))
                if _is_effectively_empty(cleaned):
                    continue
                out[k] = cleaned
            return out if out else None

        out = {}
        for k, v in value.items():
            if k in {'self', 'avatarUrls', 'iconUrl'}:
                continue
            cleaned = _compact_issue_value(v)
            if _is_effectively_empty(cleaned):
                continue
            out[k] = cleaned
        return out if out else None

    return value


def _looks_like_jira_entity(value: Mapping[str, Any]) -> bool:
    if 'self' not in value:
        return False
    markers = {'id', 'name', 'key', 'value', 'displayName', 'accountId'}
    return any(k in value for k in markers)


def _is_effectively_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def _parse_issue(raw_issue: Mapping[str, Any]) -> JiraIssue:
    fields = raw_issue.get('fields') or {}
    if not isinstance(fields, dict):
        fields = {}

    status = fields.get('status') or {}
    priority = fields.get('priority') or {}
    assignee = fields.get('assignee') or {}
    issuetype = fields.get('issuetype') or {}

    labels = fields.get('labels') or []
    if not isinstance(labels, list):
        labels = []

    def name_or_none(value: Any) -> str | None:
        if isinstance(value, dict):
            name = value.get('name') or value.get('displayName')
            return str(name) if name else None
        return str(value) if value else None

    def str_or_none(value: Any) -> str | None:
        return str(value) if value else None

    return JiraIssue(
        key=str(raw_issue.get('key') or ''),
        summary=str(fields.get('summary') or ''),
        status=name_or_none(status),
        priority=name_or_none(priority),
        assignee=name_or_none(assignee),
        created=str_or_none(fields.get('created')),
        updated=str_or_none(fields.get('updated')),
        duedate=str_or_none(fields.get('duedate')),
        issuetype=name_or_none(issuetype),
        labels=[str(x) for x in labels if x],
    )


def _parse_snapshot_issue(raw_issue: Mapping[str, Any]) -> JiraIssue:
    def str_or_none(value: Any) -> str | None:
        return str(value) if value else None

    labels = raw_issue.get('labels') or []
    if not isinstance(labels, list):
        labels = []

    return JiraIssue(
        key=str(raw_issue.get('key') or ''),
        summary=str(raw_issue.get('summary') or ''),
        status=str_or_none(raw_issue.get('status')),
        priority=str_or_none(raw_issue.get('priority')),
        assignee=str_or_none(raw_issue.get('assignee')),
        created=str_or_none(raw_issue.get('created')),
        updated=str_or_none(raw_issue.get('updated')),
        duedate=str_or_none(raw_issue.get('duedate')),
        issuetype=str_or_none(raw_issue.get('issuetype')),
        labels=[str(x) for x in labels if x],
    )


def _load_jira_client(config: dict[str, Any]) -> JiraClient:
    jira_cfg = config.get('jira') or {}
    if not isinstance(jira_cfg, dict):
        raise RuntimeError('Invalid config: [jira] must be a table')

    _load_dotenv()

    base_url = os.environ.get('JIRA_URL') or ''
    token = os.environ.get('JIRA_TOKEN') or ''
    username = os.environ.get('JIRA_USERNAME')

    if not base_url or not token:
        raise RuntimeError(
            'Missing Jira auth. Set `JIRA_URL` and `JIRA_TOKEN` (and optionally `JIRA_USERNAME`) '
            'or put them into `.env`.'
        )

    api_version = int(jira_cfg.get('api_version') or 2)
    return JiraClient(base_url=base_url, token=token, username=username, api_version=api_version)


def _latest_snapshot() -> Path | None:
    candidates = _sorted_snapshots()
    return candidates[-1] if candidates else None


def _sorted_snapshots() -> list[Path]:
    if not SNAPSHOT_DIR.exists():
        return []
    return sorted(SNAPSHOT_DIR.glob('*.json'))


def _previous_snapshot(current: Path | None) -> Path | None:
    candidates = _sorted_snapshots()
    if not candidates:
        return None
    if current is None:
        return candidates[-2] if len(candidates) >= 2 else None
    try:
        idx = candidates.index(current)
    except ValueError:
        return candidates[-2] if len(candidates) >= 2 else None
    return candidates[idx - 1] if idx >= 1 else None


def _load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(_read_text(path))


def _render_brief(
    *,
    config: dict[str, Any],
    snapshot: dict[str, Any],
    theme_resolver: ThemeResolver | None,
    classifications: dict[str, IssueClassification] | None,
    rendered_at: dt.datetime,
    jira_sync_error: str | None,
) -> str:
    jira_cfg = config.get('jira') or {}
    taxonomy_cfg = config.get('taxonomy') or {}
    confidence_low = float(taxonomy_cfg.get('confidence_low') or 0.80)

    issues_raw = snapshot.get('issues') or []
    issues = [_parse_snapshot_issue(x) for x in issues_raw]

    snapshot_generated_at = snapshot.get('generated_at') or rendered_at.isoformat(timespec='seconds')
    today = rendered_at.date()
    jql = snapshot.get('jql') or jira_cfg.get('jql') or ''

    counts_by_status: dict[str, int] = {}
    for issue in issues:
        counts_by_status[issue.status or 'Unknown'] = counts_by_status.get(issue.status or 'Unknown', 0) + 1

    def jira_link(key: str) -> str:
        base = os.environ.get('JIRA_URL', '').rstrip('/')
        if not base:
            return key
        return f'{base}/browse/{key}'

    def theme_label(issue_key: str) -> str | None:
        if not classifications or not theme_resolver:
            return None
        cls = classifications.get(issue_key)
        if not cls:
            return None
        canonical = theme_resolver.resolve_path(cls.theme_id)
        title_path = theme_resolver.title_path(canonical) if canonical else None
        return title_path or canonical or cls.theme_id

    def due_bucket(issue: JiraIssue) -> tuple[dt.date | None, int | None]:
        due = _parse_ymd_date(issue.duedate)
        if not due:
            return None, None
        return due, (due - today).days

    def format_issue_line(
        issue: JiraIssue, *, cls: IssueClassification | None = None, include_theme: bool = True
    ) -> str:
        status = issue.status or '—'
        priority = issue.priority or '—'

        due, delta = due_bucket(issue)
        due_part = ''
        if due and delta is not None:
            if delta < 0:
                due_part = f', due {due.isoformat()} ({abs(delta)}d overdue)'
            elif delta == 0:
                due_part = f', due {due.isoformat()} (today)'
            else:
                due_part = f', due {due.isoformat()} (in {delta}d)'

        suffix = ''
        if cls:
            suffix = f' | conf={cls.confidence:.2f}' + (' | locked' if cls.locked else '')

        theme_part = ''
        if include_theme:
            label = theme_label(issue.key)
            if label:
                theme_part = f' — {label}'

        return f'- [{issue.key}]({jira_link(issue.key)}) — {issue.summary} ({status}, {priority}{due_part}{suffix}){theme_part}'

    grouped: dict[str, list[tuple[JiraIssue, IssueClassification | None, str | None]]] = {}
    needs_review: list[str] = []

    for issue in issues:
        cls = classifications.get(issue.key) if classifications else None
        canonical = theme_resolver.resolve_path(cls.theme_id) if (theme_resolver and cls) else None
        title_path = theme_resolver.title_path(canonical) if (theme_resolver and canonical) else None
        group_key = title_path or canonical or (cls.theme_id if cls else 'Без темы')
        grouped.setdefault(group_key, []).append((issue, cls, canonical))

        if not cls:
            needs_review.append(f'- {issue.key} — {issue.summary} — нет классификации (theme_id)')
        elif cls.confidence < confidence_low:
            needs_review.append(
                f'- {issue.key} — {issue.summary} — низкая уверенность {cls.confidence:.2f} ({cls.theme_id})'
            )
        elif not canonical and theme_resolver:
            needs_review.append(f'- {issue.key} — {issue.summary} — theme_id не маппится на taxonomy ({cls.theme_id})')

    open_issues = [issue for issue in issues if not _is_done_status(issue.status)]
    done_issues = [issue for issue in issues if _is_done_status(issue.status)]

    overdue_or_soon: list[tuple[int, JiraIssue]] = []
    for issue in open_issues:
        due, delta = due_bucket(issue)
        if not due or delta is None:
            continue
        if delta <= 2:
            overdue_or_soon.append((delta, issue))
    overdue_or_soon.sort(key=lambda t: (t[0], _priority_rank(t[1].priority), t[1].key))

    focus_candidates = sorted(
        open_issues,
        key=lambda i: (
            _priority_rank(i.priority),
            due_bucket(i)[0] or dt.date.max,
            _status_rank(i.status),
            i.key,
        ),
    )[:5]

    lines: list[str] = []
    lines.append(f'# Daily Brief — {today.isoformat()}')
    lines.append('')
    lines.append(f'**Rendered at**: {rendered_at.isoformat(timespec="seconds")}')
    lines.append(f'**Snapshot at**: {str(snapshot_generated_at)}')
    if jira_sync_error:
        lines.append(f'**Jira sync**: unavailable ({jira_sync_error})')
    lines.append('')
    lines.append('## Jira Query')
    lines.append('```jql')
    lines.append(' '.join(line.strip() for line in str(jql).splitlines() if line.strip()))
    lines.append('```')
    lines.append('')
    lines.append('## Summary')
    lines.append(f'- Total: {len(issues)}')
    if counts_by_status:
        status_parts = ', '.join(
            f'{k}: {v}' for k, v in sorted(counts_by_status.items(), key=lambda kv: (-kv[1], kv[0]))
        )
        lines.append(f'- By status: {status_parts}')
    lines.append(f'- Open: {len(open_issues)} | Done: {len(done_issues)}')
    lines.append('')

    lines.append('## Focus candidates (top 5)')
    if focus_candidates:
        for issue in focus_candidates:
            cls = classifications.get(issue.key) if classifications else None
            lines.append(format_issue_line(issue, cls=cls, include_theme=True))
    else:
        lines.append('- (none)')
    lines.append('')

    lines.append('## Overdue / Due soon (<= 2 days)')
    if overdue_or_soon:
        for _, issue in overdue_or_soon:
            cls = classifications.get(issue.key) if classifications else None
            lines.append(format_issue_line(issue, cls=cls, include_theme=True))
    else:
        lines.append('- (none)')
    lines.append('')

    lines.append('## Needs Review')
    if needs_review:
        lines.extend(needs_review)
    else:
        lines.append('- (none)')
    lines.append('')

    lines.append('## By Theme')
    for group_name in sorted(grouped.keys()):
        items = grouped[group_name]
        lines.append('')
        lines.append(f'### {group_name}')
        for issue, cls, _ in sorted(
            items,
            key=lambda t: (
                _status_rank(t[0].status),
                _priority_rank(t[0].priority),
                due_bucket(t[0])[0] or dt.date.max,
                t[0].key,
            ),
        ):
            lines.append(format_issue_line(issue, cls=cls, include_theme=False))

    if done_issues:
        lines.append('')
        lines.append('## Done (FYI)')
        for issue in sorted(
            done_issues,
            key=lambda i: (
                due_bucket(i)[0] or dt.date.max,
                _priority_rank(i.priority),
                i.key,
            ),
        ):
            cls = classifications.get(issue.key) if classifications else None
            lines.append(format_issue_line(issue, cls=cls, include_theme=True))
        lines.append('')

    return _compact_lines('\n'.join(lines))


def _render_eod(
    *,
    prev: dict[str, Any] | None,
    cur: dict[str, Any],
    closing_date: dt.date,
    manual_sections: dict[str, list[str]] | None,
    theme_resolver: ThemeResolver | None,
    classifications: dict[str, IssueClassification] | None,
    jira_sync_error: str | None,
) -> str:
    prev_issues = {str(x.get('key') or ''): x for x in (prev.get('issues') or [])} if prev else {}
    cur_issues = {str(x.get('key') or ''): x for x in (cur.get('issues') or [])}

    prev_keys = set(prev_issues.keys())
    cur_keys = set(cur_issues.keys())

    has_prev = prev is not None

    added: list[str] = []
    removed: list[str] = []
    status_changed: list[tuple[str, str | None, str | None]] = []
    updated_changed: list[str] = []

    if has_prev:
        added = sorted(cur_keys - prev_keys)
        removed = sorted(prev_keys - cur_keys)

        for key in sorted(prev_keys & cur_keys):
            prev_name = prev_issues[key].get('status')
            cur_name = cur_issues[key].get('status')
            if prev_name != cur_name:
                status_changed.append((key, prev_name, cur_name))
            if prev_issues[key].get('updated') != cur_issues[key].get('updated'):
                updated_changed.append(key)

    generated_at = cur.get('generated_at') or _now().isoformat(timespec='seconds')
    lines: list[str] = []
    lines.append(f'# End of Day — {closing_date.isoformat()}')
    lines.append('')
    lines.append(f'**Generated at**: {generated_at}')
    if jira_sync_error:
        lines.append(f'**Jira sync**: unavailable ({jira_sync_error})')
    lines.append('')
    lines.append('## Jira Delta (vs previous snapshot)')
    if not has_prev:
        lines.append('- (no previous snapshot to compare)')
    else:
        lines.append(f'- Added: {len(added)}')
        lines.append(f'- Removed: {len(removed)}')
        lines.append(f'- Status changed: {len(status_changed)}')
        lines.append(f'- Updated: {len(updated_changed)}')
    lines.append('')

    if has_prev and added:
        lines.append('### Added')
        for key in added:
            lines.append(f'- {key}')
        lines.append('')

    if has_prev and removed:
        lines.append('### Removed')
        for key in removed:
            lines.append(f'- {key}')
        lines.append('')

    if has_prev and status_changed:
        lines.append('### Status changed')
        for key, before, after in status_changed:
            lines.append(f'- {key}: {before or "—"} → {after or "—"}')
        lines.append('')

    def resolve_theme(issue_key: str) -> str | None:
        if not theme_resolver or not classifications:
            return None
        cls = classifications.get(issue_key)
        if not cls:
            return None
        canonical = theme_resolver.resolve_path(cls.theme_id)
        title_path = theme_resolver.title_path(canonical) if canonical else None
        return title_path or canonical or cls.theme_id

    def has_content(body: list[str] | None) -> bool:
        return bool(body) and any(line.strip() for line in body)

    def has_time_tracking(body: list[str] | None) -> bool:
        if not has_content(body):
            return False
        cleaned = [line.strip() for line in (body or []) if line.strip()]
        placeholder = {'- Suggested buckets (adjust):'}
        if cleaned and all(line in placeholder for line in cleaned):
            return False
        return True

    notes_body = (manual_sections or {}).get('Notes (keep short)')
    links_body = (manual_sections or {}).get('Links (optional)')
    time_body = (manual_sections or {}).get('Time Tracking (draft)')
    friction_body = (manual_sections or {}).get('Friction / Improvements (optional)')

    lines.append('## Notes (keep short)')
    if has_content(notes_body):
        lines.extend(notes_body or [])
    else:
        lines.append('- Add 3–7 bullets: what moved, what blocked, what decided.')
    lines.append('')

    lines.append('## Links (optional)')
    if has_content(links_body):
        lines.extend(links_body or [])
    else:
        lines.append('- Add links to meeting/technical notes created today.')
    lines.append('')

    mentioned_keys: list[str] = []
    if has_content(notes_body):
        text = '\n'.join(notes_body or [])
        mentioned_keys = re.findall(r'\b[A-Z][A-Z0-9]+-\d+\b', text)
        mentioned_keys = list(dict.fromkeys(mentioned_keys))

    stale_mentioned: list[str] = []
    if prev and mentioned_keys:
        status_changed_keys = {k for k, _b, _a in status_changed}
        for key in mentioned_keys:
            if key not in prev_issues or key not in cur_issues:
                continue
            if key in status_changed_keys:
                continue
            if key in updated_changed:
                continue
            stale_mentioned.append(key)

    lines.append('## Jira Hygiene (optional)')
    if not prev:
        lines.append('- (no previous snapshot to compare)')
    elif stale_mentioned:
        lines.append('- Mentioned in Notes, but Jira `updated` did not change since previous snapshot:')
        for key in stale_mentioned[:10]:
            issue = _parse_snapshot_issue(cur_issues.get(key) or {})
            status = issue.status or '—'
            lines.append(f'  - {key} — {issue.summary} ({status})')
        lines.append('- If you worked on them today: consider moving status and/or adding a short comment.')
    else:
        lines.append('- (none)')
    lines.append('')

    lines.append('## Time Tracking (draft)')
    if has_time_tracking(time_body):
        lines.extend(time_body or [])
        lines.append('')
    else:
        candidate_keys = []
        candidate_keys.extend([key for key, _b, _a in status_changed])
        candidate_keys.extend(added)
        candidate_keys.extend(updated_changed)
        candidate_keys = [k for i, k in enumerate(candidate_keys) if k and k not in candidate_keys[:i]]
        if not candidate_keys:
            candidate_keys = sorted(cur_keys)

        lines.append('- Suggested buckets (adjust):')
        for key in candidate_keys[:8]:
            raw = cur_issues.get(key) or {}
            issue = _parse_snapshot_issue(raw)
            theme = resolve_theme(key)
            theme_part = f' — {theme}' if theme else ''
            lines.append(f'  - {key} — __h — {issue.summary}{theme_part}')
        lines.append('')

    lines.append('## Friction / Improvements (optional)')
    if has_content(friction_body):
        lines.extend(friction_body or [])
    else:
        lines.append('- Add 1–3 bullets: what was inconvenient and should be improved.')
    lines.append('')

    return _compact_lines('\n'.join(lines))


def cmd_doctor(args: argparse.Namespace) -> int:
    include_archive = bool(args.include_archive)

    paths_to_scan: list[Path] = []
    for name in ('configs', 'docs', 'notes', 'scripts', 'templates'):
        p = REPO_ROOT / name
        if p.exists():
            paths_to_scan.append(p)

    if include_archive:
        paths_to_scan.append(REPO_ROOT / 'archive')

    abs_path_hits: list[tuple[Path, int, str]] = []
    abs_path_re = re.compile(r'/home/[^\\s]+')

    for root in paths_to_scan:
        for file_path in root.rglob('*'):
            if not file_path.is_file():
                continue
            try:
                rel = file_path.relative_to(REPO_ROOT)
            except ValueError:
                rel = file_path
            if str(rel) in {'scripts/kb.py', 'configs/kb.toml'}:
                continue
            if file_path.suffix.lower() not in {'.md', '.txt', '.py', '.sh', '.toml', '.json', '.sql', '.yaml', '.yml'}:
                continue
            try:
                content = _read_text(file_path)
            except (OSError, UnicodeDecodeError):
                continue
            for idx, line in enumerate(content.splitlines(), start=1):
                if abs_path_re.search(line):
                    abs_path_hits.append((file_path.relative_to(REPO_ROOT), idx, line.strip()))

    report_lines: list[str] = []
    report_lines.append(f'# KB Doctor — {_now().date().isoformat()}')
    report_lines.append('')

    now = _now()
    work_date = _default_work_date(now)
    eod_date = None
    if EOD_PATH.exists():
        try:
            eod_date = _parse_ymd_date(_extract_eod_date(_read_text(EOD_PATH)) or '')
        except OSError:
            eod_date = None
    latest_snapshot = _latest_snapshot()
    snapshot_at = None
    if latest_snapshot:
        try:
            snapshot_at = str(_load_snapshot(latest_snapshot).get('generated_at') or '')
        except (OSError, json.JSONDecodeError):
            snapshot_at = None

    report_lines.append('## State')
    report_lines.append(f'- Now: {now.strftime("%Y-%m-%d %H:%M (%A) %Z")}')
    report_lines.append(f'- Default work date: {work_date.isoformat()}')
    report_lines.append(f'- `.env` present: {"yes" if DOTENV_PATH.exists() else "no"}')
    report_lines.append(f'- Latest snapshot: {latest_snapshot.name if latest_snapshot else "(none)"}')
    if snapshot_at:
        report_lines.append(f'- Latest snapshot at: {snapshot_at}')
    report_lines.append(f'- EOD container date: {eod_date.isoformat() if eod_date else "(missing/unknown)"}')
    if eod_date and eod_date != work_date:
        report_lines.append('- Hint: run `python3 scripts/kb.py open-day` to switch the container date.')
    report_lines.append('')

    report_lines.append('## Checks')
    report_lines.append(f'- Scan roots: {", ".join(str(p.relative_to(REPO_ROOT)) for p in paths_to_scan)}')
    report_lines.append(f'- Absolute path hits: {len(abs_path_hits)}')
    report_lines.append('')

    if abs_path_hits:
        report_lines.append('## Absolute Paths')
        for path, line_no, line in abs_path_hits[:50]:
            report_lines.append(f'- {path}:{line_no} — {line}')
        if len(abs_path_hits) > 50:
            report_lines.append(f'- … {len(abs_path_hits) - 50} more')
        report_lines.append('')

    output = _compact_lines('\n'.join(report_lines))
    if args.write:
        out_path = Path(args.write)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        _write_text(out_path, output)
        print(str(out_path))
    else:
        print(output)
    return 0


def cmd_retro(args: argparse.Namespace) -> int:
    days = int(getattr(args, 'days', 7) or 7)
    if days <= 0:
        raise RuntimeError('--days must be >= 1')

    logs: list[Path] = []
    if DAILY_LOGS_DIR.exists():
        for p in DAILY_LOGS_DIR.glob('*.md'):
            if p.name == 'README.md':
                continue
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}\.md', p.name):
                logs.append(p)

    logs = sorted(logs, reverse=True)[:days]

    items: list[tuple[str, str]] = []
    for path in logs:
        try:
            md = _read_text(path)
        except OSError:
            continue
        sections = _extract_h2_section_bodies(md)
        friction_body: list[str] | None = None
        for title, body in sections.items():
            if title.strip().lower().startswith('friction'):
                friction_body = body
                break
        if not friction_body:
            continue
        for line in friction_body:
            stripped = line.strip()
            if not stripped.startswith('-'):
                continue
            text = stripped.lstrip('-').strip()
            if not text:
                continue
            if text.lower().startswith('add 1–3 bullets') or text.lower().startswith('add 1-3 bullets'):
                continue
            items.append((path.stem, text))

    now = _now()
    out_lines: list[str] = []
    out_lines.append(f'# Retro — {now.date().isoformat()}')
    out_lines.append('')
    out_lines.append(f'**Generated at**: {now.isoformat(timespec="seconds")}')
    out_lines.append(f'**Window**: last {len(logs)} daily logs')
    out_lines.append('')
    out_lines.append('## Sources')
    if logs:
        for p in sorted(logs):
            out_lines.append(f'- {p.relative_to(REPO_ROOT)}')
    else:
        out_lines.append('- (none)')
    out_lines.append('')
    out_lines.append('## Friction Items')
    if items:
        for day, text in items:
            out_lines.append(f'- {day} — {text}')
    else:
        out_lines.append('- (none)')
    out_lines.append('')

    output = _compact_lines('\n'.join(out_lines))
    if getattr(args, 'write', None):
        out_path = Path(args.write)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        _write_text(out_path, output)
        print(str(out_path))
    else:
        print(output)
    return 0


def cmd_time_report(args: argparse.Namespace) -> int:
    now = _now()
    target_date = _parse_ymd_date(getattr(args, 'date', None)) or _default_work_date(now)
    week_mode = bool(getattr(args, 'week', False))
    jira_mode = bool(getattr(args, 'jira', False))

    start = target_date - dt.timedelta(days=target_date.weekday()) if week_mode else target_date
    dates = [start + dt.timedelta(days=i) for i in range((target_date - start).days + 1)]

    latest_snapshot = _latest_snapshot()
    issue_titles: dict[str, str] = {}
    if latest_snapshot:
        try:
            snapshot = _load_snapshot(latest_snapshot)
            for raw in snapshot.get('issues') or []:
                issue = _parse_snapshot_issue(raw)
                if issue.key:
                    issue_titles[issue.key] = issue.summary
        except (OSError, json.JSONDecodeError):
            issue_titles = {}

    # Add stable titles for "bucket tasks"; they are excluded from the assigned-issues snapshot.
    for key, title in _load_time_bucket_titles().items():
        issue_titles.setdefault(key, title)

    per_day: dict[dt.date, dict[str, float]] = {}
    per_day_missing: dict[dt.date, list[str]] = {}
    per_day_reviews: dict[dt.date, dict[str, list[str]]] = {}
    per_day_lines: dict[dt.date, list[TimeTrackingEntry]] = {}

    for day in dates:
        entries, missing, reviews, lines = _extract_time_entries_for_day(day=day, issue_titles=issue_titles)
        if entries:
            per_day[day] = entries
        if missing:
            per_day_missing[day] = missing
        if reviews:
            per_day_reviews[day] = reviews
        if lines:
            per_day_lines[day] = lines

    totals: dict[str, float] = {}
    for day_entries in per_day.values():
        for bucket, hours in day_entries.items():
            totals[bucket] = totals.get(bucket, 0.0) + hours

    missing_all: dict[str, None] = {}
    for day_missing in per_day_missing.values():
        for bucket in day_missing:
            missing_all[bucket] = None

    total_hours = round(sum(totals.values()), 2)
    days_with_any_data = sum(1 for day in dates if day in per_day or day in per_day_missing)
    if jira_mode:
        total_minutes = 0
        for day in dates:
            for entry in per_day_lines.get(day, []):
                total_minutes += _hours_to_minutes(entry.hours)
        days_without_any_data = [day for day in dates if day not in per_day and day not in per_day_missing]

        output = _format_time_report_jira(
            now=now,
            week_mode=week_mode,
            start=start,
            target_date=target_date,
            dates=dates,
            per_day_lines=per_day_lines,
            issue_titles=issue_titles,
            total_minutes=int(total_minutes),
            days_without_any_data=days_without_any_data,
            missing_buckets=sorted(missing_all.keys()),
        )
        if getattr(args, 'write', None):
            out_path = Path(args.write)
            if not out_path.is_absolute():
                out_path = REPO_ROOT / out_path
            _write_text(out_path, output)
            print(str(out_path))
        else:
            print(output)
        return 0

    if week_mode:
        title = f'Time Report — week-to-date ({start.isoformat()}..{target_date.isoformat()})'
    else:
        title = f'Time Report — {target_date.isoformat()}'

    out_lines: list[str] = []
    out_lines.append(f'# {title}')
    out_lines.append('')
    out_lines.append(f'**Generated at**: {now.isoformat(timespec="seconds")}')
    out_lines.append('')
    out_lines.append('## Totals')
    out_lines.append(f'- Filled: {total_hours}h')
    out_lines.append(f'- Days scanned: {days_with_any_data}/{len(dates)}')
    out_lines.append(f'- Missing buckets: {len(missing_all)}')
    out_lines.append('')

    if not week_mode:
        out_lines.append('## Worklog (copy/paste)')
        entries = per_day_lines.get(target_date, [])
        if entries:
            for entry in entries:
                label = _format_bucket_worklog_label(bucket=entry.bucket, issue_titles=issue_titles)
                duration = _format_hours_hhmm(entry.hours)
                desc = entry.review or 'без деталей'
                if desc == 'без деталей' and entry.description:
                    desc = _derive_time_entry_review(entry.description)
                out_lines.append(f'- {label} — {duration} — {desc}')
        else:
            out_lines.append('- (none)')
        out_lines.append('')

    if week_mode:
        out_lines.append('## By Day (filled)')
        any_days = False
        for day in dates:
            hours = round(sum(per_day.get(day, {}).values()), 2)
            if hours <= 0:
                continue
            any_days = True
            out_lines.append(f'- {day.isoformat()}: {hours}h')
        if not any_days:
            out_lines.append('- (none)')
        out_lines.append('')

    out_lines.append('## By Bucket (filled)')
    if totals:
        bucket_reviews: dict[str, str] = {}
        for day in dates:
            for bucket, reviews in per_day_reviews.get(day, {}).items():
                if not reviews:
                    continue
                bucket_reviews[bucket] = _pick_time_entry_review(reviews)

        lines_by_bucket: dict[str, list[TimeTrackingEntry]] = {}
        if not week_mode and dates:
            for entry in per_day_lines.get(dates[-1], []):
                lines_by_bucket.setdefault(entry.bucket, []).append(entry)

        for bucket, hours in sorted(totals.items(), key=lambda kv: (-kv[1], kv[0])):
            label = _format_bucket_label(bucket=bucket, issue_titles=issue_titles)
            review = bucket_reviews.get(bucket) or 'без деталей'
            out_lines.append(f'- {label} — {round(hours, 2)}h — {review}')

            if not week_mode:
                for entry in lines_by_bucket.get(bucket, []):
                    desc = entry.description or entry.review or 'без деталей'
                    out_lines.append(f'  - {round(entry.hours, 2)}h — {desc}')
    else:
        out_lines.append('- (none)')
    out_lines.append('')

    if missing_all:
        out_lines.append('## Missing (needs confirmation)')
        for bucket in sorted(missing_all.keys()):
            out_lines.append(f'- {_format_bucket_label(bucket=bucket, issue_titles=issue_titles)}')
        out_lines.append('')

    output = _compact_lines('\n'.join(out_lines))
    if getattr(args, 'write', None):
        out_path = Path(args.write)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        _write_text(out_path, output)
        print(str(out_path))
    else:
        print(output)
    return 0


def _format_bucket_label(*, bucket: str, issue_titles: Mapping[str, str]) -> str:
    key = _extract_issue_key(bucket)
    if key and key in issue_titles:
        return f'{key} — {issue_titles[key]}'
    return bucket


def _format_bucket_worklog_label(*, bucket: str, issue_titles: Mapping[str, str]) -> str:
    key = _extract_issue_key(bucket)
    if key and key in issue_titles:
        return f'{key} ({issue_titles[key]})'
    return key or bucket


def _hours_to_minutes(hours: float) -> int:
    return max(0, int(round(hours * 60)))


def _format_hours_hhmm(hours: float) -> str:
    minutes = _hours_to_minutes(hours)
    h = minutes // 60
    m = minutes % 60
    return f'{h}:{m:02d}'


def _format_minutes_jira(minutes: int) -> str:
    minutes = max(0, int(minutes))
    h = minutes // 60
    m = minutes % 60
    if h <= 0:
        return f'{m}m'
    if m <= 0:
        return f'{h}h'
    return f'{h}h {m}m'


def _format_hours_jira(hours: float) -> str:
    return _format_minutes_jira(_hours_to_minutes(hours))


def _format_time_report_jira(
    *,
    now: dt.datetime,
    week_mode: bool,
    start: dt.date,
    target_date: dt.date,
    dates: Sequence[dt.date],
    per_day_lines: Mapping[dt.date, Sequence[TimeTrackingEntry]],
    issue_titles: Mapping[str, str],
    total_minutes: int,
    days_without_any_data: Sequence[dt.date],
    missing_buckets: Sequence[str],
) -> str:
    lines: list[str] = []

    tz = now.tzname() or 'local'
    total_s = _format_minutes_jira(int(total_minutes))

    if week_mode:
        lines.append(f'Неделя для списания: {start.isoformat()}..{target_date.isoformat()} ({tz}).')
    else:
        lines.append(f'День для списания: {target_date.isoformat()} ({tz}).')

    lines.append(f'Заполнено по KB: `{total_s}`.')

    if days_without_any_data:
        missing_days_s = ', '.join(day.isoformat() for day in days_without_any_data)
        lines.append(f'Нет записей: {missing_days_s}.')

    if missing_buckets:
        missing_keys = ', '.join(f'`{b}`' for b in missing_buckets)
        lines.append(f'Есть `__h` (нужно подтвердить): {missing_keys}.')

    lines.append('')

    for day in dates:
        entries = list(per_day_lines.get(day, []))
        if not entries:
            continue

        lines.append(f'- {day.isoformat()}')
        for entry in entries:
            key = _extract_issue_key(entry.bucket) or entry.bucket
            title = _normalize_space(issue_titles.get(key) or entry.title or '<название?>').replace('`', "'")
            duration = _format_hours_jira(entry.hours)
            desc = entry.description or entry.review or 'без деталей'
            desc = _normalize_space(desc).replace('`', "'")

            lines.append(f'  - `{key}` : `{title}` : `{duration}`')
            lines.append(f'    - `{desc}`')

        lines.append('')

    return _compact_lines('\n'.join(lines))


@dataclasses.dataclass(frozen=True)
class TimeTrackingEntry:
    bucket: str
    title: str
    hours: float
    description: str
    review: str


def _extract_time_entries_for_day(
    *, day: dt.date, issue_titles: Mapping[str, str]
) -> tuple[dict[str, float], list[str], dict[str, list[str]], list[TimeTrackingEntry]]:
    md = _load_day_markdown(day)
    if not md:
        return {}, [], {}, []

    sections = _extract_h2_section_bodies(md)
    time_body: list[str] | None = None
    for title, body in sections.items():
        if title.strip().lower().startswith('time tracking'):
            time_body = body
            break
    if not time_body:
        return {}, [], {}, []

    totals: dict[str, float] = {}
    missing: dict[str, None] = {}
    reviews: dict[str, list[str]] = {}
    lines: list[TimeTrackingEntry] = []

    for raw_line in time_body:
        line = raw_line.strip()
        if not line.startswith('-'):
            continue
        item = line.lstrip('-').strip()
        if not item:
            continue
        if item.lower().startswith('suggested buckets'):
            continue

        parsed = _parse_time_tracking_item(item)
        if not parsed:
            continue

        bucket_raw, rest = parsed
        bucket = _extract_issue_key(bucket_raw) or bucket_raw
        hours = _parse_hours_to_float(rest)
        if hours is None:
            missing[bucket] = None
            continue
        totals[bucket] = totals.get(bucket, 0.0) + hours

        review = _derive_time_entry_review(rest)
        if review:
            reviews.setdefault(bucket, []).append(review)

        title = _extract_time_entry_title(rest)
        description = _extract_time_entry_description(rest)
        lines.append(TimeTrackingEntry(bucket=bucket, title=title, hours=hours, description=description, review=review))

    return totals, sorted(missing.keys()), reviews, lines


def _parse_time_tracking_item(item: str) -> tuple[str, str] | None:
    bucket_raw: str | None = None
    rest: str | None = None

    split = _split_outside_parens(text=item, seps=('—',))
    if split:
        bucket_raw, rest = split
    else:
        split = _split_outside_parens(text=item, seps=('–',))
        if split:
            bucket_raw, rest = split

    if not bucket_raw or not rest:
        return None

    return bucket_raw.strip(), rest.strip()


def _split_outside_parens(*, text: str, seps: tuple[str, ...]) -> tuple[str, str] | None:
    depth = 0
    for i, ch in enumerate(text):
        if ch == '(':
            depth += 1
            continue
        if ch == ')':
            depth = max(0, depth - 1)
            continue
        if depth == 0 and ch in seps:
            left = text[:i].strip()
            right = text[i + 1 :].strip()
            if left and right:
                return left, right
            return None
    return None


def _split_all_outside_parens(*, text: str, seps: tuple[str, ...]) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    depth = 0

    def flush() -> None:
        part = ''.join(buf).strip()
        if part:
            parts.append(part)

    for ch in text:
        if ch == '(':
            depth += 1
            buf.append(ch)
            continue
        if ch == ')':
            depth = max(0, depth - 1)
            buf.append(ch)
            continue

        if depth == 0 and ch in seps:
            flush()
            buf = []
            continue

        buf.append(ch)

    flush()
    return parts


def _normalize_space(text: str) -> str:
    return re.sub(r'\s+', ' ', text.strip())


def _looks_like_hours_placeholder(text: str) -> bool:
    value = text.strip()
    if not value:
        return False
    return '__' in value or '?' in value


def _normalize_time_tracking_item(*, item: str, issue_titles: Mapping[str, str]) -> str:
    """
    Ensure time-tracking items include a stable issue title when possible.

    Canonical form:
    - KEY — Title — 1:30 (details)
    - KEY — Title — __h
    """

    parsed = _parse_time_tracking_item(item)
    if not parsed:
        return item

    bucket_raw, rest = parsed
    issue_key = _extract_issue_key(bucket_raw)
    if not issue_key:
        return item

    title = issue_titles.get(issue_key)
    if not title:
        return item

    parts = _split_all_outside_parens(text=rest, seps=('—', '–'))
    if not parts:
        return item

    # If the first segment is not a duration/placeholder, assume the title is already present.
    if not _looks_like_hours_placeholder(parts[0]) and _parse_hours_to_float(parts[0]) is None:
        return item

    normalized_title = _normalize_space(title)
    out_parts: list[str] = [normalized_title, parts[0].strip()]
    for part in parts[1:]:
        if _normalize_space(part).casefold() == normalized_title.casefold():
            continue
        out_parts.append(part.strip())

    new_rest = ' — '.join(p for p in out_parts if p)
    return f'{bucket_raw.strip()} — {new_rest}'.strip()


def _normalize_time_tracking_body(
    *, body_lines: Sequence[str], issue_titles: Mapping[str, str]
) -> tuple[list[str], bool]:
    changed = False
    out: list[str] = []

    for raw in body_lines:
        line = raw.rstrip('\n')
        stripped = line.lstrip()
        indent = line[: len(line) - len(stripped)]

        if not stripped.startswith('-'):
            out.append(line)
            continue

        item = stripped.lstrip('-').strip()
        if not item:
            out.append(line)
            continue
        if item.lower().startswith('suggested buckets'):
            out.append(line)
            continue

        normalized = _normalize_time_tracking_item(item=item, issue_titles=issue_titles)
        if normalized != item:
            changed = True
        out.append(f'{indent}- {normalized}'.rstrip())

    return out, changed


def _normalize_time_tracking_titles_in_markdown(*, markdown: str, issue_titles: Mapping[str, str]) -> tuple[str, bool]:
    """
    Rewrite only the '## Time Tracking (draft)' section body, preserving the rest.
    """

    lines = markdown.splitlines()
    out: list[str] = []

    in_time = False
    buf: list[str] = []
    changed = False

    def flush_time() -> None:
        nonlocal changed, buf
        normalized, body_changed = _normalize_time_tracking_body(body_lines=buf, issue_titles=issue_titles)
        out.extend(normalized)
        changed = changed or body_changed
        buf = []

    for line in lines:
        if line.startswith('## '):
            title = line[3:].strip()
            if in_time:
                flush_time()
                in_time = False

            out.append(line)
            if title.lower().startswith('time tracking'):
                in_time = True
            continue

        if in_time:
            buf.append(line)
        else:
            out.append(line)

    if in_time:
        flush_time()

    result = '\n'.join(out).rstrip() + '\n'
    if result != markdown.rstrip() + '\n':
        changed = True
    return result, changed


def _build_issue_titles(*, snapshot: Mapping[str, Any] | None) -> dict[str, str]:
    issue_titles: dict[str, str] = {}

    if snapshot:
        try:
            for raw in snapshot.get('issues') or []:
                issue = _parse_snapshot_issue(raw)
                if issue.key:
                    issue_titles[issue.key] = issue.summary
        except Exception:  # noqa: BLE001
            issue_titles = {}

    for key, title in _load_time_bucket_titles().items():
        issue_titles.setdefault(key, title)

    return issue_titles


def _maybe_normalize_time_tracking_titles_in_file(
    *, path: Path, expected_date: dt.date, issue_titles: Mapping[str, str]
) -> None:
    if not path.exists():
        return
    try:
        md = _read_text(path)
    except OSError:
        return
    if _extract_eod_date(md) != expected_date.isoformat():
        return
    updated, changed = _normalize_time_tracking_titles_in_markdown(markdown=md, issue_titles=issue_titles)
    if changed:
        _write_text(path, updated)


def _derive_time_entry_review(rest: str) -> str:
    review_source = ''
    if '(' in rest and ')' in rest:
        start = rest.find('(')
        end = rest.rfind(')')
        if 0 <= start < end:
            review_source = rest[start + 1 : end].strip()

    if not review_source:
        review_source = rest

    text = re.sub(r'~?\b\d{1,2}:\d{2}\s*[–-]\s*~?\d{1,2}:\d{2}\b', ' ', review_source)
    text = re.sub(r'~?\b\d{1,2}:\d{2}\b', ' ', text)
    text = re.sub(r'[;,·+]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    # Prefer keeping the prefix when the whole string is already short (e.g. "заведение задач: PROJ-152..155").
    candidate = text
    candidate_words = [w for w in candidate.split() if w]
    if len(candidate_words) > 5 and ':' in candidate:
        candidate = candidate.split(':')[-1].strip()
        candidate_words = [w for w in candidate.split() if w]

    words = candidate_words
    if not words:
        return 'без деталей'
    return ' '.join(words[:5])


def _extract_time_entry_title(rest: str) -> str:
    """
    Extract a Jira issue title from the right-hand side of the time-tracking line when present.

    Canonical forms:
    - Title — 1:30 (details)
    - Title — __h
    """

    parts = _split_all_outside_parens(text=rest, seps=('—', '–'))
    if not parts:
        return ''

    head = parts[0].strip()
    if not head:
        return ''
    if _looks_like_hours_placeholder(head):
        return ''
    if _parse_hours_to_float(head) is not None:
        return ''
    return _normalize_space(head)


def _extract_time_entry_description(rest: str) -> str:
    """
    Extract a human-friendly description of the time entry from the right-hand side of the tracking line.

    Prefers text inside parentheses, because that's how we usually write the "what was done" part.
    Falls back to a lightly-cleaned version of `rest`.
    """

    source = ''
    if '(' in rest and ')' in rest:
        start = rest.find('(')
        end = rest.rfind(')')
        if 0 <= start < end:
            source = rest[start + 1 : end].strip()

    if not source:
        source = rest

    source = re.sub(r'\s*(?:—|–)\s*округлено\s*$', '', source, flags=re.IGNORECASE).strip()
    source = re.sub(r'^\s*(\d{1,2}\s*:\s*\d{2})\s*', '', source).strip()
    source = re.sub(
        r'^\s*(\d+(?:[.,]\d+)?)\s*(?:ч|час(?:а|ов)?|h|hr|hrs|hour|hours)\b', '', source, flags=re.IGNORECASE
    ).strip()
    source = re.sub(
        r'^\s*(\d+(?:[.,]\d+)?)\s*(?:м|мин|минут(?:а|ы)?|m|min|mins|minute|minutes)\b', '', source, flags=re.IGNORECASE
    ).strip()
    source = re.sub(r'~?\b\d{1,2}:\d{2}\s*[–-]\s*~?\d{1,2}:\d{2}\b', ' ', source)
    source = re.sub(r'~?\b\d{1,2}:\d{2}\b', ' ', source)
    source = re.sub(r'\s*[;,·]\s*', ' ', source)
    source = source.strip(' -–—:;,.+')
    source = re.sub(r'\s+', ' ', source).strip()

    if not source:
        return _derive_time_entry_review(rest)
    return source


def _pick_time_entry_review(reviews: list[str]) -> str:
    for review in reversed(reviews):
        if review:
            return review
    return 'без деталей'


def _load_day_markdown(day: dt.date) -> str | None:
    # Prefer the active day container for the current work date: it is continuously updated
    # during the day and may diverge from a previously generated daily log.
    work_date = _default_work_date(_now())
    if day == work_date and EOD_PATH.exists():
        try:
            md = _read_text(EOD_PATH)
        except OSError:
            md = None
        if md and _extract_eod_date(md) == day.isoformat():
            return md

    daily_path = DAILY_LOGS_DIR / f'{day.isoformat()}.md'
    if daily_path.exists():
        try:
            return _read_text(daily_path)
        except OSError:
            return None

    if EOD_PATH.exists():
        try:
            md = _read_text(EOD_PATH)
        except OSError:
            return None
        if _extract_eod_date(md) == day.isoformat():
            return md

    return None


def _load_time_bucket_titles(path: Path = TIME_BUCKETS_PATH) -> dict[str, str]:
    """
    Load stable Jira titles for bucket tasks (e.g., PROJ-9/PROJ-32/...) from `notes/work/time-buckets.md`.

    This keeps `time-report` outputs readable even though these bucket tasks are excluded
    from the assigned-issues snapshot JQL.
    """

    try:
        if not path.exists():
            return {}
        md = _read_text(path)
    except OSError:
        return {}

    out: dict[str, str] = {}
    for raw_line in md.splitlines():
        line = raw_line.strip()
        if not line.startswith('|'):
            continue

        cells = [c.strip() for c in line.strip('|').split('|')]
        if len(cells) < 3:
            continue

        key = cells[0].strip('` ').strip()
        title = cells[2].strip('` ').strip()
        if not re.fullmatch(r'[A-Z][A-Z0-9]+-\d+', key):
            continue
        if not title or title.lower() in {'название', 'key'}:
            continue
        out[key] = title

    return out


def _extract_issue_key(text: str) -> str | None:
    m = re.search(r'\b[A-Z][A-Z0-9]+-\d+\b', text)
    return m.group(0) if m else None


def _extract_active_open_questions(markdown: str) -> list[str]:
    sections = _extract_h2_section_bodies(markdown)
    for title, body in sections.items():
        if title.strip().lower() in {'active', 'open'}:
            out: list[str] = []
            for raw in body:
                line = raw.strip()
                if not line.startswith('-'):
                    continue
                item = line.lstrip('-').strip()
                if not item or item == '(none)':
                    continue
                out.append(item)
            return out

    out = []
    for raw in markdown.splitlines():
        line = raw.strip()
        if not line.startswith('-'):
            continue
        item = line.lstrip('-').strip()
        if not item or item == '(none)':
            continue
        out.append(item)
    return out


def _extract_open_project_todos(markdown: str) -> dict[str, list[str]]:
    current_project: str | None = None
    out: dict[str, list[str]] = {}

    for raw in markdown.splitlines():
        line = raw.rstrip('\n')
        if line.startswith('## '):
            current_project = line.removeprefix('## ').strip()
            continue

        stripped = line.strip()
        m = re.match(r'^- \[([ xX])\]\s*(.+)$', stripped)
        if not m:
            continue

        checked = (m.group(1) or '').strip()
        text = (m.group(2) or '').strip()
        if not text or checked.lower() == 'x':
            continue

        project = current_project or 'Uncategorized'
        out.setdefault(project, []).append(text)

    return out


def _extract_needs_review_items(markdown: str) -> list[str]:
    sections = _extract_h2_section_bodies(markdown)
    for title, body in sections.items():
        if title.strip().lower().startswith('needs review'):
            out: list[str] = []
            for raw in body:
                line = raw.strip()
                if not line.startswith('-'):
                    continue
                item = line.lstrip('-').strip()
                if not item or item == '(none)':
                    continue
                out.append(item)
            return out
    return []


def _extract_note_mentioned_keys(markdown: str) -> list[str]:
    sections = _extract_h2_section_bodies(markdown)
    for title, body in sections.items():
        if title.strip().lower().startswith('notes'):
            text = '\n'.join(body or [])
            keys = re.findall(r'\b[A-Z][A-Z0-9]+-\d+\b', text)
            return list(dict.fromkeys(keys))
    return []


def _extract_notes_pending_items(markdown: str) -> list[str]:
    """
    Extract carry-over candidates from "## Notes" section.

    Conventions:
    - Bullets starting with "TODO" (case-insensitive) are considered pending.
    - Markdown checkboxes: "- [ ] ..." are pending, "- [x] ..." are done.
    """

    sections = _extract_h2_section_bodies(markdown)
    for title, body in sections.items():
        if not title.strip().lower().startswith('notes'):
            continue
        out: list[str] = []
        for raw in body:
            line = raw.strip()
            if not line.startswith('-'):
                continue
            item = line.lstrip('-').strip()
            if not item or item == '(none)':
                continue

            if re.match(r'(?i)^todo\b', item):
                out.append(item)
                continue

            m = re.match(r'^\[([ xX])\]\s*(.+)$', item)
            if m:
                checked = (m.group(1) or '').strip()
                text = (m.group(2) or '').strip()
                if checked.lower() == 'x':
                    continue
                if text:
                    out.append(text)

        return out
    return []


def _extract_time_tracking_placeholders(markdown: str) -> list[str]:
    sections = _extract_h2_section_bodies(markdown)
    for title, body in sections.items():
        if not title.strip().lower().startswith('time tracking'):
            continue
        out: list[str] = []
        for raw in body:
            line = raw.strip()
            if '__h' not in line:
                continue
            if not line.startswith('-'):
                continue
            item = line.lstrip('-').strip()
            if not item:
                continue
            if item.lower().startswith('suggested buckets'):
                continue
            bucket = item.split('—', 1)[0].split('–', 1)[0].strip()
            out.append(bucket or item)
        return out
    return []


def _extract_jira_hygiene_items(markdown: str) -> list[str]:
    sections = _extract_h2_section_bodies(markdown)
    for title, body in sections.items():
        if title.strip().lower().startswith('jira hygiene'):
            items: list[str] = []
            for raw in body:
                line = raw.strip()
                if not line.startswith('-'):
                    continue
                item = line.lstrip('-').strip()
                if not item:
                    continue
                if item.lower().startswith('(none)') or item.lower().startswith('(no previous snapshot'):
                    continue
                if item.lower().startswith('mentioned in notes') or item.lower().startswith('if you worked'):
                    continue
                if _extract_issue_key(item):
                    items.append(item)
            return items
    return []


def _weekday_short_ru(date: dt.date) -> str:
    names = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    return names[date.weekday()]


def _format_date_with_weekday_ru(date: dt.date) -> str:
    return f'{date.isoformat()} ({_weekday_short_ru(date)})'


def _default_carryover_target_date(closing_date: dt.date) -> dt.date:
    target = closing_date + dt.timedelta(days=1)
    if closing_date.weekday() <= 4:
        while target.weekday() >= 5:
            target += dt.timedelta(days=1)
    return target


def _upsert_open_questions_carryover(*, closing_date: dt.date, carry_items: Sequence[str]) -> None:
    if not carry_items:
        return

    target_date = _default_carryover_target_date(closing_date)
    start_marker = f'<!-- carryover:{closing_date.isoformat()} -->'
    end_marker = f'<!-- /carryover:{closing_date.isoformat()} -->'

    existing_md = ''
    if OPEN_QUESTIONS_PATH.exists():
        try:
            existing_md = _read_text(OPEN_QUESTIONS_PATH)
        except OSError:
            existing_md = ''

    if not existing_md.strip():
        existing_md = _compact_lines(
            '\n'.join(
                [
                    '# Open Questions',
                    '',
                    'This is an optional, lightweight “inbox” for questions that were deferred during the day.',
                    '',
                    '## Active',
                    '',
                    '- (none)',
                    '',
                    '## Resolved (log)',
                    '',
                    '- (none)',
                    '',
                ]
            )
        )

    lines = existing_md.splitlines()

    def find_section(title: str) -> tuple[int | None, int]:
        start: int | None = None
        for idx, line in enumerate(lines):
            if line.strip() == f'## {title}':
                start = idx
                break
        if start is None:
            return None, len(lines)
        end = len(lines)
        for idx in range(start + 1, len(lines)):
            if lines[idx].startswith('## '):
                end = idx
                break
        return start, end

    active_start, active_end = find_section('Active')
    if active_start is None:
        # Insert Active section before the first existing section (or at the end).
        insert_at = len(lines)
        for idx, line in enumerate(lines):
            if line.startswith('## '):
                insert_at = idx
                break
        block = ['', '## Active', '', '- (none)', '']
        lines[insert_at:insert_at] = block
        active_start, active_end = find_section('Active')

    assert active_start is not None

    block_start: int | None = None
    block_end: int | None = None
    for idx in range(active_start + 1, active_end):
        if lines[idx].strip() == start_marker:
            block_start = idx
            continue
        if block_start is not None and lines[idx].strip() == end_marker:
            block_end = idx
            break

    # Track existing items outside the carryover block (to avoid duplicates).
    outside_items: set[str] = set()
    for idx in range(active_start + 1, active_end):
        if block_start is not None and block_end is not None and block_start <= idx <= block_end:
            continue
        stripped = lines[idx].strip()
        if not stripped.startswith('-'):
            continue
        item = stripped.lstrip('-').strip()
        if item and item != '(none)':
            outside_items.add(item)

    block_items: set[str] = set()
    if block_start is not None and block_end is not None:
        for idx in range(block_start + 1, block_end):
            stripped = lines[idx].strip()
            if not stripped.startswith('-'):
                continue
            item = stripped.lstrip('-').strip()
            if not item:
                continue
            if item.startswith('Перенос на '):
                continue
            block_items.add(item)

    pending: list[str] = []
    seen: set[str] = set()
    for raw in carry_items:
        value = str(raw or '').strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        if value in outside_items:
            continue
        if value in block_items:
            continue
        pending.append(value)

    if not pending:
        return

    # Remove placeholder "- (none)" if we are about to add real items.
    for idx in range(active_start + 1, active_end):
        if lines[idx].strip() == '- (none)':
            del lines[idx]
            active_start, active_end = find_section('Active')
            assert active_start is not None
            break

    # Indices may have shifted after edits.
    block_start = None
    block_end = None
    for idx in range(active_start + 1, active_end):
        if lines[idx].strip() == start_marker:
            block_start = idx
            continue
        if block_start is not None and lines[idx].strip() == end_marker:
            block_end = idx
            break

    if block_start is not None and block_end is not None:
        insert_at = block_end
        lines[insert_at:insert_at] = [f'  - {item}' for item in pending]
        _write_text(OPEN_QUESTIONS_PATH, _compact_lines('\n'.join(lines)))
        return

    insert_idx = active_start + 1
    if insert_idx < len(lines) and lines[insert_idx].strip() != '':
        lines.insert(insert_idx, '')
        insert_idx += 1

    block_lines: list[str] = []
    block_lines.append(start_marker)
    target_label = _format_date_with_weekday_ru(target_date)
    closing_label = _format_date_with_weekday_ru(closing_date)
    block_lines.append(f'- Перенос на {target_label} (с {closing_label}):')
    for item in pending:
        block_lines.append(f'  - {item}')
    block_lines.append(end_marker)
    block_lines.append('')

    lines[insert_idx:insert_idx] = block_lines
    _write_text(OPEN_QUESTIONS_PATH, _compact_lines('\n'.join(lines)))


def _rollover_pending_items_from_eod(*, closing_date: dt.date, eod_path: Path) -> None:
    try:
        md = _read_text(eod_path)
    except OSError:
        return

    if _extract_eod_date(md) != closing_date.isoformat():
        return

    carry_items: list[str] = []
    carry_items.extend(_extract_notes_pending_items(md))

    time_placeholders = _extract_time_tracking_placeholders(md)
    if time_placeholders:
        buckets = ', '.join(time_placeholders[:10])
        suffix = '…' if len(time_placeholders) > 10 else ''
        carry_items.append(f'Time tracking {closing_date.isoformat()}: confirm __h for {buckets}{suffix}.')

    _upsert_open_questions_carryover(closing_date=closing_date, carry_items=carry_items)


def _rollover_open_questions_to_next_day(*, closing_date: dt.date) -> None:
    """
    Best-effort rollover for `notes/work/open-questions.md` (manual inbox).

    Today the file contains day-scoped list headers like:
      - High priority (YYYY-MM-DD (Дд)):
        - ...

    When closing the day, we move the header's date to the next workday and merge
    with the existing next-day header if it already exists.
    """

    if not OPEN_QUESTIONS_PATH.exists():
        return

    target_date = _default_carryover_target_date(closing_date)

    try:
        md = _read_text(OPEN_QUESTIONS_PATH)
    except OSError:
        return

    updated = _rollover_open_questions_high_priority(md=md, closing_date=closing_date, target_date=target_date)
    if updated == md:
        return

    _write_text(OPEN_QUESTIONS_PATH, _compact_lines(updated))


def _rollover_open_questions_high_priority(*, md: str, closing_date: dt.date, target_date: dt.date) -> str:
    from_date = closing_date.isoformat()
    to_date = target_date.isoformat()

    lines = md.splitlines()

    active_idx: int | None = None
    for i, line in enumerate(lines):
        if line.strip() == '## Active':
            active_idx = i
            break
    if active_idx is None:
        return md

    active_end = len(lines)
    for i in range(active_idx + 1, len(lines)):
        if lines[i].startswith('## '):
            active_end = i
            break

    def find_block(date_iso: str) -> tuple[int, int, str, list[str]] | None:
        for idx in range(active_idx + 1, active_end):
            stripped = lines[idx].strip()
            if not stripped.startswith('- High priority'):
                continue
            m = re.search(r'\b\d{4}-\d{2}-\d{2}\b', stripped)
            if not m or m.group(0) != date_iso:
                continue

            indent_match = re.match(r'^(\s*)-', lines[idx])
            indent = indent_match.group(1) if indent_match else ''
            child_prefix = indent + '  '

            end = idx + 1
            children: list[str] = []
            while end < active_end and lines[end].startswith(child_prefix):
                children.append(lines[end])
                end += 1

            return idx, end, indent, children
        return None

    from_block = find_block(from_date)
    if not from_block:
        return md

    to_block = find_block(to_date)

    from_header, from_end, from_indent, from_children = from_block
    new_header = f'{from_indent}- High priority ({_format_date_with_weekday_ru(target_date)}):'

    if not to_block:
        lines[from_header] = new_header
        return '\n'.join(lines) + '\n'

    to_header, to_end, _to_indent, to_children = to_block

    seen = {c.strip() for c in to_children}
    merged = list(to_children)
    for child in from_children:
        key = child.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(child)

    # Replace target children.
    lines[to_header + 1 : to_end] = merged
    delta = len(merged) - len(to_children)

    # Delete source block (adjust indices if target was above).
    adj_from_header = from_header + delta if to_header < from_header else from_header
    adj_from_end = from_end + delta if to_header < from_header else from_end
    del lines[adj_from_header:adj_from_end]

    return '\n'.join(lines) + '\n'


def cmd_questions(args: argparse.Namespace) -> int:
    now = _now()
    target_date = _parse_ymd_date(getattr(args, 'date', None)) or _default_work_date(now)

    latest_snapshot = _latest_snapshot()
    issue_titles: dict[str, str] = {}
    if latest_snapshot:
        try:
            snapshot = _load_snapshot(latest_snapshot)
            for raw in snapshot.get('issues') or []:
                issue = _parse_snapshot_issue(raw)
                if issue.key:
                    issue_titles[issue.key] = issue.summary
        except (OSError, json.JSONDecodeError):
            issue_titles = {}

    for key, title in _load_time_bucket_titles().items():
        issue_titles.setdefault(key, title)

    problems: list[str] = []
    if BRIEF_PATH.exists():
        try:
            brief_md = _read_text(BRIEF_PATH)
        except OSError:
            brief_md = ''
        brief_date = _extract_daily_brief_date(brief_md) if brief_md else None
        if brief_date and brief_date != target_date.isoformat():
            problems.append(
                f'`notes/work/daily-brief.md` is for {brief_date} (expected {target_date.isoformat()}); run `python3 scripts/kb.py day-start`.'
            )
    else:
        problems.append('`notes/work/daily-brief.md` is missing; run `python3 scripts/kb.py day-start`.')

    if EOD_PATH.exists():
        try:
            eod_md = _read_text(EOD_PATH)
        except OSError:
            eod_md = ''
        eod_date = _extract_eod_date(eod_md) if eod_md else None
        if eod_date and eod_date != target_date.isoformat():
            problems.append(
                f'`notes/work/end-of-day.md` is for {eod_date} (expected {target_date.isoformat()}); run `python3 scripts/kb.py open-day`.'
            )
    else:
        problems.append('`notes/work/end-of-day.md` is missing; run `python3 scripts/kb.py open-day`.')

    manual_questions: list[str] = []
    if OPEN_QUESTIONS_PATH.exists():
        try:
            manual_questions = _extract_active_open_questions(_read_text(OPEN_QUESTIONS_PATH))
        except OSError:
            manual_questions = []

    project_todos: dict[str, list[str]] = {}
    if PROJECT_TODOS_PATH.exists():
        try:
            project_todos = _extract_open_project_todos(_read_text(PROJECT_TODOS_PATH))
        except OSError:
            project_todos = {}

    needs_review: list[str] = []
    if BRIEF_PATH.exists():
        try:
            needs_review = _extract_needs_review_items(_read_text(BRIEF_PATH))
        except OSError:
            needs_review = []

    md_day = _load_day_markdown(target_date) or ''
    mentioned_keys = _extract_note_mentioned_keys(md_day) if md_day else []
    _filled, missing, _reviews, _lines = _extract_time_entries_for_day(day=target_date, issue_titles=issue_titles)
    missing_filtered: list[str] = []
    for bucket in missing:
        key = _extract_issue_key(bucket)
        if key and key not in mentioned_keys:
            continue
        missing_filtered.append(bucket)

    jira_hygiene: list[str] = _extract_jira_hygiene_items(md_day) if md_day else []

    out_lines: list[str] = []
    out_lines.append(f'# Questions — {target_date.isoformat()}')
    out_lines.append('')
    out_lines.append(f'**Generated at**: {now.isoformat(timespec="seconds")}')
    out_lines.append('')

    if problems:
        out_lines.append('## Problems (repo state)')
        for item in problems:
            out_lines.append(f'- {item}')
        out_lines.append('')

    if missing_filtered:
        out_lines.append('## Time Tracking (needs confirmation)')
        for bucket in missing_filtered:
            out_lines.append(f'- {_format_bucket_label(bucket=bucket, issue_titles=issue_titles)} — __h')
        out_lines.append('')

    if jira_hygiene:
        out_lines.append('## Jira Hygiene (possible drift)')
        for item in jira_hygiene:
            label_key = _extract_issue_key(item) or item
            label = _format_bucket_label(bucket=label_key, issue_titles=issue_titles)
            out_lines.append(f'- {label} — confirm status/comment update?')
        out_lines.append('')

    if needs_review:
        out_lines.append('## Needs Review (taxonomy)')
        for item in needs_review:
            out_lines.append(f'- {item}')
        out_lines.append('')

    if manual_questions:
        out_lines.append('## Open Questions (manual)')
        for item in manual_questions:
            out_lines.append(f'- {item}')
        out_lines.append('')

    if project_todos:
        out_lines.append('## TODOs (by project)')
        max_items = 20
        shown = 0
        for project_name in sorted(project_todos.keys()):
            items = project_todos.get(project_name) or []
            if not items:
                continue
            out_lines.append(f'- {project_name} — {len(items)} open')
            for item in items:
                if shown >= max_items:
                    break
                out_lines.append(f'  - {item}')
                shown += 1
            if shown >= max_items:
                break
        total = sum(len(v) for v in project_todos.values())
        if total > shown:
            try:
                todo_rel = str(PROJECT_TODOS_PATH.relative_to(REPO_ROOT))
            except ValueError:
                todo_rel = str(PROJECT_TODOS_PATH)
            out_lines.append(f'- … {total - shown} more (see `{todo_rel}`)')
        out_lines.append('')

    if not (problems or missing_filtered or jira_hygiene or needs_review or manual_questions or project_todos):
        out_lines.append('## Nothing pending')
        out_lines.append('- (none)')
        out_lines.append('')

    output = _compact_lines('\n'.join(out_lines))
    if getattr(args, 'write', None):
        out_path = Path(args.write)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        _write_text(out_path, output)
        print(str(out_path))
    else:
        print(output)
    return 0


def _parse_hours_to_float(text: str) -> float | None:
    value = text.strip()
    if not value:
        return None

    head = value.split('(', 1)[0].strip()
    if not head:
        return None
    if '__' in head or '?' in head:
        return None

    m = re.search(r'(\d{1,2})\s*:\s*(\d{2})', head)
    if m:
        return int(m.group(1)) + int(m.group(2)) / 60

    total = 0.0
    found = False

    for m in re.finditer(r'(\d+(?:[.,]\d+)?)\s*(ч|час(?:а|ов)?|h|hr|hrs|hour|hours)\b', head, flags=re.IGNORECASE):
        total += float(m.group(1).replace(',', '.'))
        found = True

    for m in re.finditer(
        r'(\d+(?:[.,]\d+)?)\s*(м|мин|минут(?:а|ы)?|m|min|mins|minute|minutes)\b',
        head,
        flags=re.IGNORECASE,
    ):
        total += float(m.group(1).replace(',', '.')) / 60
        found = True

    if found:
        return round(total, 2)

    m = re.fullmatch(r'\s*(\d+(?:[.,]\d+)?)\s*', head)
    if m:
        return float(m.group(1).replace(',', '.'))

    return None


def cmd_jira_sync(args: argparse.Namespace) -> int:
    out_path = _jira_sync(config_path=_resolve_repo_path(args.config), jql_override=args.jql, out_path=args.out)
    print(str(out_path))
    return 0


def _jira_sync(*, config_path: Path, jql_override: str | None, out_path: str | None) -> Path:
    config = _load_toml(config_path)
    jira_cfg = config.get('jira') or {}
    if not isinstance(jira_cfg, dict):
        raise RuntimeError('Invalid config: [jira] must be a table')

    jql = jql_override or str(jira_cfg.get('jql') or '')
    fields = list(jira_cfg.get('fields') or [])
    max_results = int(jira_cfg.get('max_results') or 200)

    client = _load_jira_client(config)
    issues = client.search(jql=jql, fields=fields, max_results=max_results)

    ts = _now()
    snapshot = {
        'generated_at': ts.isoformat(timespec='seconds'),
        'jql': jql,
        'issues': [dataclasses.asdict(issue) for issue in issues],
    }

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    resolved_out = _resolve_repo_path(out_path) if out_path else SNAPSHOT_DIR / f'{_slug_timestamp(ts)}.json'
    _json_dump(resolved_out, snapshot)
    return resolved_out


def cmd_render_brief(args: argparse.Namespace) -> int:
    config = _load_toml(_resolve_repo_path(args.config))

    snapshot_path = _resolve_repo_path(args.snapshot) if args.snapshot else _latest_snapshot()
    if not snapshot_path:
        raise RuntimeError('No snapshots found. Run `jira-sync` first.')
    snapshot = _load_snapshot(snapshot_path)

    theme_resolver, classifications = _load_theme_context(
        config,
        taxonomy_override=args.taxonomy,
        classification_db_override=args.classification_db,
    )

    md = _render_brief(
        config=config,
        snapshot=snapshot,
        theme_resolver=theme_resolver,
        classifications=classifications,
        rendered_at=_now(),
        jira_sync_error=None,
    )

    out_path = _resolve_repo_path(args.out) if args.out else BRIEF_PATH
    _write_text(out_path, md)
    print(str(out_path))
    return 0


def _load_theme_context(
    config: dict[str, Any],
    *,
    taxonomy_override: str | None,
    classification_db_override: str | None,
) -> tuple[ThemeResolver | None, dict[str, IssueClassification] | None]:
    taxonomy_cfg = config.get('taxonomy') or {}
    taxonomy_path_raw = (taxonomy_override or taxonomy_cfg.get('path') or '').strip()
    taxonomy_path = Path(taxonomy_path_raw) if taxonomy_path_raw else Path()
    if taxonomy_path and not taxonomy_path.is_absolute():
        taxonomy_path = _resolve_repo_path(taxonomy_path)

    class_db_raw = (classification_db_override or taxonomy_cfg.get('classification_db') or '').strip()
    class_db_path = Path(class_db_raw) if class_db_raw else Path()
    if class_db_path and not class_db_path.is_absolute():
        class_db_path = _resolve_repo_path(class_db_path)

    theme_resolver: ThemeResolver | None = None
    if taxonomy_path and taxonomy_path.exists():
        theme_resolver = ThemeResolver(_parse_simple_taxonomy_yaml(taxonomy_path))

    classifications: dict[str, IssueClassification] | None = None
    if class_db_path and class_db_path.exists():
        classifications = _load_classification(class_db_path)

    return theme_resolver, classifications


def cmd_render_eod(args: argparse.Namespace) -> int:
    config = _load_toml(_resolve_repo_path(args.config))

    snapshot_path = _resolve_repo_path(args.snapshot) if args.snapshot else _latest_snapshot()
    if not snapshot_path:
        raise RuntimeError('No snapshots found. Run `jira-sync` first.')
    cur = _load_snapshot(snapshot_path)

    prev_path = _resolve_repo_path(args.prev) if args.prev else None
    prev: dict[str, Any] | None = None
    if prev_path:
        prev = _load_snapshot(prev_path)

    generated_at = cur.get('generated_at') or _now().isoformat(timespec='seconds')
    snapshot_date = _parse_ymd_date(str(generated_at)) or _now().date()
    closing_date = _parse_ymd_date(getattr(args, 'date', None)) or snapshot_date

    out_path = _resolve_repo_path(args.out) if args.out else EOD_PATH
    daily_log_path = DAILY_LOGS_DIR / f'{closing_date.isoformat()}.md'

    preserve_md: str | None = None
    if out_path.exists():
        existing = _read_text(out_path)
        if _extract_eod_date(existing) == closing_date.isoformat():
            preserve_md = existing
    if preserve_md is None and daily_log_path.exists():
        existing = _read_text(daily_log_path)
        if _extract_eod_date(existing) == closing_date.isoformat():
            preserve_md = existing

    sections = _extract_h2_section_bodies(preserve_md) if preserve_md else {}
    manual_sections: dict[str, list[str]] = {}

    for title, body in sections.items():
        t = title.strip().lower()
        if t.startswith('notes') and 'Notes (keep short)' not in manual_sections:
            manual_sections['Notes (keep short)'] = body
        elif t.startswith('links') and 'Links (optional)' not in manual_sections:
            manual_sections['Links (optional)'] = body
        elif t.startswith('time tracking') and 'Time Tracking (draft)' not in manual_sections:
            manual_sections['Time Tracking (draft)'] = body
        elif t.startswith('friction') and 'Friction / Improvements (optional)' not in manual_sections:
            manual_sections['Friction / Improvements (optional)'] = body

    issue_titles = _build_issue_titles(snapshot=cur)
    if 'Time Tracking (draft)' in manual_sections:
        normalized, _changed = _normalize_time_tracking_body(
            body_lines=manual_sections['Time Tracking (draft)'], issue_titles=issue_titles
        )
        manual_sections['Time Tracking (draft)'] = normalized

    theme_resolver, classifications = _load_theme_context(
        config,
        taxonomy_override=getattr(args, 'taxonomy', None),
        classification_db_override=getattr(args, 'classification_db', None),
    )

    md = _render_eod(
        prev=prev,
        cur=cur,
        closing_date=closing_date,
        manual_sections=manual_sections,
        theme_resolver=theme_resolver,
        classifications=classifications,
        jira_sync_error=getattr(args, 'jira_sync_error', None),
    )

    _write_text(out_path, md)
    if getattr(args, 'write_daily_log', False):
        _write_text(daily_log_path, md)
    print(str(out_path))
    return 0


def cmd_open_day(args: argparse.Namespace) -> int:
    now = _now()
    target_date = _parse_ymd_date(getattr(args, 'date', None)) or _default_work_date(now)
    out_path = _resolve_repo_path(args.out) if getattr(args, 'out', None) else EOD_PATH
    rotate_previous = not bool(getattr(args, 'no_rotate', False))

    resolved = _ensure_open_day(target_date=target_date, out_path=out_path, rotate_previous=rotate_previous)
    # Best-effort: inject stable titles into Time Tracking lines using the latest snapshot,
    # so the KB keeps titles even if Jira changes later.
    snapshot: dict[str, Any] | None = None
    latest = _latest_snapshot()
    if latest:
        try:
            snapshot = _load_snapshot(latest)
        except (OSError, json.JSONDecodeError):
            snapshot = None
    issue_titles = _build_issue_titles(snapshot=snapshot)
    _maybe_normalize_time_tracking_titles_in_file(path=resolved, expected_date=target_date, issue_titles=issue_titles)
    print(str(resolved))
    reminders = _load_reminders_db()
    if reminders or REMINDERS_PATH.exists():
        print('')
        _print_reminders_summary(target_date=target_date, entries=reminders)
    return 0


def cmd_render_jira_doc(args: argparse.Namespace) -> int:
    config_path = _resolve_repo_path(args.config)
    config = _load_toml(config_path)
    out_path = _resolve_repo_path(args.out) if getattr(args, 'out', None) else JIRA_DOC_PATH
    md = _render_jira_doc(config=config, rendered_at=_now(), config_path=config_path)
    _write_text(out_path, md)
    print(str(out_path))
    return 0


def _render_jira_doc(*, config: dict[str, Any], rendered_at: dt.datetime, config_path: Path) -> str:
    jira_cfg = config.get('jira') or {}
    if not isinstance(jira_cfg, dict):
        jira_cfg = {}

    jql = str(jira_cfg.get('jql') or '').strip()
    jql_one_line = ' '.join(line.strip() for line in jql.splitlines() if line.strip())

    lines: list[str] = []
    lines.append('# Jira — Assigned Tasks (helper)')
    lines.append('')
    lines.append(f'**Updated**: {rendered_at.date().isoformat()}')
    try:
        config_rel = str(config_path.relative_to(REPO_ROOT))
    except ValueError:
        config_rel = str(config_path)
    lines.append(f'**Canonical**: `{config_rel}` → `[jira].jql`')
    lines.append('')
    lines.append('## JQL')
    lines.append('```jql')
    lines.append(jql_one_line)
    lines.append('```')
    lines.append('')
    lines.append('## Notes')
    lines.append('- Edit JQL in `configs/kb.toml`; this file is generated.')
    lines.append('- Scope: only `assignee` (мои задачи).')
    lines.append('')
    return _compact_lines('\n'.join(lines))


def _ensure_open_day(*, target_date: dt.date, out_path: Path, rotate_previous: bool) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    DAILY_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    existing_md: str | None = None
    existing_date: dt.date | None = None

    if out_path.exists():
        try:
            existing_md = _read_text(out_path)
        except OSError:
            existing_md = None
        if existing_md is not None:
            existing_date_str = _extract_eod_date(existing_md)
            existing_date = _parse_ymd_date(existing_date_str) if existing_date_str else None

    if rotate_previous and existing_md and existing_date and existing_date != target_date:
        rotate_path = DAILY_LOGS_DIR / f'{existing_date.isoformat()}.md'
        if not rotate_path.exists():
            _write_text(rotate_path, existing_md)

    if rotate_previous and existing_md and existing_date is None:
        rotate_path = DAILY_LOGS_DIR / f'unknown-{_slug_timestamp(_now())}.md'
        if not rotate_path.exists():
            _write_text(rotate_path, existing_md)

    if existing_date == target_date and existing_md:
        existing_sections = {title.strip().lower() for title in _extract_h2_section_bodies(existing_md).keys()}
        missing_chunks: list[str] = []

        if not any(title.startswith('links') for title in existing_sections):
            missing_chunks.extend(
                [
                    '## Links (optional)',
                    '- Add links to meeting/technical notes created today.',
                    '',
                ]
            )
        if not any(title.startswith('time tracking') for title in existing_sections):
            missing_chunks.extend(
                [
                    '## Time Tracking (draft)',
                    '- Suggested buckets (adjust):',
                    '',
                ]
            )

        if missing_chunks:
            stitched = existing_md.rstrip() + '\n\n' + '\n'.join(missing_chunks).rstrip() + '\n'
            _write_text(out_path, stitched)
        return out_path

    opened_at = _now().isoformat(timespec='seconds')
    md = _render_open_day_skeleton(target_date=target_date, opened_at=opened_at)
    _write_text(out_path, md)
    return out_path


def _render_open_day_skeleton(*, target_date: dt.date, opened_at: str) -> str:
    lines: list[str] = []
    lines.append(f'# End of Day — {target_date.isoformat()}')
    lines.append('')
    lines.append(f'**Generated at**: {opened_at}')
    lines.append('**How to update**: run `python3 scripts/kb.py end-day` (or `--date YYYY-MM-DD`)')
    lines.append('')
    lines.append('## Notes (keep short)')
    lines.append('- Add bullets during the day (what moved, what blocked, what decided).')
    lines.append('')
    lines.append('## Links (optional)')
    lines.append('- Add links to meeting/technical notes created today.')
    lines.append('')
    lines.append('## Time Tracking (draft)')
    lines.append('- Suggested buckets (adjust):')
    lines.append('')
    lines.append('## Friction / Improvements (optional)')
    lines.append('- Add 1–3 bullets: what was inconvenient and should be improved.')
    lines.append('')
    return _compact_lines('\n'.join(lines))


def cmd_day_start(args: argparse.Namespace) -> int:
    # Best-effort: run jira-sync; if unavailable, render from the latest snapshot (if any).
    config_path = _resolve_repo_path(args.config)
    config = _load_toml(config_path)

    rendered_at = _now()
    sync_error: str | None = None
    snapshot_path: Path | None = None

    try:
        snapshot_path = _jira_sync(config_path=config_path, jql_override=args.jql, out_path=None)
    except Exception as e:  # noqa: BLE001
        sync_error = str(e)
        snapshot_path = _latest_snapshot()

    out_path = _resolve_repo_path(args.out) if args.out else BRIEF_PATH

    if snapshot_path and snapshot_path.exists():
        snapshot = _load_snapshot(snapshot_path)
        theme_resolver, classifications = _load_theme_context(
            config,
            taxonomy_override=args.taxonomy,
            classification_db_override=args.classification_db,
        )
        md = _render_brief(
            config=config,
            snapshot=snapshot,
            theme_resolver=theme_resolver,
            classifications=classifications,
            rendered_at=rendered_at,
            jira_sync_error=sync_error,
        )
        _write_text(out_path, md)
        try:
            _ensure_open_day(target_date=rendered_at.date(), out_path=EOD_PATH, rotate_previous=True)
        except OSError:
            pass
        try:
            issue_titles = _build_issue_titles(snapshot=snapshot)
            _maybe_normalize_time_tracking_titles_in_file(
                path=EOD_PATH, expected_date=rendered_at.date(), issue_titles=issue_titles
            )
        except Exception:  # noqa: BLE001
            pass
        try:
            _write_text(
                JIRA_DOC_PATH, _render_jira_doc(config=config, rendered_at=rendered_at, config_path=config_path)
            )
        except OSError:
            pass
        print(str(out_path))
        reminders = _load_reminders_db()
        if reminders or REMINDERS_PATH.exists():
            print('')
            _print_reminders_summary(target_date=rendered_at.date(), entries=reminders)
        return 0

    # No snapshot available: write a minimal stub so `notes/work/daily-brief.md` exists.
    lines: list[str] = []
    lines.append(f'# Daily Brief — {rendered_at.date().isoformat()}')
    lines.append('')
    lines.append(f'**Rendered at**: {rendered_at.isoformat(timespec="seconds")}')
    if sync_error:
        lines.append(f'**Jira sync**: unavailable ({sync_error})')
    else:
        lines.append('**Jira sync**: unavailable (no snapshots found)')
    lines.append('')
    lines.append('## Notes')
    lines.append("- Set Jira env vars (`.env`) or paste today's tasks export.")
    lines.append('')

    _write_text(out_path, _compact_lines('\n'.join(lines)))
    print(str(out_path))
    return 0


def cmd_end_day(args: argparse.Namespace) -> int:
    # Best-effort: run jira-sync; if unavailable, render from existing snapshots (if any).
    explicit_date = _parse_ymd_date(getattr(args, 'date', None))
    now = _now()
    closing_date = explicit_date or _default_work_date(now)

    config_path = _resolve_repo_path(args.config)
    latest_before = _latest_snapshot()
    sync_error: str | None = None

    cur_snapshot: Path | None = None
    prev_snapshot: Path | None = None

    try:
        cur_snapshot = _jira_sync(config_path=config_path, jql_override=args.jql, out_path=None)
        prev_snapshot = latest_before
    except Exception as e:  # noqa: BLE001
        sync_error = str(e)
        cur_snapshot = latest_before
        prev_snapshot = _previous_snapshot(cur_snapshot)

    if not cur_snapshot:
        out_path = _resolve_repo_path(args.out) if args.out else EOD_PATH
        daily_log_path = DAILY_LOGS_DIR / f'{closing_date.isoformat()}.md'

        lines: list[str] = []
        lines.append(f'# End of Day — {closing_date.isoformat()}')
        lines.append('')
        lines.append(f'**Generated at**: {_now().isoformat(timespec="seconds")}')
        if sync_error:
            lines.append(f'**Jira sync**: unavailable ({sync_error})')
        else:
            lines.append('**Jira sync**: unavailable (no snapshots found)')
        lines.append('')
        lines.append('## Notes (keep short)')
        lines.append('- Add 3–7 bullets: what moved, what blocked, what decided.')
        lines.append('')
        lines.append('## Time Tracking (draft)')
        lines.append('- Suggested buckets (adjust):')
        lines.append('')
        lines.append('## Friction / Improvements (optional)')
        lines.append('- Add 1–3 bullets: what was inconvenient and should be improved.')
        lines.append('')

        md = _compact_lines('\n'.join(lines))
        _write_text(out_path, md)
        _write_text(daily_log_path, md)
        print(str(out_path))
        _rollover_pending_items_from_eod(closing_date=closing_date, eod_path=out_path)
        _rollover_open_questions_to_next_day(closing_date=closing_date)
        return 0

    eod_args = argparse.Namespace(
        config=args.config,
        snapshot=str(cur_snapshot),
        prev=str(prev_snapshot) if prev_snapshot else None,
        out=args.out,
        date=closing_date.isoformat(),
        write_daily_log=True,
        taxonomy=getattr(args, 'taxonomy', None),
        classification_db=getattr(args, 'classification_db', None),
        jira_sync_error=sync_error,
    )
    result = cmd_render_eod(eod_args)
    out_path = _resolve_repo_path(args.out) if args.out else EOD_PATH
    if result == 0:
        _rollover_pending_items_from_eod(closing_date=closing_date, eod_path=out_path)
        _rollover_open_questions_to_next_day(closing_date=closing_date)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='kb.py',
        description='Knowledge Base maintenance helper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
              python scripts/kb.py doctor --write notes/work/kb-doctor.md
              python scripts/kb.py retro --days 7 --write notes/work/retro.md
              python scripts/kb.py time-report
              python scripts/kb.py time-report --week
              python scripts/kb.py questions
              python scripts/kb.py jira-sync
              python scripts/kb.py jira-issue PROJ-123
              python scripts/kb.py jira-issue --all PROJ-123
              python scripts/kb.py render-brief
              python scripts/kb.py open-day
              python scripts/kb.py render-jira-doc
              python scripts/kb.py day-start
              python scripts/kb.py end-day
            """
        ).strip(),
    )
    parser.add_argument('--config', default=str(DEFAULT_CONFIG_PATH), help='Path to kb.toml config')

    sub = parser.add_subparsers(dest='cmd', required=True)

    p_doctor = sub.add_parser('doctor', help='Scan repo for common problems (paths, drift, etc.)')
    p_doctor.add_argument('--include-archive', action='store_true', help='Also scan archive/')
    p_doctor.add_argument('--write', help='Write report to a file instead of stdout')
    p_doctor.set_defaults(func=cmd_doctor)

    p_retro = sub.add_parser('retro', help="Aggregate 'Friction / Improvements' from recent daily logs (report-only)")
    p_retro.add_argument('--days', type=int, default=7, help='How many recent daily logs to scan')
    p_retro.add_argument('--write', help='Write report to a file instead of stdout')
    p_retro.set_defaults(func=cmd_retro)

    p_time = sub.add_parser('time-report', help='Summarize time tracking from daily logs (day or week-to-date)')
    p_time.add_argument('--date', help='Target day (YYYY-MM-DD). Default: today (00:00–06:00 → yesterday).')
    p_time.add_argument('--week', action='store_true', help='Week-to-date (Mon..target day) instead of a single day')
    p_time.add_argument(
        '--jira',
        action='store_true',
        help="Output a Telegram-friendly Jira worklog template (per-day, backticks, '1h 30m')",
    )
    p_time.add_argument('--write', help='Write report to a file instead of stdout')
    p_time.set_defaults(func=cmd_time_report)

    p_questions = sub.add_parser('questions', help='Show pending questions / missing info for the user to confirm')
    p_questions.add_argument('--date', help='Target day (YYYY-MM-DD). Default: today (00:00–06:00 → yesterday).')
    p_questions.add_argument('--write', help='Write report to a file instead of stdout')
    p_questions.set_defaults(func=cmd_questions)

    p_typos = sub.add_parser('typos', help='List/search/update a typo->fix glossary (notes/work/typos.md)')
    p_typos.add_argument(
        '--query',
        help='Search typo glossary (substring match; falls back to fuzzy word match, then suggestions)',
    )
    p_typos.add_argument(
        '--add', nargs=2, action='append', metavar=('TYPO', 'FIX'), help='Add/replace an entry (repeatable)'
    )
    p_typos.set_defaults(func=cmd_typos)

    p_reminders = sub.add_parser('reminders', help='List/search/update reminders (notes/work/reminders.md)')
    p_reminders.add_argument('--date', help='Target day (YYYY-MM-DD). Default: today (00:00–06:00 → yesterday).')
    p_reminders.add_argument('--all', action='store_true', help='List all reminders without date filtering.')
    p_reminders.add_argument(
        '--add', nargs=2, action='append', metavar=('RULE', 'TEXT'), help='Add an entry (repeatable)'
    )
    p_reminders.set_defaults(func=cmd_reminders)

    p_sync = sub.add_parser('jira-sync', help='Fetch Jira issues and write a snapshot to logs/')
    p_sync.add_argument('--jql', help='Override JQL from config')
    p_sync.add_argument('--out', help='Output path (default: logs/jira-snapshots/<timestamp>.json)')
    p_sync.set_defaults(func=cmd_jira_sync)

    p_issue = sub.add_parser('jira-issue', help='Fetch a Jira issue JSON (compact by default; use --all for raw)')
    p_issue.add_argument('--all', action='store_true', help='Print the full raw payload (very verbose)')
    p_issue.add_argument('--comments', action='store_true', help='Include a compact tail of comments in the output')
    p_issue.add_argument(
        '--comment-limit',
        type=int,
        default=5,
        help='How many most recent comments to include with --comments (default: 5)',
    )
    p_issue.add_argument('key', help='Issue key (e.g., PROJ-123)')
    p_issue.set_defaults(func=cmd_jira_issue)

    p_brief = sub.add_parser('render-brief', help='Generate notes/work/daily-brief.md from latest snapshot')
    p_brief.add_argument('--snapshot', help='Snapshot JSON path (default: latest in logs/jira-snapshots/)')
    p_brief.add_argument('--out', help='Output markdown path (default: notes/work/daily-brief.md)')
    p_brief.add_argument('--taxonomy', help='Override taxonomy.yaml path')
    p_brief.add_argument('--classification-db', help='Override jira_mindmap.db path')
    p_brief.set_defaults(func=cmd_render_brief)

    p_eod = sub.add_parser('render-eod', help='Generate end-of-day delta from snapshots')
    p_eod.add_argument('--snapshot', help='Snapshot JSON path (default: latest in logs/jira-snapshots/)')
    p_eod.add_argument('--prev', help='Previous snapshot JSON path (default: none)')
    p_eod.add_argument('--date', help='Day to close (YYYY-MM-DD). Default: snapshot date.')
    p_eod.add_argument('--out', help='Output markdown path (default: notes/work/end-of-day.md)')
    p_eod.add_argument('--write-daily-log', action='store_true', help='Also write notes/daily-logs/<date>.md')
    p_eod.add_argument('--taxonomy', help='Override taxonomy.yaml path')
    p_eod.add_argument('--classification-db', help='Override jira_mindmap.db path')
    p_eod.set_defaults(func=cmd_render_eod)

    p_open = sub.add_parser('open-day', help="Ensure today's end-of-day container exists (for log-notes)")
    p_open.add_argument('--date', help='Target day (YYYY-MM-DD). Default: today (00:00–06:00 → yesterday).')
    p_open.add_argument('--out', help='Output markdown path (default: notes/work/end-of-day.md)')
    p_open.add_argument(
        '--no-rotate', action='store_true', help='Do not copy previous end-of-day.md into notes/daily-logs/'
    )
    p_open.set_defaults(func=cmd_open_day)

    p_jira_doc = sub.add_parser('render-jira-doc', help='Generate notes/work/jira.md from configs/kb.toml')
    p_jira_doc.add_argument('--out', help='Output markdown path (default: notes/work/jira.md)')
    p_jira_doc.set_defaults(func=cmd_render_jira_doc)

    p_day = sub.add_parser('day-start', help='Sync Jira and generate daily brief')
    p_day.add_argument('--jql', help='Override JQL from config')
    p_day.add_argument('--out', help='Output markdown path (default: notes/work/daily-brief.md)')
    p_day.add_argument('--taxonomy', help='Override taxonomy.yaml path')
    p_day.add_argument('--classification-db', help='Override jira_mindmap.db path')
    p_day.set_defaults(func=cmd_day_start)

    p_end = sub.add_parser('end-day', help='Sync Jira and generate end-of-day delta report')
    p_end.add_argument('--jql', help='Override JQL from config')
    p_end.add_argument('--date', help='Day to close (YYYY-MM-DD). If omitted and 00:00–06:00 → closes yesterday.')
    p_end.add_argument('--out', help='Output markdown path (default: notes/work/end-of-day.md)')
    p_end.add_argument('--taxonomy', help='Override taxonomy.yaml path')
    p_end.add_argument('--classification-db', help='Override jira_mindmap.db path')
    p_end.set_defaults(func=cmd_end_day)

    return parser


def main(argv: list[str] | None = None) -> int:
    _load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as e:  # noqa: BLE001
        print(f'ERROR: {e}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
