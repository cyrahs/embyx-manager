"""The effect-transcript format (docs/rewrite/SCENARIOS.md), Python side.

A transcript is one JSON document: what an operation returned, the ordered calls it made to
the fakes, the final directory tree, the final database rows and the fakes' final state.
`crates/embyx-core/src/scenario.rs` is the Rust side and must render the same bytes for the
same scenario; the normalization steps below are mirrored there one for one.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any

FORMAT = 1
UUID_RE = re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')
SECTIONS = ('outcome', 'calls', 'tree', 'rows', 'fakes')


def snapshot_tree(roots: dict[str, Path]) -> dict[str, list[dict[str, Any]]]:
    """Every entry under each root (the root itself left out), sorted by path."""
    tree: dict[str, list[dict[str, Any]]] = {}
    for name, root in roots.items():
        entries: list[dict[str, Any]] = []
        for directory, dirnames, filenames in os.walk(root, followlinks=False):
            for entry in [*dirnames, *filenames]:
                path = Path(directory) / entry
                info = path.lstat()
                relative = path.relative_to(root).as_posix()
                if stat.S_ISLNK(info.st_mode):
                    entries.append({'path': relative, 'kind': 'symlink', 'target': str(path.readlink())})
                elif stat.S_ISDIR(info.st_mode):
                    entries.append({'path': relative, 'kind': 'dir'})
                else:
                    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
                    entries.append(
                        {
                            'path': relative,
                            'kind': 'file',
                            'size': info.st_size,
                            'nlink': info.st_nlink,
                            'sha256': digest,
                        }
                    )
        entries.sort(key=lambda entry: entry['path'])
        tree[name] = entries
    return tree


def _walk_strings(value: Any, change: Any) -> Any:
    """`value` with every string (object keys included) passed through `change`."""
    if isinstance(value, str):
        return change(value)
    if isinstance(value, list):
        return [_walk_strings(item, change) for item in value]
    if isinstance(value, dict):
        return {change(key): _walk_strings(item, change) for key, item in value.items()}
    return value


def _ordered_strings(value: Any, out: list[str]) -> None:
    """Strings in the canonical order: object keys sorted, keys before their values."""
    if isinstance(value, str):
        out.append(value)
    elif isinstance(value, list):
        for item in value:
            _ordered_strings(item, out)
    elif isinstance(value, dict):
        for key in sorted(value):
            out.append(key)
            _ordered_strings(value[key], out)


def _replace_roots(document: dict[str, Any], roots: dict[str, Path]) -> dict[str, Any]:
    # Longest first, so a root inside another root's path is not cut short.
    ordered = sorted(roots.items(), key=lambda item: len(str(item[1])), reverse=True)

    def change(text: str) -> str:
        for name, root in ordered:
            text = text.replace(str(root), '{' + name + '}')
        return text

    return _walk_strings(document, change)


def _replace_uuids(document: dict[str, Any]) -> dict[str, Any]:
    seen: dict[str, int] = {}
    strings: list[str] = []
    for section in SECTIONS:
        _ordered_strings(document.get(section), strings)
    for text in strings:
        for match in UUID_RE.findall(text):
            seen.setdefault(match.lower(), len(seen) + 1)

    def change(text: str) -> str:
        return UUID_RE.sub(lambda match: f'<uuid:{seen[match.group(0).lower()]}>', text)

    return _walk_strings(document, change)


def _get_path(value: Any, dotted: str) -> Any:
    for part in dotted.split('.') if dotted else []:
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return None
    return value


def _set_path(value: Any, dotted: str, new: Any) -> None:
    parts = dotted.split('.')
    for part in parts[:-1]:
        value = value[int(part)] if isinstance(value, list) else value[part]
    last = parts[-1]
    if isinstance(value, list):
        value[int(last)] = new
    else:
        value[last] = new


def _rank_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _replace_volatile(document: dict[str, Any], volatile: list[str]) -> None:
    """Replace each selected value by its rank among the distinct values of its selector.

    `rows.<table>.<column>` selects a column of every dumped row; `calls.<fake>.<method>.<path>`
    a dotted path inside every matching call record. Equal values stay equal and the order
    between them is kept, which is all a test may rely on for a timestamp or a generated id.
    """
    for selector in volatile:
        section, _, rest = selector.partition('.')
        places: list[tuple[Any, str]] = []
        if section == 'rows':
            table, _, column = rest.partition('.')
            places = [(row, column) for row in document['rows'].get(table, []) if column in row]
        elif section == 'calls':
            fake, method, path = rest.split('.', 2)
            places = [
                (call, path)
                for call in document['calls']
                if call['fake'] == fake and call['method'] == method and _get_path(call, path) is not None
            ]
        else:
            msg = f'volatile selector {selector!r} must start with rows. or calls.'
            raise ValueError(msg)
        values = sorted({_rank_key(_get_path(holder, path)) for holder, path in places})
        ranks = {value: index + 1 for index, value in enumerate(values)}
        for holder, path in places:
            _set_path(holder, path, f'<volatile:{ranks[_rank_key(_get_path(holder, path))]}>')


def _order_calls(calls: list[dict[str, Any]], mode: str, lane_keys: dict[str, str]) -> list[dict[str, Any]]:
    """Give each call its lane and sequence number; in `lanes` mode, group the calls by lane.

    `total`: one lane (`main`), the global order is the contract. `lanes`: concurrent work
    makes only the order within a lane meaningful (say, the calls about one file); a call's
    lane is the value at its fake's `lane_keys` path, `main` when there is none.
    """
    if mode not in {'total', 'lanes'}:
        msg = f'ordering mode {mode!r} is neither total nor lanes'
        raise ValueError(msg)
    laned = []
    for call in calls:
        lane = 'main'
        if mode == 'lanes' and call['fake'] in lane_keys:
            value = _get_path(call, lane_keys[call['fake']])
            if value is not None:
                lane = value if isinstance(value, str) else _rank_key(value)
            if UUID_RE.search(lane):
                # UUIDs are numbered by first appearance, which needs the lanes ordered first.
                msg = f'lane {lane!r} contains a UUID; choose a lane key without one'
                raise ValueError(msg)
        laned.append({**call, 'lane': lane})
    if mode == 'lanes':
        laned.sort(key=lambda call: call['lane'])  # stable: keeps the order inside a lane
    counters: dict[str, int] = {}
    for call in laned:
        counters[call['lane']] = counters.get(call['lane'], 0) + 1
        call['seq'] = counters[call['lane']]
    return laned


def render(  # noqa: PLR0913 - one keyword per part of the transcript
    *,
    scenario: str,
    spec: dict[str, Any],
    outcome: dict[str, Any],
    calls: list[dict[str, Any]],
    roots: dict[str, Path],
    rows: dict[str, Any],
    fakes: dict[str, Any],
) -> str:
    ordering = spec.get('ordering', {})
    document: dict[str, Any] = {
        'format': FORMAT,
        'scenario': scenario,
        'operation': spec['operation'],
        'outcome': outcome,
        'calls': calls,
        'tree': snapshot_tree(roots),
        'rows': rows,
        'fakes': fakes,
    }
    # The steps run in this order on both sides: root paths, call lanes, UUIDs, volatile values.
    document = _replace_roots(document, roots)
    document['calls'] = _order_calls(document['calls'], ordering.get('mode', 'total'), ordering.get('lane_keys', {}))
    document = _replace_uuids(document)
    _replace_volatile(document, list(spec.get('volatile', [])))
    return json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
