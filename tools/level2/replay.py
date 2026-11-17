"""Level 2 decision replay: the ID and routing decisions of both builds over the names an export holds.

The decisions that file a video are pure: the AVID a title or file name reads as (`AvidParser.get_avid`
with the stored `avid` rules), its brand (`get_brand`) and the folder the archive files it under
(`ArchivePipeline.find_dst_dir` with the stored `archive` config, for every normal route's
destination). This tool collects every name an export (`mask.py export`) holds, makes those
decisions with the Python baseline in this process and with the Rust crates through the
`embyx-monitor` example `replay`, and compares them one by one, warnings included.

    cargo build -q -p embyx-monitor --example replay
    uv run python tools/level2/replay.py DIR --binary target/debug/examples/replay --out replay.json

The names: acquisition AVIDs and the file names of their archived and replace paths and task
directories, the `dn` of every stored magnet, playlist entries (AVID and title), merge task names
and part file names, fill-actor candidate file names. The report holds counts and the differing
names with both decisions: values from a masked export only.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src'))

from embyx_manager.config.models import SECTION_MODELS  # noqa: E402
from embyx_manager.core.avid import AvidConfig, AvidParser, get_brand  # noqa: E402
from embyx_manager.monitor.archive import ArchivePipeline  # noqa: E402
from embyx_manager.monitor.reports import RunContext  # noqa: E402


def _rows(directory: Path, table: str) -> Iterator[dict[str, Any]]:
    path = directory / f'{table}.jsonl'
    if path.exists():
        for line in path.read_text(encoding='utf-8').splitlines():
            yield json.loads(line)


def _json_list(text: str | None) -> list[Any]:
    if not text:
        return []
    with_value = json.loads(text)
    return with_value if isinstance(with_value, list) else []


def _name(path: Any) -> str | None:
    return PurePosixPath(path).name if isinstance(path, str) and path else None


def sections(directory: Path) -> dict[str, str]:
    """The stored `avid` and `archive` sections as text; the defaults' dump for a missing one."""
    stored = {row['section']: row['value_json'] for row in _rows(directory, 'app_config')}
    return {name: stored.get(name) or SECTION_MODELS[name]().model_dump_json() for name in ('avid', 'archive')}


def names(directory: Path) -> list[str]:
    """Every name the export holds that a pipeline reads an AVID from, once each, in order."""
    found: list[str | None] = []
    for row in _rows(directory, 'archive_acquisitions'):
        found += [row['avid'], _name(row.get('task_dir_path'))]
        for column in ('archived_paths_json', 'replace_paths_json'):
            found += [_name(path) for path in _json_list(row.get(column))]
    for row in _rows(directory, 'archive_magnet_attempts'):
        found += parse_qs(urlsplit(row.get('magnet') or '').query).get('dn', [])
    for row in _rows(directory, 'playlists'):
        for entry in _json_list(row.get('entries_json')):
            if isinstance(entry, list) and len(entry) == 3:  # noqa: PLR2004 - [rank, avid, title]
                found += [str(entry[1]), str(entry[2])]
    for row in _rows(directory, 'merge_tasks'):
        found += [row.get('avid'), row.get('merged_name'), _name(row.get('library_dir'))]
        found += [_name(part) for part in _json_list(row.get('parts_json'))]
    for row in _rows(directory, 'fill_actor_candidates'):
        found += [_name(row.get('source_path')), _name(row.get('cloud_source_path'))]
    return list(dict.fromkeys(name for name in found if name))


def python_decisions(texts: dict[str, str], inputs: list[str]) -> list[dict[str, Any]]:
    rules = SECTION_MODELS['avid'].model_validate_json(texts['avid'])
    archive = SECTION_MODELS['archive'].model_validate_json(texts['archive'])
    parser = AvidParser(
        AvidConfig(id_exceptions=tuple(rules.id_exceptions), ignored_id_patterns=tuple(rules.ignored_id_patterns))
    )
    pipeline = ArchivePipeline(config=archive, avid_parser=parser)
    destinations = list(dict.fromkeys(archive.mapping.values()))
    out = []
    for text in inputs:
        avid = parser.get_avid(text)
        ctx = RunContext(logger=logging.getLogger('archive'))
        routes = [pipeline.find_dst_dir(avid, pipeline.dst_dir / dst, ctx) for dst in destinations] if avid else []
        out.append({
            'input': text,
            'avid': avid,
            'brand': get_brand(avid) if avid else None,
            'routes': [None if route is None else str(route) for route in routes],
            'log': list(ctx.log_tail),
        })  # fmt: skip
    return out


def rust_decisions(binary: str, texts: dict[str, str], inputs: list[str]) -> list[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix='embyx-replay-') as work:
        request = Path(work) / 'request.json'
        request.write_text(json.dumps({**texts, 'inputs': inputs}, ensure_ascii=False), encoding='utf-8')
        done = subprocess.run([binary, str(request)], capture_output=True, check=True, timeout=600)
    return [json.loads(line) for line in done.stdout.decode('utf-8').splitlines()]


def compare(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> dict[str, Any]:
    report: dict[str, Any] = {'inputs': len(left), 'equal': 0, 'different': []}
    if len(left) != len(right):
        report['different'].append({'input': None, 'python': f'{len(left)} answers', 'rust': f'{len(right)} answers'})
        return report
    for python, rust in zip(left, right, strict=True):
        if python == rust:
            report['equal'] += 1
        else:
            report['different'].append({'input': python['input'], 'python': python, 'rust': rust})
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('export', type=Path, help='a directory written by mask.py export')
    parser.add_argument('--binary', required=True, help='the embyx-monitor `replay` example')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    texts = sections(args.export)
    inputs = names(args.export)
    report = compare(python_decisions(texts, inputs), rust_decisions(args.binary, texts, inputs))
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'{report["equal"]} of {report["inputs"]} names decided alike; {len(report["different"])} different')
    for item in report['different'][:20]:
        print(' ', json.dumps(item['input'], ensure_ascii=False))
    return 1 if report['different'] else 0


if __name__ == '__main__':
    logging.disable(logging.CRITICAL)
    sys.exit(main())
