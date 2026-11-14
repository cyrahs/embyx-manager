"""List the compiled Rust tests with the crate each one belongs to, for tools/parity/parity.py.

`cargo test -- --list` prints `path::to::name: test` per test binary but never names the crate,
so two crates with a test at the same module path would satisfy each other's `// py:` claims.
This builds the test binaries once (`cargo test <args> --no-run --message-format=json`, the
same compile as `cargo test <args>`), runs each with `--list --format terse` and prefixes every
line with the crate's directory under `crates/` and a tab:

    embyx-core\tavid::tests::get_avid: test

    python3 tools/parity/list_rust_tests.py --workspace --locked > rust-tests.txt
    python3 tools/parity/list_rust_tests.py -p embyx-transcode-worker -p embyx-manager --locked

Standard library only; runs on the Linux and the Windows job.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_binaries(cargo_args: list[str]) -> list[tuple[str, str]]:
    """`(crate directory, executable)` for every test binary the arguments build, in cargo's order."""
    built = subprocess.run(
        ['cargo', 'test', *cargo_args, '--no-run', '--message-format=json'],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    binaries = []
    for line in built.stdout.splitlines():
        if not line.startswith('{'):
            continue
        message = json.loads(line)
        if message.get('reason') != 'compiler-artifact' or not message.get('profile', {}).get('test'):
            continue
        executable = message.get('executable')
        if executable:
            binaries.append((Path(message['manifest_path']).parent.name, executable))
    return binaries


def main() -> int:
    binaries = test_binaries(sys.argv[1:])
    if not binaries:
        print('error: cargo built no test binary', file=sys.stderr)
        return 1
    for crate, executable in binaries:
        listed = subprocess.run(
            [executable, '--list', '--format', 'terse'], check=True, stdout=subprocess.PIPE, text=True
        )
        for line in listed.stdout.splitlines():
            if line.endswith(': test'):
                print(f'{crate}\t{line}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
