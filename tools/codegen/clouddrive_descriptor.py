"""Extract CloudDrive's gRPC descriptor from the baseline's generated stubs.

The Python tree has no `.proto` file, only `clouddrive_pb2.py`, which embeds the serialized
`FileDescriptorProto` of `clouddrive.proto`. The Rust client (`crates/embyx-clients`) is
generated from it at build time with tonic-prost-build (`build.rs`), so no `protoc` is needed.

    uv run python tools/codegen/clouddrive_descriptor.py          # rewrite the .bin file
    uv run python tools/codegen/clouddrive_descriptor.py --check  # what CI runs

The output is a `FileDescriptorSet` holding `clouddrive.proto` and the protobuf files it
imports (`timestamp`, `empty`, `descriptor`): prost-build looks their messages up while it
generates, and maps them to `prost-types` instead of generating them again.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT = REPO_ROOT / 'crates' / 'embyx-clients' / 'proto' / 'clouddrive.fds.bin'


def descriptor_set() -> bytes:
    from google.protobuf import descriptor_pb2  # noqa: PLC0415 - after the path check

    from embyx_manager.clients.clouddrive import clouddrive_pb2  # noqa: PLC0415

    module = Path(clouddrive_pb2.__file__).resolve()
    expected = REPO_ROOT / 'src' / 'embyx_manager' / 'clients' / 'clouddrive' / 'clouddrive_pb2.py'
    if module != expected:
        msg = f'clouddrive_pb2 resolves to {module}, not the baseline tree ({expected})'
        raise SystemExit(msg)
    descriptor_set = descriptor_pb2.FileDescriptorSet()
    # Dependencies first, then the bytes protoc embedded for clouddrive.proto itself.
    for dependency in clouddrive_pb2.DESCRIPTOR.dependencies:
        descriptor_set.file.append(descriptor_pb2.FileDescriptorProto.FromString(dependency.serialized_pb))
    descriptor_set.file.append(descriptor_pb2.FileDescriptorProto.FromString(clouddrive_pb2.DESCRIPTOR.serialized_pb))
    return descriptor_set.SerializeToString(deterministic=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    parser.add_argument('--check', action='store_true', help='fail if the committed file differs')
    args = parser.parse_args()
    data = descriptor_set()
    relative = OUTPUT.relative_to(REPO_ROOT)
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_bytes() != data:
            print(f'{relative} is stale; run tools/codegen/clouddrive_descriptor.py')
            return 1
        print(f'{relative} matches the baseline stubs ({len(data)} bytes)')
        return 0
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_bytes(data)
    print(f'wrote {relative} ({len(data)} bytes)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
