"""CloudDrive messages in protobuf's JSON mapping, as the scenario fake records them (WP0, WP3).

The fake CloudDrive (`tools/scenarios/fakes/clouddrive.py`) records every request and
response with `MessageToDict(preserving_proto_field_name=True,
always_print_fields_with_no_presence=True)`. The Rust fake re-decodes the same bytes with
the baseline's descriptor and serializes them with prost-reflect; these cases pin that both
give the same JSON: defaults printed, unset `optional` fields left out, enums by name,
64-bit integers as strings, maps with string keys, timestamps in RFC 3339.

Inputs: messages of the RPCs the client uses, built with the baseline's `clouddrive_pb2`.
Outputs: the message's full name, its serialized bytes (hex, deterministic) and the JSON.
"""

from __future__ import annotations

from typing import Any

from google.protobuf import empty_pb2, timestamp_pb2
from google.protobuf.json_format import MessageToDict

from embyx_manager.clients.clouddrive import clouddrive_pb2 as pb


def _messages() -> list[Any]:
    created = timestamp_pb2.Timestamp(seconds=1767225600, nanos=500_000_000)
    return [
        empty_pb2.Empty(),
        pb.FileOperationResult(),
        pb.FileOperationResult(success=True, resultFilePaths=['/115/out/a.txt']),
        pb.MoveFileRequest(theFilePaths=['/115/in/a.txt'], destPath='/115/out', conflictPolicy=pb.MoveFileRequest.Skip),
        pb.MoveFileRequest(theFilePaths=['/a'], destPath='/b', conflictPolicy=pb.MoveFileRequest.Overwrite),
        pb.MoveFileRequest(theFilePaths=[], destPath='/b'),
        pb.ListSubFileRequest(path='/115', forceRefresh=True),
        pb.CloudDriveSystemInfo(IsLogin=True, UserName='fake', SystemReady=True),
        pb.CloudDriveFile(
            id='/115/in/a.txt',
            name='a.txt',
            fullPathName='/115/in/a.txt',
            size=115_000_000_000,
            createTime=created,
            fileHashes={2: '0123456789abcdef0123456789abcdef01234567', 1: 'ABCDEF'},
        ),
        pb.SubFilesReply(subFiles=[pb.CloudDriveFile(id='/x', name='x', fullPathName='/x', isDirectory=True)]),
        pb.AddOfflineFileRequest(urls='magnet:?xt=urn:btih:aaa', toFolder='/115/embyx_in', checkFolderAfterSecs=10),
        pb.CopyTaskRequest(sourcePath='/a', destPath='/b'),
    ]


def _cases() -> list[dict[str, Any]]:
    return [
        {
            'message': message.DESCRIPTOR.full_name,
            'bytes': message.SerializeToString(deterministic=True).hex(),
            'json': MessageToDict(message, preserving_proto_field_name=True, always_print_fields_with_no_presence=True),
        }
        for message in _messages()
    ]


GOLDENS = {'clouddrive/json_mapping.json': _cases}
