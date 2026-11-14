"""A fake CloudDrive gRPC server that keeps a small state and records every call.

It serves the 14 RPCs `clients/clouddrive/client.py` uses, over a real gRPC socket, so a
scenario exercises the baseline's own client stack (`CloudDriveClient`, `AsyncCloudDrive`)
end to end. The semantics below are this fake's, not CloudDrive's: both builds run against
the same rules, which is what makes their transcripts comparable. WP3 ports this file to
Rust rule for rule (docs/rewrite/SCENARIOS.md, "Fakes").

Calls are recorded as `{'fake': 'clouddrive', 'method': <RPC>, 'request': ..., 'response':
...}` (or `'error': {'code', 'details'}`) with protobuf's JSON mapping: proto field names,
fields at their default value included, enums by name, 64-bit integers as strings.
"""

from __future__ import annotations

import posixpath
import threading
from concurrent import futures
from typing import Any, Self

import grpc
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict

from embyx_manager.clients.clouddrive import clouddrive_pb2 as pb
from embyx_manager.clients.clouddrive import clouddrive_pb2_grpc as pb_grpc

USED_RPCS = (
    'GetSystemInfo',
    'GetSubFiles',
    'CreateFolder',
    'RenameFile',
    'MoveFile',
    'CopyFile',
    'GetCopyTasks',
    'GetUploadFileList',
    'RestartCopyTask',
    'CancelCopyTask',
    'DeleteFiles',
    'AddOfflineFiles',
    'ListOfflineFilesByPath',
    'RemoveOfflineFiles',
)


def to_json(message: Any) -> dict[str, Any]:
    return MessageToDict(message, preserving_proto_field_name=True, always_print_fields_with_no_presence=True)


class FakeCloudDrive(pb_grpc.CloudDriveFileSrvServicer):
    """State: directories, files (size and hashes), offline tasks, copy tasks, the upload queue."""

    def __init__(self, seed: dict[str, Any], record: Any) -> None:
        self._lock = threading.Lock()
        self._record = record
        self.directories: set[str] = {'/', *seed.get('directories', [])}
        # A seeded `id` (default: the path) and `write_time` (nanoseconds since the epoch, default:
        # none) stay with the file when it moves, as CloudDrive's do; both are dumped when seeded.
        self.files: dict[str, dict[str, Any]] = {
            entry['path']: {
                'size': int(entry.get('size', 0)),
                'hashes': dict(entry.get('hashes', {})),
                **{key: entry[key] for key in ('id', 'write_time') if key in entry},
            }
            for entry in seed.get('files', [])
        }
        for path in list(self.files):
            self._add_parents(path)
        self.offline_tasks: list[dict[str, Any]] = [dict(task) for task in seed.get('offline_tasks', [])]
        self.copy_tasks: list[dict[str, Any]] = [dict(task) for task in seed.get('copy_tasks', [])]
        self.uploads: list[dict[str, Any]] = [dict(upload) for upload in seed.get('uploads', [])]
        # Scripted failures: the next `times` calls of `method` fail with `code` and `details`.
        self.errors: list[dict[str, Any]] = [dict(error) for error in seed.get('errors', [])]

    # ---------- state helpers ----------

    def _add_parents(self, path: str) -> None:
        parent = posixpath.dirname(path)
        while parent not in self.directories:
            self.directories.add(parent)
            parent = posixpath.dirname(parent)

    def _entry(self, path: str) -> pb.CloudDriveFile:
        name = posixpath.basename(path)
        if path in self.directories:
            return pb.CloudDriveFile(id=path, name=name, fullPathName=path, isDirectory=True)
        file = self.files[path]
        # TOML keys are strings; `fileHashes` is map<uint32, string>.
        hashes = {int(kind): value for kind, value in file['hashes'].items()}
        entry = pb.CloudDriveFile(
            id=file.get('id', path), name=name, fullPathName=path, size=file['size'], fileHashes=hashes
        )
        if 'write_time' in file:
            seconds, nanos = divmod(int(file['write_time']), 1_000_000_000)
            entry.writeTime.seconds = seconds
            entry.writeTime.nanos = nanos
        return entry

    def _exists(self, path: str) -> bool:
        return path in self.directories or path in self.files

    def _children(self, directory: str) -> list[str]:
        paths = [
            path for path in (*self.directories, *self.files) if path != '/' and posixpath.dirname(path) == directory
        ]
        return sorted(paths)

    def _remove(self, path: str) -> None:
        prefix = path.rstrip('/') + '/'
        self.files = {key: value for key, value in self.files.items() if key != path and not key.startswith(prefix)}
        self.directories = {key for key in self.directories if key != path and not key.startswith(prefix)}

    def _relocate(self, source: str, target: str) -> None:
        prefix = source + '/'
        self.files = {
            (target + key[len(source) :] if key == source or key.startswith(prefix) else key): value
            for key, value in self.files.items()
        }
        self.directories = {
            target + key[len(source) :] if key == source or key.startswith(prefix) else key for key in self.directories
        }

    def edit(self, op: str, args: dict[str, Any]) -> None:
        """Change the state without a call, as CloudDrive itself does (WP14).

        `add_file` (`path`, `size`, `hashes`), `add_directory` and `remove` (`path`),
        `update_copy_tasks` (the `fields` of every task copying into `dest_path`, e.g. a
        finished copy), `drop_copy_tasks` (those tasks leave the list) and `set_uploads` (the
        upload queue becomes `uploads`) and `fail` (a scripted failure as the seed's `errors`
        hold them: the next `times` calls of `method` fail with `code` and `details`).
        """
        with self._lock:
            match op:
                case 'add_file':
                    self.files[args['path']] = {
                        'size': int(args.get('size', 0)),
                        'hashes': dict(args.get('hashes', {})),
                    }
                    self._add_parents(args['path'])
                case 'add_directory':
                    self.directories.add(args['path'])
                    self._add_parents(args['path'])
                case 'remove':
                    self._remove(args['path'])
                case 'update_copy_tasks':
                    for task in self.copy_tasks:
                        if task['dest_path'] == args['dest_path']:
                            task.update(args['fields'])
                case 'drop_copy_tasks':
                    self.copy_tasks = [task for task in self.copy_tasks if task['dest_path'] != args['dest_path']]
                case 'set_uploads':
                    self.uploads = [dict(upload) for upload in args['uploads']]
                case 'fail':
                    self.errors.append(dict(args))
                case _:
                    msg = f'unknown CloudDrive edit {op!r}'
                    raise ValueError(msg)

    def dump(self) -> dict[str, Any]:
        return {
            'directories': sorted(self.directories),
            'files': [{'path': path, **self.files[path]} for path in sorted(self.files)],
            'offline_tasks': self.offline_tasks,
            'copy_tasks': self.copy_tasks,
            'uploads': self.uploads,
        }

    # ---------- recording ----------

    def _call(self, method: str, request: Any, context: grpc.ServicerContext, handle: Any) -> Any:
        with self._lock:
            record: dict[str, Any] = {'fake': 'clouddrive', 'method': method, 'request': to_json(request)}
            scripted = next(
                (error for error in self.errors if error['method'] == method and error.get('times', 1) > 0), None
            )
            if scripted is not None:
                scripted['times'] = scripted.get('times', 1) - 1
                record['error'] = {'code': scripted['code'], 'details': scripted.get('details', '')}
                self._record(record)
                context.abort(getattr(grpc.StatusCode, scripted['code']), scripted.get('details', ''))
            try:
                response = handle(request)
            except _Abort as abort:
                record['error'] = {'code': abort.code, 'details': abort.details}
                self._record(record)
                context.abort(getattr(grpc.StatusCode, abort.code), abort.details)
            record['response'] = (
                to_json(response) if not isinstance(response, list) else [to_json(item) for item in response]
            )
            self._record(record)
            return response

    # ---------- the RPCs ----------

    def GetSystemInfo(self, request: empty_pb2.Empty, context: grpc.ServicerContext) -> Any:  # noqa: N802
        return self._call(
            'GetSystemInfo',
            request,
            context,
            lambda _: pb.CloudDriveSystemInfo(IsLogin=True, UserName='fake', SystemReady=True),
        )

    def GetSubFiles(self, request: pb.ListSubFileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.ListSubFileRequest) -> list[pb.SubFilesReply]:
            if request.path in self.files:
                details = f"can't open a file as directory: {request.path}"
                raise _Abort(code='INVALID_ARGUMENT', details=details)
            if request.path not in self.directories:
                details = f'path not found: {request.path}'
                raise _Abort(code='NOT_FOUND', details=details)
            return [pb.SubFilesReply(subFiles=[self._entry(path) for path in self._children(request.path)])]

        yield from self._call('GetSubFiles', request, context, handle)

    def CreateFolder(self, request: pb.CreateFolderRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.CreateFolderRequest) -> pb.CreateFolderResult:
            if request.parentPath not in self.directories:
                details = f'path not found: {request.parentPath}'
                raise _Abort(code='NOT_FOUND', details=details)
            path = posixpath.join(request.parentPath, request.folderName)
            if self._exists(path):
                return pb.CreateFolderResult(result=pb.FileOperationResult(success=False, errorMessage='file exists'))
            self.directories.add(path)
            return pb.CreateFolderResult(
                folderCreated=self._entry(path), result=pb.FileOperationResult(success=True, resultFilePaths=[path])
            )

        return self._call('CreateFolder', request, context, handle)

    def RenameFile(self, request: pb.RenameFileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.RenameFileRequest) -> pb.FileOperationResult:
            source = request.theFilePath
            target = posixpath.join(posixpath.dirname(source), request.newName)
            if not self._exists(source):
                return pb.FileOperationResult(success=False, errorMessage=f'file not found: {source}')
            if self._exists(target):
                return pb.FileOperationResult(success=False, errorMessage=f'file exists: {target}')
            self._relocate(source, target)
            return pb.FileOperationResult(success=True, resultFilePaths=[target])

        return self._call('RenameFile', request, context, handle)

    def MoveFile(self, request: pb.MoveFileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.MoveFileRequest) -> pb.FileOperationResult:
            if request.destPath not in self.directories:
                return pb.FileOperationResult(success=False, errorMessage=f'destination not found: {request.destPath}')
            moved = []
            for source in request.theFilePaths:
                if not self._exists(source):
                    return pb.FileOperationResult(
                        success=False, errorMessage=f'file not found: {source}', resultFilePaths=moved
                    )
                target = posixpath.join(request.destPath, posixpath.basename(source))
                if self._exists(target):
                    if request.conflictPolicy == pb.MoveFileRequest.Skip:
                        continue
                    if request.conflictPolicy != pb.MoveFileRequest.Overwrite:
                        details = 'the fake only overwrites or skips on a conflict'
                        raise _Abort(code='UNIMPLEMENTED', details=details)
                    self._remove(target)
                self._relocate(source, target)
                moved.append(target)
            return pb.FileOperationResult(success=True, resultFilePaths=moved)

        return self._call('MoveFile', request, context, handle)

    def CopyFile(self, request: pb.CopyFileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.CopyFileRequest) -> pb.FileOperationResult:
            if request.destPath not in self.directories:
                return pb.FileOperationResult(success=False, errorMessage=f'destination not found: {request.destPath}')
            for source in request.theFilePaths:
                # Queued only: a scenario finishes a copy through the seed of a later step.
                self.copy_tasks.append(
                    {
                        'source_path': source,
                        'dest_path': request.destPath,
                        'status': 'Pending',
                        'total_bytes': 0,
                        'uploaded_bytes': 0,
                        'failed_files': 0,
                        'paused': False,
                        'errors': [],
                    }
                )
            return pb.FileOperationResult(success=True)

        return self._call('CopyFile', request, context, handle)

    def GetCopyTasks(self, request: empty_pb2.Empty, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(_: empty_pb2.Empty) -> pb.GetCopyTaskResult:
            return pb.GetCopyTaskResult(
                copyTasks=[
                    pb.CopyTask(
                        sourcePath=task['source_path'],
                        destPath=task['dest_path'],
                        status=pb.CopyTask.TaskStatus.Value(task['status']),
                        totalBytes=task['total_bytes'],
                        uploadedBytes=task['uploaded_bytes'],
                        failedFiles=task['failed_files'],
                        paused=task['paused'],
                        errors=[pb.TaskError(message=message) for message in task['errors']],
                    )
                    for task in self.copy_tasks
                ]
            )

        return self._call('GetCopyTasks', request, context, handle)

    def GetUploadFileList(self, request: pb.GetUploadFileListRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.GetUploadFileListRequest) -> pb.GetUploadFileListResult:
            matching = [upload for upload in self.uploads if request.filter in upload['dest_path']]
            return pb.GetUploadFileListResult(
                totalCount=len(self.uploads),
                totalCountFiltered=len(matching),
                uploadFiles=[
                    pb.UploadFileInfo(
                        destPath=upload['dest_path'],
                        size=upload.get('size', 0),
                        transferedBytes=upload.get('transferred', 0),
                        status=upload.get('status', 'Transfer'),
                        errorMessage=upload.get('error', ''),
                    )
                    for upload in matching
                ],
            )

        return self._call('GetUploadFileList', request, context, handle)

    def RestartCopyTask(self, request: pb.CopyTaskRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.CopyTaskRequest) -> empty_pb2.Empty:
            for task in self.copy_tasks:
                if task['source_path'] == request.sourcePath and task['dest_path'] == request.destPath:
                    task['status'] = 'Pending'
                    task['failed_files'] = 0
                    task['errors'] = []
            return empty_pb2.Empty()

        return self._call('RestartCopyTask', request, context, handle)

    def CancelCopyTask(self, request: pb.CopyTaskRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.CopyTaskRequest) -> empty_pb2.Empty:
            self.copy_tasks = [
                task
                for task in self.copy_tasks
                if not (task['source_path'] == request.sourcePath and task['dest_path'] == request.destPath)
            ]
            return empty_pb2.Empty()

        return self._call('CancelCopyTask', request, context, handle)

    def DeleteFiles(self, request: pb.MultiFileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.MultiFileRequest) -> pb.FileOperationResult:
            missing = [path for path in request.path if not self._exists(path)]
            if missing:
                return pb.FileOperationResult(success=False, errorMessage=f'file not found: {missing[0]}')
            for path in request.path:
                self._remove(path)
            return pb.FileOperationResult(success=True)

        return self._call('DeleteFiles', request, context, handle)

    def AddOfflineFiles(self, request: pb.AddOfflineFileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.AddOfflineFileRequest) -> pb.FileOperationResult:
            if request.toFolder not in self.directories:
                return pb.FileOperationResult(success=False, errorMessage=f'destination not found: {request.toFolder}')
            for url in request.urls.split('\n'):
                info_hash = _btih(url)
                if any(task['info_hash'] == info_hash for task in self.offline_tasks):
                    return pb.FileOperationResult(success=False, errorMessage='任务已存在')
                self.offline_tasks.append(
                    {
                        'name': _display_name(url) or info_hash,
                        'url': url,
                        'folder': request.toFolder,
                        'info_hash': info_hash,
                        'status': 'OFFLINE_INIT',
                        'size': 0,
                        'progress': 0.0,
                    }
                )
            return pb.FileOperationResult(success=True)

        return self._call('AddOfflineFiles', request, context, handle)

    def ListOfflineFilesByPath(self, request: pb.FileRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.FileRequest) -> pb.OfflineFileListResult:
            return pb.OfflineFileListResult(
                offlineFiles=[
                    pb.OfflineFile(
                        name=task['name'],
                        size=task['size'],
                        url=task['url'],
                        status=pb.OfflineFileStatus.Value(task['status']),
                        infoHash=task['info_hash'],
                        percendDone=task['progress'],
                    )
                    for task in self.offline_tasks
                    if task['folder'] == request.path
                ]
            )

        return self._call('ListOfflineFilesByPath', request, context, handle)

    def RemoveOfflineFiles(self, request: pb.RemoveOfflineFilesRequest, context: grpc.ServicerContext) -> Any:  # noqa: N802
        def handle(request: pb.RemoveOfflineFilesRequest) -> pb.FileOperationResult:
            # Matched case-sensitively, as CloudDrive does; an unknown hash still answers success.
            removed = [task for task in self.offline_tasks if task['info_hash'] in request.infoHashes]
            self.offline_tasks = [task for task in self.offline_tasks if task not in removed]
            if request.deleteFiles:
                for task in removed:
                    self._remove(posixpath.join(task['folder'], task['name']))
            return pb.FileOperationResult(success=True)

        return self._call('RemoveOfflineFiles', request, context, handle)


class _Abort(Exception):  # noqa: N818 - control flow inside the fake, not an error of the code under test
    def __init__(self, *, code: str, details: str) -> None:
        super().__init__(details)
        self.code = code
        self.details = details


def _btih(url: str) -> str:
    marker = 'urn:btih:'
    start = url.find(marker)
    if start < 0:
        return url
    return url[start + len(marker) :].split('&', 1)[0].lower()


def _display_name(url: str) -> str:
    from urllib.parse import parse_qs, urlsplit  # noqa: PLC0415

    return parse_qs(urlsplit(url).query).get('dn', [''])[0]


class FakeCloudDriveServer:
    """The fake on a local port; `address` is what `CloudDriveClient(address=..., secure=False)` takes."""

    def __init__(self, seed: dict[str, Any], record: Any) -> None:
        self.fake = FakeCloudDrive(seed, record)
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb_grpc.add_CloudDriveFileSrvServicer_to_server(self.fake, self._server)
        port = self._server.add_insecure_port('127.0.0.1:0')
        self.address = f'127.0.0.1:{port}'

    def __enter__(self) -> Self:
        self._server.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.stop(grace=None)
