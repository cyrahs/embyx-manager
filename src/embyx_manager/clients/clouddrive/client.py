"""Synchronous CloudDrive gRPC client."""

import errno

import grpc
from google.protobuf import empty_pb2

from embyx_manager.clients.clouddrive import clouddrive_pb2, clouddrive_pb2_grpc

GRPC_TIMEOUT_SECONDS = 30.0

#: MoveFile gets its own budget: a cloud-side move is executed by the provider and
#: can take far longer than a listing. Cutting it off at the general timeout leaves
#: the move in an unknown state that then has to be observed to a conclusion.
MOVE_TIMEOUT_SECONDS = 300.0

#: How soon CloudDrive re-checks the destination folder after an offline task is
#: added. Zero disables the check, and with a persistent directory cache (115)
#: nothing else ever expires the folder listing, so the finished download would
#: stay invisible to both the API and the mount.
CHECK_FOLDER_AFTER_SECONDS = 10


class CloudDriveClient:
    def __init__(self, *, address: str, api_token: str, secure: bool = True) -> None:
        """gRPC client for one CloudDrive server."""
        self._api_token = api_token
        self.channel = (
            grpc.secure_channel(address, grpc.ssl_channel_credentials()) if secure else grpc.insecure_channel(address)
        )
        self.stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(self.channel)

    def close(self) -> None:
        self.channel.close()

    def _metadata(self) -> list[tuple[str, str]]:
        return [('authorization', f'Bearer {self._api_token}')]

    def get_system_info(self) -> clouddrive_pb2.CloudDriveSystemInfo:
        """Unauthenticated server ping."""
        return self.stub.GetSystemInfo(empty_pb2.Empty(), timeout=GRPC_TIMEOUT_SECONDS)

    def get_sub_files(self, path: str, *, force_refresh: bool = False) -> list[clouddrive_pb2.CloudDriveFile]:
        request = clouddrive_pb2.ListSubFileRequest(path=path, forceRefresh=force_refresh)
        files = []
        try:
            for response in self.stub.GetSubFiles(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS):
                files.extend(response.subFiles)
        except grpc.RpcError as e:
            if getattr(e, 'code', None) and e.code() == grpc.StatusCode.NOT_FOUND:
                details = getattr(e, 'details', lambda: None)()
                msg = details or f'CloudDrive path not found: "{path}"'
                raise FileNotFoundError(errno.ENOENT, msg, path) from e
            if getattr(e, 'code', None) and e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                details = getattr(e, 'details', lambda: None)()
                if details and "can't open a file as directory" in details:
                    raise NotADirectoryError(errno.ENOTDIR, details, path) from e
            raise
        return files

    def create_folder(self, parent_path: str, folder_name: str) -> clouddrive_pb2.CreateFolderResult:
        request = clouddrive_pb2.CreateFolderRequest(parentPath=parent_path, folderName=folder_name)
        return self.stub.CreateFolder(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)

    def rename_file(self, file_path: str, new_name: str) -> clouddrive_pb2.FileOperationResult:
        request = clouddrive_pb2.RenameFileRequest(theFilePath=file_path, newName=new_name)
        return self.stub.RenameFile(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)

    def move_file(
        self,
        source_paths: list[str],
        dest_path: str,
        conflict_policy: int = 0,
    ) -> clouddrive_pb2.FileOperationResult:
        """conflict_policy: 0=overwrite, 1=rename, 2=skip."""
        request = clouddrive_pb2.MoveFileRequest(
            theFilePaths=source_paths,
            destPath=dest_path,
            conflictPolicy=conflict_policy,
        )
        return self.stub.MoveFile(request, metadata=self._metadata(), timeout=MOVE_TIMEOUT_SECONDS)

    def add_offline_file(
        self,
        urls: str | list[str],
        dst_dir: str,
        *,
        check_folder_after_secs: int = CHECK_FOLDER_AFTER_SECONDS,
    ) -> clouddrive_pb2.FileOperationResult:
        if isinstance(urls, str):
            urls = [urls]
        request = clouddrive_pb2.AddOfflineFileRequest(
            urls='\n'.join(urls),
            toFolder=dst_dir,
            checkFolderAfterSecs=check_folder_after_secs,
        )
        return self.stub.AddOfflineFiles(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)

    def list_offline_files_by_path(self, path: str) -> list[clouddrive_pb2.OfflineFile]:
        """Every offline task under path, whatever its status."""
        request = clouddrive_pb2.FileRequest(path=path)
        result = self.stub.ListOfflineFilesByPath(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)
        return list(result.offlineFiles)

    def remove_offline_files(
        self,
        info_hashes: list[str],
        path: str,
        *,
        delete_files: bool,
    ) -> clouddrive_pb2.FileOperationResult:
        """Drop offline tasks by info hash; the path names the cloud they live in."""
        request = clouddrive_pb2.RemoveOfflineFilesRequest(
            infoHashes=info_hashes,
            path=path,
            deleteFiles=delete_files,
        )
        return self.stub.RemoveOfflineFiles(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)
