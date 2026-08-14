"""Synchronous CloudDrive gRPC client."""

import errno

import grpc
from google.protobuf import empty_pb2

from embyx_manager.clients.clouddrive import clouddrive_pb2, clouddrive_pb2_grpc

GRPC_TIMEOUT_SECONDS = 30.0


class CloudDriveClient:
    def __init__(
        self,
        *,
        address: str,
        api_token: str,
        secure: bool = True,
        cloud_name: str = '',
        cloud_account_id: str = '',
    ) -> None:
        """gRPC client for one CloudDrive server.

        ``cloud_name`` / ``cloud_account_id`` identify the storage account for
        offline-task bookkeeping calls (``clear_finished_offline_files``).
        """
        self._api_token = api_token
        self._cloud_name = cloud_name
        self._cloud_account_id = cloud_account_id
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
        return self.stub.MoveFile(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)

    def add_offline_file(self, urls: str | list[str], dst_dir: str) -> clouddrive_pb2.FileOperationResult:
        if isinstance(urls, str):
            urls = [urls]
        request = clouddrive_pb2.AddOfflineFileRequest(
            urls='\n'.join(urls),
            toFolder=dst_dir,
            checkFolderAfterSecs=0,
        )
        return self.stub.AddOfflineFiles(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)

    def list_offline_files_by_path(self, path: str) -> list[clouddrive_pb2.OfflineFile]:
        """Every offline task under path, whatever its status."""
        request = clouddrive_pb2.FileRequest(path=path)
        result = self.stub.ListOfflineFilesByPath(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)
        return list(result.offlineFiles)

    def list_finished_offline_files_by_path(self, path: str) -> clouddrive_pb2.OfflineFileListResult:
        request = clouddrive_pb2.FileRequest(path=path)
        result = self.stub.ListOfflineFilesByPath(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)
        finished = [f for f in result.offlineFiles if f.status == clouddrive_pb2.OfflineFileStatus.OFFLINE_FINISHED]
        return clouddrive_pb2.OfflineFileListResult(offlineFiles=finished, status=result.status)

    def clear_finished_offline_files(self, path: str) -> None:
        request = clouddrive_pb2.ClearOfflineFileRequest(
            filter=clouddrive_pb2.ClearOfflineFileRequest.Filter.Finished,
            cloudName=self._cloud_name,
            cloudAccountId=self._cloud_account_id,
            deleteFiles=False,
            path=path,
        )
        self.stub.ClearOfflineFiles(request, metadata=self._metadata(), timeout=GRPC_TIMEOUT_SECONDS)
