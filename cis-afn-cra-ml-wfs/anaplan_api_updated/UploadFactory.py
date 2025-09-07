import os
import sys
from typing import Union

from .AnaplanConnection import AnaplanConnection
from .FileUpload import FileUpload
from .StreamUpload import StreamUpload


class UploadFactory:
    _is_file: bool
    _data: str
    _is_strtoupload: bool

    def __init__(self, data: str, IsStrToUpload: bool):
        """
        :param data: Filepath or data to upload
        """
        self._data = data
        self._is_strtoupload = IsStrToUpload
        self._is_file = os.path.isfile(data)

    def get_uploader(
        self,
        conn: AnaplanConnection,
        file_id: str,
        IsStrToUpload: bool,
        IsReturnLog=False,
    ) -> Union[FileUpload, StreamUpload]:
        """If initialized data source is a filepath, return initialized object for file upload otherwise return
           object for stream upload.

        :param conn: AnaplanConnection object containing Workspace and Model ID, and AuthToken object
        :type conn: AnaplanConnection
        :param file_id: ID of the file to upload
        :type file_id: str
        :return: Initialized upload object
        :rtype: Union[FileUpload, StreamUpload]
        """
        if self._is_file:
            return FileUpload(conn, file_id, IsReturnLog=IsReturnLog)
        elif IsStrToUpload == True:
            return StreamUpload(conn, file_id, IsReturnLog=IsReturnLog)
        else:
            return FileUpload(
                conn,
                file_id,
                "Err01get_uploader: File '" + self._data + "' does not exist!",
                True,
            )
