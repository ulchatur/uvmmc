import logging
import re
import sys
from functools import partial
from io import StringIO

from .Upload import Upload

logger = logging.getLogger(__name__)


def Return_IsRegexInStr(TxtToAnalyze, TxtRegexPattern) -> bool:
    return bool(re.search(TxtRegexPattern, TxtToAnalyze))


class StreamUpload(Upload):
    def upload(self, chunk_size, data, TxtPreviousLog="", IsReturnLog=False):
        """Upload data held in memory to Anaplan model

        :param chunk_size: Upload request body size in MB between 1 and 50
        :type chunk_size: int
        :param data: String data to be uploaded to Anaplan
        :type data: str
        """
        if Return_IsRegexInStr(TxtPreviousLog, r"Err\d{2}") == True and IsReturnLog == True:
            return TxtPreviousLog
        elif Return_IsRegexInStr(TxtPreviousLog, r"Err\d{2}") == True and IsReturnLog == False:
            logger.debug(TxtPreviousLog)
            print(TxtPreviousLog)
            sys.exit()
        stream_upload = False
        url = "".join(
            [
                super().get_base_url(),
                super().get_workspace(),
                "/models/",
                super().get_model(),
                "/files/",
                super().get_file_id(),
            ]
        )
        io_data = StringIO(data)  # Convert str to StingIO for enumeration
        metadata_update = super().file_metadata(url)  # Update file metadata to begin upload process

        if metadata_update:
            logger.info(f"Starting upload of file {super().get_file_id()}.")
            # Loop through enumerated data, sending chunks of the specified size to Anaplan until all data is uploaded
            for chunk_num, data in enumerate(iter(partial(io_data.read, chunk_size * (1024**2)), "")):
                stream_upload = super().file_data(
                    "".join([url, "/chunks/", str(chunk_num)]),
                    chunk_num,
                    data.encode("utf-8"),
                )

            # Once all data is uploaded mark the file complete to indicate the file is ready for use
            if stream_upload:
                complete_upload = super().file_metadata("".join([url, "/complete"]))
                if complete_upload and IsReturnLog == False:
                    logger.info(f"Upload of str {super().get_file_id()} complete.")
                elif complete_upload and IsReturnLog == True:
                    return "Ok01StreamUpload: Str uploaded succesfully!"
