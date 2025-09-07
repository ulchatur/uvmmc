import logging
import math
import os
import re
import sys
from functools import partial

from .Upload import Upload

logger = logging.getLogger(__name__)


def Return_IsRegexInStr(TxtToAnalyze, TxtRegexPattern) -> bool:
    return bool(re.search(TxtRegexPattern, TxtToAnalyze))


class FileUpload(Upload):
    def upload(self, chunk_size: int, file: str, TxtPreviousLog="", IsReturnLog=False):
        """Upload a local file to Anaplan model

        :param chunk_size: Desired size of the chunk, in megabytes
        :type chunk_size: int
        :param file: Path to the local file to be uploaded to Anaplan
        :type file: str
        """
        if (
            os.path.isfile(file) == False and IsReturnLog == False
        ):  # 1. if os.path.isfile(file) ==False and IsReturnLog==False
            print("Err01FileUpload: File does not exist!")
            sys.exit()
        elif (
            os.path.isfile(file) == False and IsReturnLog == True
        ):  # 1. if os.path.isfile(file) ==False and IsReturnLog==False
            return "Err01FileUpload: File does not exist!"
        if (
            Return_IsRegexInStr(TxtPreviousLog, r"Err\d{2}") == True and IsReturnLog == True
        ):  # 2. if Return_IsRegexInStr(TxtPreviousLog, r"Err\d{2}") == True and IsReturnLog==True
            return TxtPreviousLog
        elif (
            Return_IsRegexInStr(TxtPreviousLog, r"Err\d{2}") == True and IsReturnLog == False
        ):  # 2. if Return_IsRegexInStr(TxtPreviousLog, r"Err\d{2}") == True and IsReturnLog==True
            logger.debug(TxtPreviousLog)
            print(TxtPreviousLog)
            sys.exit()
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

        metadata_update = super().file_metadata(url)
        # Confirm that the metadata update for the requested file was OK before proceeding with file upload
        if metadata_update:
            logger.info(f"Starting upload of file {super().get_file_id()}.")

            # with open(file, 'rt') as file:
            # 	# Enumerate the file contents in specified chunk size
            # 	for chunk_num, data in enumerate(iter(partial(file.read, chunk_size * (1024 ** 2)), '')):
            # 		print(url + "/chunks/" + str(chunk_num))
            # 		complete = super().file_data(''.join([url, "/chunks/", str(chunk_num)]), chunk_num,
            # 		                             data.encode('utf-8'))

            try:
                NumMemorySizeFile = os.path.getsize(file)
            except Exception as TxtException:
                if IsReturnLog == True:  # 3. if IsReturnLog==True
                    return "Err01FileUpload: Error with file. Further details: " + str(TxtException)
                print("Err01FileUpload: Error with file. Further details: " + str(TxtException))
                sys.exit()
            except SystemExit:
                raise  # Re-raise the SystemExit exception
            TotalChunks = math.ceil(NumMemorySizeFile / (chunk_size * (1024 * 1024)))
            # print("Num Total of chunks to do: " + str(TotalChunks))
            # Enumerate the chunk numbers
            for CounterChunks in range(TotalChunks):
                # Calculate the start and end positions of the chunk in the file
                NumChunkPositionStarts = CounterChunks * chunk_size * (1024 * 1024)
                NumChunkPositionEnd = min((CounterChunks + 1) * chunk_size * (1024 * 1024), NumMemorySizeFile)
                # Read the chunk data from the file
                with open(file, "rb") as f:
                    f.seek(NumChunkPositionStarts)
                    data = f.read(NumChunkPositionEnd - NumChunkPositionStarts)
                # Upload the chunk
                # print(url + "/chunks/" + str(CounterChunks))
                complete = super().file_data("".join([url, "/chunks/", str(CounterChunks)]), CounterChunks, data)
            if complete:
                complete_upload = super().file_metadata("".join([url, "/complete"]))
                if complete_upload and IsReturnLog == False:
                    print(f"Upload of file {super().get_file_id()} complete.")
                    logger.info(f"Upload of file {super().get_file_id()} complete.")
                elif complete_upload and IsReturnLog == True:
                    return "Ok01FileUpload: File uploaded succesfully!"
