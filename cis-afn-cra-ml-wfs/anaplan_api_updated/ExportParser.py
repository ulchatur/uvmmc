# ===========================================================================
# This function reads the JSON results of the completed Anaplan task and returns
# the job details.
# ===========================================================================
import logging
from distutils.util import strtobool
from typing import List

import pandas as pd

from anaplan_api_updated import anaplan

from .AnaplanConnection import AnaplanConnection
from .Parser import Parser
from .ParserResponse import ParserResponse

logger = logging.getLogger(__name__)


class ExportParser(Parser):
    results: List[ParserResponse]

    def __init__(self, conn: AnaplanConnection, results: dict, url: str):
        super().__init__(conn=conn, results=results, url=url)
        ExportParser.results = []
        ExportParser.results.append(ExportParser.parse_response(conn, results, url))

    @staticmethod
    def get_results() -> List[ParserResponse]:
        """Get the list of task result details

        :return: Exported file as str
        :rtype: List[ParserResponse]
        """
        # return ExportParser.results
        if ExportParser.results[0].get_task_detail() == "File export completed.":
            return ExportParser.results[0].get_export_file()
        else:
            return (
                "Err01ExportParserget_results: Export was unable to be completed. Further details: "
                + ExportParser.results[0].get_task_detail()
            )

    @staticmethod
    def parse_response(conn, results, url) -> ParserResponse:
        """Parse the JSON response for a task into an object with standardized format.

        :param conn: AnaplanConnection object with authentication, workspace and model IDs
        :type conn: AnaplanConnection
        :param results: JSON dict with task results.
        :type results: dict
        :param url: URL of Anaplan task
        :type url: str
        :return: Array with overall task result as string, file contents in string,
                        boolean if error dump is available, and dataframe with error dump.
        :rtype: ParserResponse
        """

        job_status = results["currentStep"]
        failure_dump = bool(strtobool(str(results["result"]["failureDumpAvailable"]).lower()))
        edf = pd.DataFrame()

        if job_status == "Failed.":
            """Should Never happen for Export type tasks"""
            return Parser.failure_message(results)
        else:
            # IF failure dump is available download
            if failure_dump:
                edf = Parser.get_dump("".join([url, "/dump"]))

            success_report = str(results["result"]["successful"])

            # details key only present in import task results
            if "objectId" in results["result"]:
                object_id = results["result"]["objectId"]
                file_contents = anaplan.get_file(conn, object_id)
                logger.info(f"The requested job is {job_status}")
                logger.info(f"Failure Dump Available: {failure_dump}, Successful: {success_report}")
                return ParserResponse("File export completed.", file_contents, False, edf)
