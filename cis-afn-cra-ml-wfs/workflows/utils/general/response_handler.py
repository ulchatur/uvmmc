import logging
from typing import Union  
import time
from time import gmtime, strftime


def response_handler(
    res_dict: dict, response: Union[object, None], time_start: float, success: bool
):
    """
    Handles responses for API calls depending on success/failure
    """
    res = res_dict.copy()
    if success is True:
        res["success"] = True
        res["response_time"] = response_time(time_start=time_start)
        res["response"] = response
        logging.info("SUCCESS")
        return res
    elif success is False:
        res["success"] = False
        res["response_time"] = response_time(time_start=time_start)
        res["response"] = None
        logging.info("FAILURE")
        return res


def response_time(time_start):
    """
    Calculates response time and timestamp using timestamp float
    """
    return {
        "response_time": "{0:.2f} seconds".format(time.time() - time_start),
        "timestamp": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
    }

