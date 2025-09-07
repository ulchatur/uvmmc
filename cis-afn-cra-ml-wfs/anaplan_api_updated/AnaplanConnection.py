# ===============================================================================
# Created:        13 Sep 2018
# @author:        Jesse Wilson (Anaplan Asia Pte Ltd)
# Description:    Class to contain Anaplan connection details required for all API calls
# Input:          Authorization header string, workspace ID string, and model ID string
# Output:         None
# ===============================================================================
import time
from dataclasses import dataclass

import requests

from .AuthToken import AuthToken


@dataclass()
class AnaplanConnection(object):
    """
    AnaplanConnection object stores AuthToken, workspace and model IDs.
    Model ID must be unique.
    """

    _authorization: AuthToken
    _workspace_id: str
    _model_id: str

    def __init__(self, authorization, workspace_id, model_id):
        self._authorization = authorization
        self._workspace_id = workspace_id
        self._model_id = model_id

    def get_auth(self) -> AuthToken:
        """Fetch the AuthToken object.

        :return: Object with authorization and token expiry time.
        :rtype: AuthToken
        """
        # Following is to fix bug (likely to garbage collector) when having multiple instances for multiple workspaces and modelids, seems like it has to wait until the token is valid to perform actions
        workspace_id = self._workspace_id
        model_id = self._model_id
        Txturl = f"https://api.anaplan.com/2/0/workspaces/{workspace_id}/models/{model_id}/files"
        DictHeaders = {"Authorization": self._authorization.token_value}
        TotalAttempts = 10
        CounterAttempts = 0
        while CounterAttempts < TotalAttempts:  # 1. while CounterAttempts < TotalAttempts
            try:  # 1. try (Trying to get the file list to verify connection)
                response = requests.get(Txturl, headers=DictHeaders, timeout=(5, 30))
                break  # 1. try (Trying to get the file list to verify connection)
            except:  # 1. try (Trying to get the file list to verify connection)
                time.sleep(1)
                CounterAttempts += 1
                pass
        if CounterAttempts == TotalAttempts:  # 1. if CounterAttempts==TotalAttempts
            return None
        return self._authorization

    def get_workspace(self) -> str:
        """Fetch the workspace ID

        :return: A 32-character string hexadecimal workspace ID.
        :rtype: str
        """
        return self._workspace_id

    def get_model(self) -> str:
        """Fetch the model ID

        :return: A 32-character string hexadecimal model ID.
        :rtype: str
        """
        return self._model_id

    def set_auth(self, authorization: AuthToken):
        """Set a new AuthToken to overwrite the object set at creation.

        :param authorization: Object with authorization and token expiry time
        :type authorization: AuthToken
        """
        self._authorization = authorization

    def set_workspace(self, workspace_id: str):
        """Set a new workspace ID

        :param workspace_id: A 32-character string hexadecimal workspace ID.
        :type workspace_id: str
        """
        self._workspace_id = workspace_id

    def set_model(self, model_id: str):
        """Set a new model ID

        :param model_id: A 32-character string hexadecimal model ID.
        :type model_id: str
        """
        self._model_id = model_id
