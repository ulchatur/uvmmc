import os
from anaplan_api_updated.anaplan import generate_authorization
from anaplan_api_updated import AuthToken, AnaplanConnection
from utils.anaplan.config_anaplan_lib import path_bin, path_cert, path_key, path_private_key

def get_auth() -> AuthToken:

    auth = generate_authorization(
        auth_type="Certificate",
        cert=path_cert,
        private_key=path_private_key,
        TxtFilePathBin=path_bin,
        TxtFilePathkey=path_key
    )
    return auth


def get_conn(workspace_id: str, model_id: str) -> AnaplanConnection:
    auth = get_auth()
    conn = AnaplanConnection(
        authorization=auth, workspace_id=workspace_id, model_id=model_id
    )
    return conn