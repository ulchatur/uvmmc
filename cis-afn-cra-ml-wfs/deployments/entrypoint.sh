#!/bin/bash -e

#checks to see if .env file exists or not in container
if [ -f /home/app/.env ]; then
  echo ".env file exists in container."
  exec "$@"
fi


if [ "${IS_VAULT_INTEGRATED}" = "false" ]; then
    # Exec replaces the shell so execution does not resume
    exec "$@"
fi

# The vault integrations take a backoff and retry approach to failed vault calls.
# 10 retries will be made, with the backoff time increasing by 5 seconds on each retry.
# Almost 5 minutes can be spent trying to connect to vault. 
DATA_PATH=.data.data
UI_ENV_VAR_CONFIG="ui_env_vars.txt"
# Retrieve the service account's token to be used for requesting a vault auth token.
JWT=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)

echo "Retrieving auth token from vault ..."

retry_count=0
while [ "$STATUS_CODE" != "200" ]
do
    retry_count=$((retry_count+1))
    echo "Attempt number: $retry_count"
    backoff_time=$((5*retry_count))
    echo "Backoff time if request fails: $backoff_time"
    echo "Vault role name: $VAULT_ROLE_NAME"
    echo "Vault url: $VAULT_URL"
    echo "Vault namespace: $VAULT_NAMESPACE"
    # Make the request to vault to get the auth token.
    STATUS_CODE=$(curl --silent --show-error \
    --output response.txt --write-out "%{http_code}" \
    --location --request POST $VAULT_URL \
    --header "X-Vault-Namespace: ${VAULT_NAMESPACE}" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "role": "'"$VAULT_ROLE_NAME"'",
        "jwt": "'"$JWT"'"
    }')
    ## Going to retry 10 times
    if [ $retry_count -eq 10 ]; then
        >&2 echo "Failed to retrieve Vault token: $(cat response.txt)"
        exit 1
    elif [ "$STATUS_CODE" != "200" ]; then
        >&1 echo "Attempt $retry_count: failed to retrieve Vault token: $(cat response.txt) - retrying in $backoff_time seconds"
    fi
    sleep $backoff_time
done

CLIENT_TOKEN=$(<response.txt jq -r '.auth.client_token')

rm response.txt
STATUS_CODE=""

echo "Retrieving secrets from vault using auth token ..."


retry_count=0
while [ "$STATUS_CODE" != "200" ]
do
    retry_count=$((retry_count+1))
    echo "Attempt number: $retry_count"
    backoff_time=$((5*retry_count))
    echo "Backoff time if request fails: $backoff_time"

    # Make the request to vault to get the secrets.
    STATUS_CODE=$(curl --silent --show-error \
    --output response.txt --write-out "%{http_code}" \
    --location --request GET $SECRETS_URL \
    --header "X-Vault-Namespace: ${VAULT_NAMESPACE}" \
    --header "X-Vault-Token: ${CLIENT_TOKEN}")

    ## Going to retry 10 times
    if [ $retry_count -eq 10 ]; then
        >&2 echo "Failed to retrieve secrets: $(cat response.txt)"
        exit 1
    elif [ "$STATUS_CODE" != "200" ]; then
        >&1 echo "Attempt $retry_count: failed to retrieve secrets: $(cat response.txt) - retrying in $backoff_time seconds"
    fi
    sleep $backoff_time
done
echo "Begin processing keys"
# Extract all the secrets from Vault and export as env vars.
# `|| true` allows this to succeed if grep finds nothing.
echo "Checking for keys using ${DATA_PATH} path"
keys=$(<response.txt jq -r "${DATA_PATH}//empty | to_entries | .[].key")
if [ "${keys}" = "" ];
then
    echo "Nothing found at ${DATA_PATH} using .data path instead"
    DATA_PATH=.data
    keys=$(<response.txt jq -r "${DATA_PATH}//empty | to_entries | .[].key")
fi
echo "keys: $keys"
for key in $keys; do
    key=${key//$'\r'}
    value=$(<response.txt jq -r "${DATA_PATH}.${key}")
    firstChars=${value:0:4}
    echo "process $key from vault $firstChars"
    export "$key"="$value"
done
echo "Done processing keys"
rm response.txt

# Start the app, passing through any parameters that have been supplied.
exec "$@"