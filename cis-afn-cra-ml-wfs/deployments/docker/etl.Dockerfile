# FROM base-images.mgti-dal-so-art.mrshmc.com/mmc/ubuntu/python:3.10-focal
FROM base-images.mgti-dal-so-art.mrshmc.com/mmc/ubuntu/python:3.11-jammy

# Change working directory
WORKDIR /usr/src/app
ENV DAGSTER_HOME=/usr/src/app
ENV TZ=America/New_York

# Copy requirements
COPY ./workflows/requirements.txt ./

# Install the required Python packages in the new environment
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --upgrade -r requirements.txt
RUN apt-get -y update

# Install git, sudo, expect, and python3
RUN apt-get -y install git
RUN apt-get update && apt-get install -y sudo
RUN sudo apt-get install expect -y
RUN sudo apt-get install python3

ARG HOME_PATH=
ENV HOME_PATH ${DAGSTER_HOME}

ARG DEPLOYMENT_MODE=
ENV DEPLOYMENT_MODE ${DEPLOYMENT_MODE}

ARG AZURE_API_CONN_STR_CODE=
ENV AZURE_API_CONN_STR_CODE ${AZURE_API_CONN_STR_CODE}

ARG IS_TEST=
ENV IS_TEST ${IS_TEST}

# Copy source code
COPY ./workflows ./

# Copy the script of vault activation into the container
COPY ./deployments/entrypoint.sh ./deployments/entrypoint.sh

# Expose the port that FastAPI will run on
EXPOSE 3030
RUN chmod u+x ./deployments/entrypoint.sh
RUN apt-get install -y jq

ENTRYPOINT ["./deployments/entrypoint.sh"]

# Run the UI
CMD ["dagster", "dev", "-f", "jobs.py", "-h", "0.0.0.0", "-p", "3030"]

# Record build metadata in the image
ARG BUILD_DATE_TIME
LABEL BUILD_DATE_TIME $BUILD_DATE_TIME
ENV BUILD_DATE_TIME $BUILD_DATE_TIME

ARG BUILD_GIT_COMMIT
LABEL BUILD_GIT_COMMIT $BUILD_GIT_COMMIT
ENV BUILD_GIT_COMMIT $BUILD_GIT_COMMIT

ARG BUILD_VERSION
LABEL BUILD_VERSION $BUILD_VERSION
ENV BUILD_VERSION $BUILD_VERSION