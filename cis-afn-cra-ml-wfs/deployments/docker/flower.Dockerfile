# Use the official Python image from the Docker Hub
FROM base-images.mgti-dal-so-art.mrshmc.com/mmc/ubuntu/python:3.11-jammy

RUN apt update && apt install tzdata -y
ENV TZ="America/New_York"

RUN apt-get update -qq \
  && apt-get install -y jq \
  && apt-get clean

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY ./api/requirements.txt ./



# Install the dependencies
RUN pip install --no-cache-dir -r ./requirements.txt

ARG DEPLOYMENT_MODE=
ENV DEPLOYMENT_MODE ${DEPLOYMENT_MODE}

ARG IS_TEST=
ENV IS_TEST ${IS_TEST}


# This copies everything from the api directory into /app in the container

# Copy the script of vault activation into the container
COPY ./deployments/entrypoint.sh ./deployments/entrypoint.sh

# Expose the port that FastAPI will run on
EXPOSE 5555

RUN chmod 777 ./deployments/entrypoint.sh

ENTRYPOINT ["./deployments/entrypoint.sh"]

# Get environment type information for redis server
RUN if [ "$IS_TEST" = "True" ]; then \
        export ENVIRONMENT_TYPE="test"; \
    else \
        export ENVIRONMENT_TYPE=$(echo "$DEPLOYMENT_MODE" | tr 'A-Z' 'a-z'); \
    fi && \
    echo "ENVIRONMENT_TYPE=$ENVIRONMENT_TYPE" >> /etc/environment

# Command to celery with flower
CMD ["sh", "-c", "celery", "--broker=redis://redis-${ENVIRONMENT_TYPE}-svc:6379/0", "flower", "--port=5555", "--url-prefix=flower"]

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