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
COPY ./api/main.py ./


# Install the dependencies
RUN pip install --no-cache-dir -r ./requirements.txt

# Copy the rest of your application code into the container
COPY ["./api/utils", "./utils"]
# This copies everything from the api directory into /app in the container

# Copy the script of vault activation into the container
COPY ./deployments/entrypoint.sh ./deployments/entrypoint.sh

# Expose the port that FastAPI will run on
EXPOSE 8000

RUN chmod 777 ./deployments/entrypoint.sh

ENTRYPOINT ["./deployments/entrypoint.sh"]

#with log file
CMD ["celery", "-A", "main.celery_app", "worker", "--loglevel=info"]

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