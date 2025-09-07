FROM base-images.mgti-dal-so-art.mrshmc.com/mmc/ubuntu:22.04

RUN apt update && apt install tzdata -y
ENV TZ="America/New_York"

RUN apt-get update -qq \
  && apt-get install -y jq lsb-release curl gpg \
  && apt-get clean

RUN curl -fsSL https://packages.redis.io/gpg | gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

RUN echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/redis.list


EXPOSE 6379

RUN apt-get update
RUN apt-get install -y redis

COPY ./deployments/redis.conf /usr/local/etc/redis/redis.conf

CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]