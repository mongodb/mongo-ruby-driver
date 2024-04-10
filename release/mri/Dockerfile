FROM debian:12

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get -y install git ruby-bundler make gcc ruby-dev zlib1g-dev

WORKDIR /app

COPY . .
