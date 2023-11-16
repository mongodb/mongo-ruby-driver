FROM debian:10

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get -y install zlib1g-dev git ruby-bundler make gcc ruby-dev

WORKDIR /app

COPY . .
