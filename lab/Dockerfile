FROM ubuntu:24.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       iproute2 procps stress-ng \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/artc/bin
COPY build/release/artc_lab_node /opt/artc/bin/artc_lab_node
COPY build/release/artc_loadgen /opt/artc/bin/artc_loadgen

WORKDIR /opt/artc
