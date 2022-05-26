FROM ubuntu:20.04
LABEL key="mvg_tracker"
LABEL version="0.1.2"
LABEL org.opencontainers.image.authors="joshuaziegler96@web.de"

RUN \
  apt-get update -qq && \
  apt-get install -qq -y \
    python3 \
    python3-pip\
    git\
    iputils-ping


RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install importlib_metadata
RUN python3 -m pip install git+https://github.com/Joshua-96/MVG_tracker.git

