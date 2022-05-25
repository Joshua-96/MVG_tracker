FROM ubuntu:20.04
LABEL key="mvg_tracker"
LABEL version="0.1.0"
LABEL org.opencontainers.image.authors="joshuaziegler96@web.de"

RUN \
  apt-get update -qq && \
  apt-get install -qq -y \
    python3 \
    python3-pip\
    iputils-ping

RUN mkdir /root/workdir
COPY requirements.txt /root/workdir
COPY LUTs /root/workdir/LUTs
COPY mvg_tracker/MVG_config.json /root/workdir
COPY mvg_tracker/data_gathering.py /root/workdir
RUN python3 -m pip install -r /root/workdir/requirements.txt
# RUN python3 data_gathering.py
