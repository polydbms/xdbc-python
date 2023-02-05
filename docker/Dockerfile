FROM ubuntu:22.04
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    cmake \
    protobuf-compiler \
    g++ \
    libboost* \
    libpq-dev \
    wget

RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh --no-check-certificate \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

RUN conda --version
RUN conda update -n base -c defaults conda
RUN conda create --name python39 python==3.9
RUN conda install numpy
RUN conda install -c conda-forge pybind11
RUN conda install -c conda-forge xtensor-python
RUN conda install -c conda-forge xtensor