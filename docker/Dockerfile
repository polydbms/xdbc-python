FROM xdbc-client:latest
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    make \
    cmake \
    protobuf-compiler \
    g++ \
    libboost-all-dev \
    libpq-dev \
    wget \
    gdb \
    odbc-postgresql \
    plocate \
    nano

RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh --no-check-certificate \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

RUN conda --version
RUN conda update -n base -c defaults conda
RUN conda create --name python39 python==3.9 -y
RUN conda install -n \
    python39 numpy pybind11 xtensor-python xtensor pandas psycopg2 sqlalchemy connectorx turbodbc pyarrow\
     -c conda-forge -y

ENV CONDA_DEFAULT_ENV=python39
ENV CONDA_PREFIX=/root/miniconda3/envs/python39
ENV PATH="$CONDA_PREFIX/bin:$PATH"
RUN echo "source activate python39" > ~/.bashrc

#update postgres odbc driver location
RUN full_path=$(locate psqlodbca.so | head -n 1) && \
    sed -i "s|Driver=psqlodbca.so|Driver=$full_path|g" /etc/odbcinst.ini

WORKDIR /workspace
