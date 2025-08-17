FROM xdbc-client:latest
ENV DEBIAN_FRONTEND=noninteractive

# Install OS deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    software-properties-common unixodbc-dev \
    build-essential libboost-all-dev libpq-dev pybind11-dev \
 && add-apt-repository ppa:deadsnakes/ppa \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    python3.9 python3.9-distutils python3.9-venv python3.9-dev python3-pip \
 && rm -rf /var/lib/apt/lists/*


# Upgrade pip
RUN python3.9 -m pip install --upgrade pip uv

# Install all Python deps together so pybind11 is available before turbodbc builds
RUN python3.9 -m pip install pybind11
RUN python3.9 -m pip install \
    pandas==2.2.* \
    duckdb==1.0.0 \
    sqlalchemy==2.0.35 \
    connectorx==0.3.3 \
    pyarrow==18.1.0 \
    modin[ray]==0.30.1 \
    ray==2.1.0 \
    psycopg2-binary \
    turbodbc==4.4.0

# Update postgres ODBC driver location
RUN full_path=$(locate psqlodbca.so | head -n 1) && \
    sed -i "s|Driver=psqlodbca.so|Driver=$full_path|g" /etc/odbcinst.ini

# Copy source
COPY python/ /workspace/python
COPY tests/ /workspace/tests
COPY CMakeLists.txt /workspace

# pyarrow from pip
RUN python3.9 -m pip install pyarrow==18.1.0
RUN python3.9 -m pip install numpy
RUN python3.9 -m pip install requests
RUN python3.9 -m pip install aiohttp

# Build C++ library and install to Python site-packages
RUN cd /workspace && mkdir -p build && cd build && rm -rf * && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DPython3_EXECUTABLE=$(which python3.9) \
        -DPython3_LIBRARY=/usr/lib/x86_64-linux-gnu/libpython3.9.so && \
    make && \
    make install

# The pyarrow shared library is located in its site-packages directory.
ENV LD_LIBRARY_PATH=/usr/local/lib/python3.9/dist-packages/pyarrow:$LD_LIBRARY_PATH

WORKDIR /workspace
