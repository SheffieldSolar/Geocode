FROM python:3.9

RUN apt-get -qq update && apt-get -qq install -y \
    curl \
    git \
    wget \
    libproj-dev \
    proj-data \
    proj-bin \
    libgeos-dev \
    libgdal-dev \
    python3-gdal \
    python3-dev \
    cmake \
    sqlite3 \
    gdal-bin \
    && apt-get autoclean && apt-get autoremove \
    > /dev/null

### Install Proj 8 (see https://github.com/SciTools/cartopy/issues/1879) ###
RUN curl -sSL https://download.osgeo.org/proj/proj-8.2.1.tar.gz | tar -xvz -C /tmp
WORKDIR /tmp/proj-8.2.1
RUN mkdir build
WORKDIR /tmp/proj-8.2.1/build
RUN cmake ..
RUN cmake --build .
RUN cmake --build . --target install
######

WORKDIR /geocode

COPY requirements.txt /geocode/requirements.txt

RUN pip install -U pip && pip install --no-cache-dir -r /geocode/requirements.txt > /dev/null

CMD ["bash"]