FROM wmoim/dim_eccodes_baseimage:2.28.0

ENV TZ="Etc/UTC" \
    DEBIAN_FRONTEND="noninteractive" \
    DEBIAN_PACKAGES="gnupg2 cron bash vim git libffi-dev libeccodes0 python3-eccodes python3-cryptography libssl-dev libudunits2-0 python3-paho-mqtt python3-dateparser python3-tz python3-setuptools" \
    ECCODES_DIR=/opt/eccodes \
    PATH="$PATH;/opt/eccodes/bin"


RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until \
    && apt-get update -y \
    && apt-get install -y ${DEBIAN_PACKAGES} \
    && apt-get install -y python3 python3-pip libeccodes-tools \
    && pip3 install --no-cache-dir https://github.com/wmo-im/csv2bufr/archive/master.zip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/pymetdecoder/archive/refs/tags/v0.1.2.zip

ENV LOG_LEVEL=INFO

#WORKDIR /build
# copy the app
COPY . /build

# install pymetdecoder and synop2bufr
RUN cd /build \
    && python3 setup.py install \
    # delete the build folder that is no longer needed after installing the modules
    && rm -r /build

RUN adduser wis2user
USER wis2user
WORKDIR /home/wis2user