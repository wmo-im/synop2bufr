# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM wmoim/dim_eccodes_baseimage:2.31.0

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Install awslambdaric
RUN pip install \
    --target ${FUNCTION_DIR} \
        awslambdaric

ENV TZ="Etc/UTC" \
    DEBIAN_FRONTEND="noninteractive" \
    DEBIAN_PACKAGES="gnupg2 cron bash vim git libffi-dev libeccodes0 python3-eccodes python3-cryptography libssl-dev libudunits2-0 python3-paho-mqtt python3-dateparser python3-tz python3-setuptools" \
    ECCODES_DIR=/opt/eccodes \
    PATH="$PATH;/opt/eccodes/bin"

RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until \
    && apt-get update -y \
    && apt-get install -y ${DEBIAN_PACKAGES} \
    && apt-get install -y python3 python3-pip libeccodes-tools \
    && pip3 install --no-cache-dir https://github.com/wmo-im/csv2bufr/archive/refs/tags/v0.7.4.zip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/pymetdecoder/archive/refs/tags/v0.1.10.zip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/synop2bufr/archive/refs/tags/v0.6.2.zip 

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
COPY . ${FUNCTION_DIR}

ENV LOG_LEVEL=INFO

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/bin/python3", "-m", "awslambdaric" ]
# Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_function.handler" ]