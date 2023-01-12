ARG BASE_IMAGE=arm64v8/debian:11
FROM ${BASE_IMAGE} AS clean-env

WORKDIR /janus-gateway

# install build tools and janus dependencies
RUN apt-get update && \
    apt-get install -y apt-utils software-properties-common git build-essential \
    	libmicrohttpd-dev libjansson-dev \
		libssl-dev libsofia-sip-ua-dev libglib2.0-dev \
		libopus-dev libogg-dev libcurl4-openssl-dev liblua5.3-dev \
		libconfig-dev pkg-config libtool automake libnice-dev libsrtp2-dev

COPY . .

FROM clean-env AS builder

# build janus-gateway
RUN sh autogen.sh && \
    ./configure --prefix=/janus-gateway/build && \
    make && \
    make install

FROM builder as devbox

ENTRYPOINT ["tail", "-F", "/dev/null"]
