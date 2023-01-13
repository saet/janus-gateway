ARG BASE_IMAGE=arm64v8/debian:11

FROM ${BASE_IMAGE} AS clean-env

# install build tools and janus dependencies
RUN apt-get update && \
    apt-get install -y apt-utils software-properties-common git build-essential \
    	libmicrohttpd-dev libjansson-dev \
		libssl-dev libsofia-sip-ua-dev libglib2.0-dev \
		libopus-dev libogg-dev libcurl4-openssl-dev liblua5.3-dev \
		libconfig-dev pkg-config libtool automake libnice-dev libsrtp2-dev

WORKDIR /

# install usrsctp to enable janus datachannel
RUN git clone https://github.com/sctplab/usrsctp && \
    cd usrsctp && \
	git checkout 87f52843f9cf7dda0d4239ec22946ab922f98876 && \
    ./bootstrap && \
    ./configure --prefix=/usr --disable-programs --disable-inet --disable-inet6 && \
    make && \
    make install

WORKDIR /janus-gateway

COPY . .

FROM clean-env AS builder

# build janus-gateway
RUN sh autogen.sh && \
    ./configure --prefix=/janus-gateway/build && \
    make && \
    make install

FROM builder as devbox

# install some common utils for dev
RUN apt-get update && \
	apt-get install -y wget curl net-tools tree neovim nano

ENTRYPOINT ["tail", "-F", "/dev/null"]
