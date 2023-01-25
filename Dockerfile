ARG BASE_IMAGE=arm32v7/debian:buster

FROM ${BASE_IMAGE} AS clean-env

# install build tools and janus dependencies
RUN apt-get update && \
    apt-get install -y apt-utils software-properties-common git build-essential \
    	libmicrohttpd-dev libjansson-dev \
		libssl-dev libsofia-sip-ua-dev libglib2.0-dev \
		libopus-dev libogg-dev libcurl4-openssl-dev liblua5.3-dev \
		libconfig-dev pkg-config libtool automake python3 python3-pip python3-setuptools python3-wheel ninja-build && \
    pip3 install meson

# install libsrtp with openssl support on armv7 only for aes_gcm_128_16 support
WORKDIR /
RUN git clone https://github.com/cisco/libsrtp.git && \
    cd libsrtp && \
	git checkout 90d05bf8980d16e4ac3f16c19b77e296c4bc207b && \
    ./configure --prefix=/usr --enable-openssl --enable-nss && \
    make && \
    make install && \
    make DESTDIR=/janus-gateway/build install

# install usrsctp to enable janus datachannel
WORKDIR /
RUN git clone https://github.com/sctplab/usrsctp && \
    cd usrsctp && \
	git checkout 87f52843f9cf7dda0d4239ec22946ab922f98876 && \
    ./bootstrap && \
    ./configure --prefix=/usr --disable-programs --disable-inet --disable-inet6 && \
    make && \
    make install && \
	make DESTDIR=/janus-gateway/build install

# install updated version of libnice
WORKDIR /
RUN git clone https://gitlab.freedesktop.org/libnice/libnice && \
    cd libnice && \
	git checkout 3d9cae16a5094aadb1651572644cb5786a8b4e2d && \
    meson --prefix=/usr build && \
    ninja -C build && \
    ninja -C build install && \
    DESTDIR=/janus-gateway/build ninja -C build install

# export libogg since janus streaming plugins now requires it
RUN mkdir -p /janus-gateway/build/usr/lib && \
    cp -r /usr/lib/arm-linux-gnueabihf/libogg.so* /janus-gateway/build/usr/lib

WORKDIR /janus-gateway

COPY . .

FROM clean-env AS builder

# build janus-gateway
RUN sh autogen.sh && \
    ./configure --prefix=/opt/janus --disable-unix-sockets --disable-sample-event-handler --disable-gelf-event-handler \
    --disable-plugin-audiobridge --disable-plugin-echotest --disable-plugin-recordplay --disable-plugin-sip \
    --disable-plugin-nosip --disable-plugin-textroom --disable-plugin-videocall --disable-plugin-videoroom \
    --disable-plugin-voicemail && \
    make && \
    make DESTDIR=/janus-gateway/build install

FROM builder as devbox

# install some common utils for dev
RUN apt-get update && \
	apt-get install -y wget curl net-tools tree neovim nano

ENTRYPOINT ["tail", "-F", "/dev/null"]
