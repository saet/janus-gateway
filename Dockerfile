FROM debian:12-slim AS base

FROM base AS builder

# install build tools and janus dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends -y apt-utils software-properties-common git build-essential \
    	libmicrohttpd-dev libjansson-dev \
		libssl-dev libsofia-sip-ua-dev libglib2.0-dev \
		libopus-dev libogg-dev libcurl4-openssl-dev liblua5.3-dev \
		libconfig-dev pkg-config libtool automake python3 python3-pip python3-setuptools python3-wheel ninja-build meson \
     	gengetopt libsrtp2-dev

# install libsrtp with openssl support on armv7 only for aes_gcm_128_16 support
WORKDIR /
RUN git clone https://github.com/cisco/libsrtp.git && \
    cd libsrtp && \
	git checkout 90d05bf8980d16e4ac3f16c19b77e296c4bc207b && \
    ./configure --enable-openssl --enable-nss && \
    make -j$(nproc) && \
    make install && \
    make DESTDIR=/janus-gateway/build install

# install usrsctp to enable janus datachannel
WORKDIR /
RUN git clone https://github.com/sctplab/usrsctp && \
    cd usrsctp && \
	git checkout 87f52843f9cf7dda0d4239ec22946ab922f98876 && \
    ./bootstrap && \
    ./configure --disable-programs --disable-inet --disable-inet6 && \
    make -j$(nproc) && \
    make install && \
	make DESTDIR=/janus-gateway/build install

# install updated version of libnice
WORKDIR /
RUN git clone https://gitlab.freedesktop.org/libnice/libnice && \
    cd libnice && \
	git checkout 3d9cae16a5094aadb1651572644cb5786a8b4e2d && \
    meson build && \
    ninja -C build && \
    ninja -C build install && \
    DESTDIR=/janus-gateway/build ninja -C build install

WORKDIR /

COPY . .

# build janus-gateway
RUN sh autogen.sh && \
    ./configure --prefix=/opt/janus --disable-unix-sockets --disable-sample-event-handler --disable-gelf-event-handler \
    --disable-all-plugins --enable-plugin-streaming --enable-plugin-videomux --enable-plugin-frame --enable-plugin-ptz && \
    make && \
    make DESTDIR=/janus-gateway/build install

FROM base

WORKDIR /opt/janus

COPY --from=builder /janus-gateway/build /

RUN ldconfig -p && \
    apt-get update && \
    apt-get install --no-install-recommends -y libgio-qt0 libjansson4 openssl ca-certificates curl libmicrohttpd12 libsrtp2-1 && \
    apt-get clean

ENTRYPOINT ["/opt/janus/bin/janus", "-o", "-F", "/opt/videocloud/janus_config"]
