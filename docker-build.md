# Building with docker

## Prerequisites

Setup qemu for docker arm on x86:
```bash
# from official docs https://docs.docker.com/build/building/multi-platform/
docker run --privileged --rm tonistiigi/binfmt --install arm64
```

## Build and run image

```bash
docker build . --platform linux/arm64 -t janus-gateway-devbox
docker run --rm -d --name devbox janus-gateway-devbox
```

To enter container:
```bash
# check if running
docker ps

# enter with bash in container
docker exec -it devbox bash
```

To stop container:
```bash
docker kill devbox
```
