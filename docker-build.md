# Building with docker

## Prerequisites

Setup qemu for docker arm on x86:
```bash
# from official docs https://docs.docker.com/build/building/multi-platform/
docker run --privileged --rm tonistiigi/binfmt --install linux/arm/v7
```

## Build and run image

```bash
# build and run (takes at least 30 min)
docker compose build --progress=plain
docker compose up -d
```

To enter container:
```bash
# check if running
docker compose ps

# enter with bash in container
docker compose exec -it janus-gateway-devbox bash
```

To stop container:
```bash
docker compose stop -t 0
```
