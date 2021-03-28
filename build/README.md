# RangeDB Docker

Docker containers are automatically built here: https://hub.docker.com/r/inklabs/rangedb

## Building Locally

### Build Image

```
docker build -f build/Dockerfile -t inklabs/rangedb:local .
```

### Run Container

```
docker run -p 8080:8080 inklabs/rangedb:local
```

## Using docker-compose to build & run locally in one step
* From root dir
    * docker-compose -f build/docker-compose.yml up --build
* If inside build directory
    * docker-compose up --build

*remove **--build** if not need to rebuild image*
