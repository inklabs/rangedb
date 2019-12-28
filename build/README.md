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
