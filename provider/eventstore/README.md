
# EventStoreDB Implementation

## Run locally for tests 

```bash
docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore:21.10.0-bionic --insecure --mem-db
```

## Run locally with functional UI features

```bash
docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore:21.10.0-bionic --insecure --run-projections=All --enable-atom-pub-over-http=true
```
