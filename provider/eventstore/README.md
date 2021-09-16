
# EventStoreDB Implementation

## Run locally for tests 

```bash
docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore --insecure
```

## Run locally with functional UI features

```bash
docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore --insecure --run-projections=All --enable-atom-pub-over-http=true
```
