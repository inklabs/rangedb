# RangeDB Examples

## HTTP API Examples

https://pkg.go.dev/github.com/inklabs/rangedb/pkg/rangedbapi

* [Get All Events](../pkg/rangedbapi/get_all_events_test.go)
* [Get Events by Aggregate Type](../pkg/rangedbapi/get_events_by_aggregate_type_test.go)
* [Get Events by Multiple Aggregate Types](../pkg/rangedbapi/get_events_by_aggregate_types_test.go)
* [Get Events by Stream](../pkg/rangedbapi/get_events_by_stream_test.go)
* [Get Events by Stream as Newline Delimited JSON](../pkg/rangedbapi/get_events_by_stream_ndjson_test.go)
* [Save Events](../pkg/rangedbapi/save_events_test.go)

## Websocket API Examples

https://pkg.go.dev/github.com/inklabs/rangedb/pkg/rangedbws

* [Stream All Events](../pkg/rangedbws/stream_all_events_test.go)
* [Stream Events by Aggregate Type](../pkg/rangedbws/stream_events_by_aggregate_type_test.go)

## gRPC Examples

https://pkg.go.dev/github.com/inklabs/rangedb/pkg/grpc/rangedbserver

* [Get All Events](../pkg/grpc/rangedbserver/get_all_events_test.go)
* [Get Events by Stream](../pkg/grpc/rangedbserver/get_events_by_stream_test.go)
* [Get Events by Aggregate Type(s)](../pkg/grpc/rangedbserver/get_events_by_aggregate_types_test.go)
* [Subscribe to All Events](../pkg/grpc/rangedbserver/subscribe_all_events_test.go)
