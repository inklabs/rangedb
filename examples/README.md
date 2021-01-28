# RangeDB Examples

## Code Examples

* Go
  * [Chat App](./chat)

---

## API Examples

### HTTP API

https://pkg.go.dev/github.com/inklabs/rangedb/pkg/rangedbapi

* [Get All Events](../pkg/rangedbapi/get_all_events_test.go)
* [Get Events by Aggregate Type](../pkg/rangedbapi/get_events_by_aggregate_type_test.go)
* [Get Events by Multiple Aggregate Types](../pkg/rangedbapi/get_events_by_aggregate_types_test.go)
* [Get Events by Stream](../pkg/rangedbapi/get_events_by_stream_test.go)
* [Get Events by Stream as Newline Delimited JSON](../pkg/rangedbapi/get_events_by_stream_ndjson_test.go)
* [Save Events](../pkg/rangedbapi/save_events_test.go)
  * [Optimistic Concurrency](../pkg/rangedbapi/save_events_with_optimistic_concurrency_test.go)
  * [Optimistic Concurrency Failure Response](../pkg/rangedbapi/save_events_with_optimistic_concurrency_failure_test.go)

### Websocket API

https://pkg.go.dev/github.com/inklabs/rangedb/pkg/rangedbws

* [Stream All Events](../pkg/rangedbws/stream_all_events_test.go)
* [Stream Events by Aggregate Type](../pkg/rangedbws/stream_events_by_aggregate_type_test.go)

### gRPC

https://pkg.go.dev/github.com/inklabs/rangedb/pkg/grpc/rangedbserver

* [Get All Events](../pkg/grpc/rangedbserver/get_all_events_test.go)
* [Get Events by Stream](../pkg/grpc/rangedbserver/get_events_by_stream_test.go)
* [Get Events by Aggregate Type(s)](../pkg/grpc/rangedbserver/get_events_by_aggregate_types_test.go)
* [Subscribe to All Events](../pkg/grpc/rangedbserver/subscribe_all_events_test.go)
* [Subscribe to Events By Aggregate Type(s)](../pkg/grpc/rangedbserver/subscribe_events_by_aggregate_type_test.go)
* [Save Events](../pkg/grpc/rangedbserver/save_test.go)
  * [Failure Response](../pkg/grpc/rangedbserver/save_failure_response_test.go)
* [Optimistic Save Events](../pkg/grpc/rangedbserver/optimistic_save_test.go)
  * [Failure Response](../pkg/grpc/rangedbserver/optimistic_save_failure_test.go)

---

## GDPR Examples

### Crypto-shredding

https://verraes.net/2019/05/eventsourcing-patterns-throw-away-the-key/

* [Encrypt/Decrypt Event](../pkg/crypto/eventencryptor/encrypt_event_test.go)
* [Delete Encryption Key](../pkg/crypto/eventencryptor/delete_encryption_key_test.go)
