# RangeDB ![RangeDB Logo](https://github.com/inklabs/rangedb/blob/master/pkg/rangedbui/static/img/rangedb-logo-white-30x30.png)

[![Build Status](https://travis-ci.org/inklabs/rangedb.svg?branch=master)](https://travis-ci.org/inklabs/rangedb)
[![Docker Build Status](https://img.shields.io/docker/cloud/build/inklabs/rangedb)](https://hub.docker.com/r/inklabs/rangedb/builds)
[![Go Report Card](https://goreportcard.com/badge/github.com/inklabs/rangedb)](https://goreportcard.com/report/github.com/inklabs/rangedb)
[![Test Coverage](https://api.codeclimate.com/v1/badges/c19eabe7c73ccc64738e/test_coverage)](https://codeclimate.com/github/inklabs/rangedb/test_coverage)
[![Maintainability](https://api.codeclimate.com/v1/badges/c19eabe7c73ccc64738e/maintainability)](https://codeclimate.com/github/inklabs/rangedb/maintainability)
[![GoDoc](https://godoc.org/github.com/inklabs/rangedb?status.svg)](https://godoc.org/github.com/inklabs/rangedb)
[![Go Version](https://img.shields.io/github/go-mod/go-version/inklabs/rangedb.svg)](https://github.com/inklabs/rangedb/blob/master/go.mod)
[![Release](https://img.shields.io/github/release/inklabs/rangedb.svg?include_prereleases&sort=semver)](https://github.com/inklabs/rangedb/releases/latest)
[![Sourcegraph](https://sourcegraph.com/github.com/inklabs/rangedb/-/badge.svg)](https://sourcegraph.com/github.com/inklabs/rangedb?badge)
[![License](https://img.shields.io/github/license/inklabs/rangedb.svg)](https://github.com/inklabs/rangedb/blob/master/LICENSE)

RangeDB is an event store database written in Go. This package includes a stand-alone database
and web server along with a library for embedding event sourced applications.

Examples are provided [here](examples).

## Backend Engines

RangeDB supports various backend database engines.

- [PostgreSQL](https://www.postgresql.org/)
- [LevelDB](https://github.com/google/leveldb)
- [EventStoreDB](https://www.eventstore.com/eventstoredb)
- [In Memory](https://github.com/inklabs/rangedb/tree/master/provider/inmemorystore)

### Coming Soon:

- [Redis](https://redis.com/)
- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [Axon Server](https://developer.axoniq.io/axon-server)

## Docker Quickstart

```
docker run -p 8080:8080 inklabs/rangedb
```

## Community

- [DDD-CQRS-ES slack group](https://github.com/ddd-cqrs-es/slack-community) channel: #rangedb
- [Upcoming topics](https://github.com/inklabs/rangedb/wiki/Upcoming-Topics) for monthly pairing sessions


## Projects using RangeDB

* [GOAuth2](https://github.com/inklabs/goauth2) - An OAuth2 Server in Go
