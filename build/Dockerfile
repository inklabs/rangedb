FROM golang:1.13.5-alpine AS build

RUN apk add --update --no-cache ca-certificates gcc g++ libc-dev

WORKDIR /code

# Download Dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download

# Test
COPY . .
RUN go generate ./... && go vet ./... && go test ./...

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags "-extldflags -static" -o /go/bin/rangedb-api ./cmd/rangedb-api

# Prepare final image
FROM scratch AS release
COPY --from=build /go/bin/rangedb-api /bin/rangedb-api
ENTRYPOINT ["/bin/rangedb-api"]
EXPOSE 8080
