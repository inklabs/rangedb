FROM golang:1.14.1-alpine AS build

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
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags "-extldflags -static" -o /go/bin/rangedb ./cmd/rangedb

# Prepare final image
FROM scratch AS release
COPY --from=build /go/bin/rangedb /bin/rangedb
ENTRYPOINT ["/bin/rangedb"]
EXPOSE 8080
