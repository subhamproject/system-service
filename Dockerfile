FROM golang:1.19.4-buster as builder

# Create and change to the app directory.
WORKDIR /app

# Expecting to copy go.mod and if present go.sum.
COPY go.* ./

RUN go mod download

# Copy local code to the container image.
COPY . ./

# Build the binary.
RUN go build -v -o system-service
EXPOSE 8083

FROM debian:buster-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists


COPY --from=builder /app/system-service /app/system-service

# Run the binary program produced by `go install`
CMD ["/app/system-service"]


