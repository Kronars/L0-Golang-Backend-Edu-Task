#!/bin/bash
echo "[Init] Nats startup"

cd $(go env GOPATH)/pkg/mod/github.com/nats-io/nats-streaming-server@v0.25*

go run nats-streaming-server.go

echo "[Init] Nats server turned off"
