#!/bin/bash
echo "[Info] Nats startup"

cd $(go env GOPATH)/pkg/mod/github.com/nats-io/nats-streaming-server@v0.25*

go run nats-streaming-server.go

echo "[Info] Nats server turned off"
