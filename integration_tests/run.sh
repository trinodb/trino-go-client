#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

LOCAL_PORT=8080
IMAGE_NAME=trinodb/trino

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

function test_cleanup() {
    docker rm -f "$CONTAINER"
}

trap test_cleanup EXIT

function test_query() {
    docker exec -t "$CONTAINER" bin/trino --server localhost:${LOCAL_PORT} --execute "$*"
}

CONTAINER=$(docker run -v "$PWD/etc:/etc/trino" -p ${LOCAL_PORT}:${LOCAL_PORT} --rm -d $IMAGE_NAME)

attempts=10
while [ $attempts -gt 0 ]; do
    attempts=$((attempts - 1))
    ready=$(test_query "SHOW SESSION" | grep task_writer_count)
    [ -n "$ready" ] && break
    echo "waiting for Trino..."
    sleep 2
done

if [ $attempts -eq 0 ]; then
    echo "timed out waiting for Trino"
    exit 1
fi

PKG=../trino
DSN=http://test@localhost:${LOCAL_PORT}
go test -v -race -timeout 10s -cover -coverprofile=coverage.out $PKG -trino_server_dsn=$DSN "$@"
