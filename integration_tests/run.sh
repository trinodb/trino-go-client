#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

LOCAL_PORT=8080
# TODO update to trino after the release
IMAGE_NAME=prestosql/presto

cd "$( dirname "${BASH_SOURCE[0]}" )"

function test_cleanup() {
   docker rm -f $CONTAINER
}

trap test_cleanup EXIT

function test_query() {
	docker exec -t $CONTAINER bin/presto --server localhost:${LOCAL_PORT} --execute "$*"
}

CONTAINER=$(docker run -v "$PWD/etc:/etc/presto" -p ${LOCAL_PORT}:${LOCAL_PORT} --rm -d $IMAGE_NAME)

attempts=10
while [ $attempts -gt 0 ]
do
	attempts=`expr $attempts - 1`
	ready=`test_query "SHOW SESSION" | grep task_writer_count`
	[ ! -z "$ready" ] && break
	echo "waiting for trino..."
	sleep 2
done

if [ $attempts -eq 0 ]
then
	echo "timed out waiting for trino"
	exit 1
fi

PKG=../trino
DSN=http://test@localhost:${LOCAL_PORT}
go test -v -cover -coverprofile=coverage.out $PKG -trino_server_dsn=$DSN $*
