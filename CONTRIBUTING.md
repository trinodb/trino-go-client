# Contributing to Trino

## Contributor License Agreement ("CLA")

In order to accept your pull request, we need you to [submit a CLA](https://github.com/trinodb/cla).

## License

By contributing to Trino, you agree that your contributions will be licensed under the [Apache License Version 2.0 (APLv2)](LICENSE).

# Go Test

Please run [go test](https://pkg.go.dev/testing) before creating a pull request.

The unit tests run against an in-process fake coordinator and finish in a few
seconds:

```bash
go test -short -v -race ./...
```

The integration tests start Trino, and an S3 emulator for the spooling protocol,
in Docker:

```bash
go test -v -race -timeout 2m ./... -trino_image_tag=latest
```

The `-trino_image_tag` flag picks the Trino release; CI runs `latest` and `372`.
To iterate against a running server, start the containers once with
`-no_cleanup`, then pass its address with `-trino_server_dsn`:

```bash
go test -run TestIntegrationNoResults ./... -no_cleanup
go test -v -run 'TestIntegration.*' ./... -trino_server_dsn=http://test@localhost:$(docker port trino-go-client-tests 8080 | head -1 | sed 's/.*://')
```

Tests that need a feature the server lacks skip themselves based on the
version the coordinator reports. `-trino_query_timeout` bounds every query
the integration tests run.

# Releases

To create a new release, a maintainer with repository write permissions needs to create and push a new git tag.
