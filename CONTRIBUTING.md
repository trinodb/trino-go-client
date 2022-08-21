# Contributing to Trino

## Contributor License Agreement ("CLA")

In order to accept your pull request, we need you to [submit a CLA](https://github.com/trinodb/cla).

## License

By contributing to Trino, you agree that your contributions will be licensed under the [Apache License Version 2.0 (APLv2)](LICENSE).

# Go Test

Please Run [go test](https://pkg.go.dev/testing) before creating Pull Request

```bash
go test -v -race -timeout 1m ./...
```

# Releases

To create a new release, a maintainer with repository write permissions needs to create and push a new git tag.
