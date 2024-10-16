# Trino Go client

A [Trino](https://trino.io) client for the [Go](https://golang.org) programming
language. It enables you to send SQL statements from your Go application to
Trino, and receive the resulting data.

[![Build Status](https://github.com/trinodb/trino-go-client/workflows/ci/badge.svg)](https://github.com/trinodb/trino-go-client/actions?query=workflow%3Aci+event%3Apush+branch%3Amaster)
[![GoDoc](https://godoc.org/github.com/trinodb/trino-go-client?status.svg)](https://godoc.org/github.com/trinodb/trino-go-client)

## Features

* Native Go implementation
* Connections over HTTP or HTTPS
* HTTP Basic, Kerberos, and JSON web token (JWT) authentication
* Per-query user information for access control
* Support custom HTTP client (tunable conn pools, timeouts, TLS)
* Supports conversion from Trino to native Go data types
  * `string`, `sql.NullString`
  * `int64`, `sql.NullInt64`
  * `float64`, `sql.NullFloat64`
  * `map`, `trino.NullMap`
  * `time.Time`, `trino.NullTime`
  * Up to 3-dimensional arrays to Go slices, of any supported type

## Requirements

* Go 1.22 or newer
* Trino 372 or newer

## Installation

You need a working environment with Go installed and $GOPATH set.

Download and install Trino database/sql driver:

```bash
go get github.com/trinodb/trino-go-client/trino
```

Make sure you have Git installed and in your $PATH.

## Usage

This Trino client is an implementation of Go's `database/sql/driver` interface.
In order to use it, you need to import the package and use the
[`database/sql`](https://golang.org/pkg/database/sql/) API then.

Use `trino` as `driverName` and a valid [DSN](#dsn-data-source-name) as the
`dataSourceName`.

Example:

```go
import "database/sql"
import _ "github.com/trinodb/trino-go-client/trino"

dsn := "http://user@localhost:8080?catalog=default&schema=test"
db, err := sql.Open("trino", dsn)
```

### Authentication

Both HTTP Basic, Kerberos, and JWT authentication are supported.

#### HTTP Basic authentication

If the DSN contains a password, the client enables HTTP Basic authentication by
setting the `Authorization` header in every request to Trino.

HTTP Basic authentication **is only supported on encrypted connections over
HTTPS**.

#### Kerberos authentication

This driver supports Kerberos authentication by setting up the Kerberos fields
in the
[Config](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config)
struct.

Please refer to the [Coordinator Kerberos
Authentication](https://trino.io/docs/current/security/server.html) for
server-side configuration.

#### JSON web token authentication

This driver supports JWT authentication by setting up the `AccessToken` field
in the
[Config](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config)
struct.

Please refer to the [Coordinator JWT
Authentication](https://trino.io/docs/current/security/jwt.html) for
server-side configuration.

#### Authorization header forwarding
This driver supports forwarding authorization headers by adding a [NamedArg](https://godoc.org/database/sql#NamedArg) with the name `accessToken` (e.g., `accessToken=<your_access_token>`) and setting the `ForwardAuthorizationHeader` field in the [Config](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config) struct to `true`. 

When enabled, this configuration will override the `AccessToken` set in the `Config` struct.


#### System access control and per-query user information

It's possible to pass user information to Trino, different from the principal
used to authenticate to the coordinator. See the [System Access
Control](https://trino.io/docs/current/develop/system-access-control.html)
documentation for details.

In order to pass user information in queries to Trino, you have to add a
[NamedArg](https://godoc.org/database/sql#NamedArg) to the query parameters
where the key is X-Trino-User. This parameter is used by the driver to inform
Trino about the user executing the query regardless of the authentication
method for the actual connection, and its value is NOT passed to the query.

Example:

```go
db.Query("SELECT * FROM foobar WHERE id=?", 1, sql.Named("X-Trino-User", string("Alice")))
```

The position of the X-Trino-User NamedArg is irrelevant and does not affect the
query in any way.

### DSN (Data Source Name)

The Data Source Name is a URL with a mandatory username, and optional query
string parameters that are supported by this driver, in the following format:

```
http[s]://user[:pass]@host[:port][?parameters]
```

The easiest way to build your DSN is by using the
[Config.FormatDSN](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config.FormatDSN)
helper function.

The driver supports both HTTP and HTTPS. If you use HTTPS it's recommended that
you also provide a custom `http.Client` that can validate (or skip) the
security checks of the server certificate, and/or to configure TLS client
authentication.

#### Parameters

*Parameters are case-sensitive*

Refer to the [Trino
Concepts](https://trino.io/docs/current/overview/concepts.html) documentation
for more information.

##### `source`

```
Type:           string
Valid values:   string describing the source of the connection to Trino
Default:        empty
```

The `source` parameter is optional, but if used, can help Trino admins
troubleshoot queries and trace them back to the original client.

##### `catalog`

```
Type:           string
Valid values:   the name of a catalog configured in the Trino server
Default:        empty
```

The `catalog` parameter defines the Trino catalog where schemas exist to
organize tables.

##### `schema`

```
Type:           string
Valid values:   the name of an existing schema in the catalog
Default:        empty
```

The `schema` parameter defines the Trino schema where tables exist. This is
also known as namespace in some environments.

##### `session_properties`

```
Type:           string
Valid values:   comma-separated list of key=value session properties
Default:        empty
```

The `session_properties` parameter must contain valid parameters accepted by
the Trino server. Run `SHOW SESSION` in Trino to get the current list.

##### `custom_client`

```
Type:           string
Valid values:   the name of a client previously registered to the driver
Default:        empty (defaults to http.DefaultClient)
```

The `custom_client` parameter allows the use of custom `http.Client` for the
communication with Trino.

Register your custom client in the driver, then refer to it by name in the DSN,
on the call to `sql.Open`:

```go
foobarClient := &http.Client{
    Transport: &http.Transport{
        Proxy: http.ProxyFromEnvironment,
        DialContext: (&net.Dialer{
            Timeout:   30 * time.Second,
            KeepAlive: 30 * time.Second,
            DualStack: true,
        }).DialContext,
        MaxIdleConns:          100,
        IdleConnTimeout:       90 * time.Second,
        TLSHandshakeTimeout:   10 * time.Second,
        ExpectContinueTimeout: 1 * time.Second,
        TLSClientConfig:       &tls.Config{
        // your config here...
        },
    },
}
trino.RegisterCustomClient("foobar", foobarClient)
db, err := sql.Open("trino", "https://user@localhost:8080?custom_client=foobar")
```

A custom client can also be used to add OpenTelemetry instrumentation. The
[otelhttp](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp)
package provides a transport wrapper that creates spans for HTTP requests and
propagates the trace ID in HTTP headers:

```go
otelClient := &http.Client{
    Transport: otelhttp.NewTransport(http.DefaultTransport),
}
trino.RegisterCustomClient("otel", otelClient)
db, err := sql.Open("trino", "https://user@localhost:8080?custom_client=otel")
```

#### Examples

```
http://user@localhost:8080?source=hello&catalog=default&schema=foobar
```

```
https://user@localhost:8443?session_properties=query_max_run_time=10m,query_priority=2
```

## Data types

### Query arguments

When passing arguments to queries, the driver supports the following Go data
types:
* integers
* `bool`
* `string`
* slices
* `trino.Numeric` - a string representation of a number
* `time.Time` - passed to Trino as a timestamp with a time zone
* the result of `trino.Date(year, month, day)` - passed to Trino as a date
* the result of `trino.Time(hour, minute, second, nanosecond)` - passed to
  Trino as a time without a time zone
* the result of `trino.TimeTz(hour, minute, second, nanosecond, location)` -
  passed to Trino as a time with a time zone
* the result of `trino.Timestamp(year, month, day, hour, minute, second,
  nanosecond)` - passed to Trino as a timestamp without a time zone
* `time.Duration` - passed to Trino as an interval day to second. Because Trino does not support nanosecond precision for intervals, if the nanosecond part of the value is not zero, an error will be returned.

It's not yet possible to pass:
* `float32` or `float64`
* `byte`
* `json.RawMessage`
* maps

To use the unsupported types, pass them as strings and use casts in the query,
like so:
```sql
SELECT * FROM table WHERE col_double = cast(? AS DOUBLE) OR col_timestamp = CAST(? AS TIMESTAMP)
```

### Response rows

When reading response rows, the driver supports most Trino data types, except:
* time and timestamps with precision - all time types are returned as
  `time.Time`. All precisions up to nanoseconds (`TIMESTAMP(9)` or `TIME(9)`)
  are supported (since this is the maximum precision Golang's `time.Time`
  supports). If a query returns columns defined with a greater precision,
  values are trimmed to 9 decimal digits. Use `CAST` to reduce the returned
  precision, or convert the value to a string that then can be parsed manually.
* `DECIMAL` - returned as string
* `IPADDRESS` - returned as string
* `INTERVAL YEAR TO MONTH` and `INTERVAL DAY TO SECOND` - returned as string
* `UUID` - returned as string

Data types like `HyperLogLog`, `SetDigest`, `QDigest`, and `TDigest` are not
supported and cannot be returned from a query.

For reading nullable columns, use:
* `trino.NullTime`
* `trino.NullMap` - which stores a map of `map[string]interface{}`
or similar structs from the `database/sql` package, like `sql.NullInt64`

To read query results containing arrays or maps, pass one of the following
structs to the `Scan()` function:

* `trino.NullSliceBool`
* `trino.NullSliceString`
* `trino.NullSliceInt64`
* `trino.NullSliceFloat64`
* `trino.NullSliceTime`
* `trino.NullSliceMap`

For two or three dimensional arrays, use `trino.NullSlice2Bool` and
`trino.NullSlice3Bool` or equivalents for other data types.

To read `ROW` values, implement the `sql.Scanner` interface in a struct. Its
`Scan()` function receives a `[]interface{}` slice, with values of the
following types:
* `bool`
* `json.Number` for any numeric Trino types
* `[]interface{}` for Trino arrays
* `map[string]interface{}` for Trino maps
* `string` for other Trino types, as character, date, time, or timestamp

## License

Apache License V2.0, as described in the [LICENSE](./LICENSE) file.

## Build

You can build the client code locally and run tests with the following command:

```
go test -v -race -timeout 2m ./...
```

## Contributing

For contributing, development, and release guidelines, see
[CONTRIBUTING.md](./CONTRIBUTING.md).
