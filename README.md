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
* Transactions through `database/sql`, with isolation levels and read-only mode
* Supports conversion from Trino to native Go data types
  * `string`, `sql.NullString`
  * `int64`, `sql.NullInt64`
  * `float64`, `sql.NullFloat64`
  * `map`, `trino.NullMap`
  * `time.Time`, `trino.NullTime`
  * Up to 3-dimensional arrays to Go slices, of any supported type

## Requirements

* Go 1.25.5 or newer
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
This driver supports forwarding authorization headers by adding a
[NamedArg](https://godoc.org/database/sql#NamedArg) with the name `accessToken`
(e.g., `accessToken=<your_access_token>`) and setting the
`ForwardAuthorizationHeader` field in the
[Config](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config)
struct to `true`.

When enabled, this configuration will override the `AccessToken` set in the
`Config` struct.

Using the `accessToken` named argument without enabling
`ForwardAuthorizationHeader` returns an error, so the token is never sent as
part of the query text, where Trino would persist it in the query history.


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

Trino 426 and newer also let a session switch its authorization user with `SET
SESSION AUTHORIZATION <user>`, reverted with `RESET SESSION AUTHORIZATION`.
Statements after the switch run as the new user until the reset. The switch is
held by the underlying connection, and `database/sql` resets a connection when
it takes it from the pool, so run the switch and every statement that should
run as the new user on one [`sql.Conn`](https://godoc.org/database/sql#Conn):

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

if _, err := conn.ExecContext(ctx, "SET SESSION AUTHORIZATION bob"); err != nil {
	return err
}
// Runs as bob. A db.Query here would run as the DSN user instead.
rows, err := conn.QueryContext(ctx, "SELECT current_user")
```

While the switch is active, the roles from the `roles` parameter and any role
selected with `SET ROLE` are not sent; they apply again after the reset. Trino
rejects both statements inside a transaction.

#### Query id and progress

`database/sql` has no way to expose the Trino query id or the statistics the
coordinator reports while a query runs. The driver makes them available through
a callback passed as a pair of
[NamedArg](https://godoc.org/database/sql#NamedArg) query parameters:

* `X-Trino-Progress-Callback` - a value implementing the `trino.ProgressUpdater`
  interface
* `X-Trino-Progress-Callback-Period` - a `time.Duration` limiting how often the
  callback is invoked while the query state does not change

Both must be set together. The callback receives a `trino.QueryProgressInfo`
with the `QueryId` and the current query statistics. It is invoked when the
query is submitted, when a result page is received, and once more when the
query finishes.

Example:

```go
type queryLogger struct{}

func (queryLogger) Update(info trino.QueryProgressInfo) {
    log.Printf("query %s is %s, %.0f%% done", info.QueryId, info.QueryStats.State, info.QueryStats.ProgressPercentage)
}

rows, err := db.Query("SELECT * FROM foobar",
    sql.Named("X-Trino-Progress-Callback", queryLogger{}),
    sql.Named("X-Trino-Progress-Callback-Period", time.Second),
)
```

Like `X-Trino-User`, these parameters are consumed by the driver and are not
passed to the query.

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
Valid values:   semicolon-separated list of key:value session properties
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

The default client does not follow HTTP redirects, because a redirect would
carry the `X-Trino-*` headers, including extra credentials, to a host other
than the one in the DSN, and a `301`, `302` or `303` response would turn the
statement `POST` into a `GET` without the query. A redirect response fails the
query instead. A custom client follows its own `CheckRedirect` policy, which
allows redirects unless set.

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

##### `query_timeout`

```
Type:           time.Duration
Valid values:   duration string
Default:        nil
```

The `query_timeout` parameter sets a timeout for the query. If the query takes
longer than the timeout, it will be cancelled. If it is not set the default
context timeout will be used.

##### `heartbeat_interval`

```
Type:           time.Duration
Valid values:   positive duration string (e.g. 30s, 1m)
Default:        unset (client uses 30s between spooling heartbeats)
```

The `heartbeat_interval` parameter sets how often the client sends a **HEAD**
heartbeat to the current `nextUri` while a **spooled** query is in progress. It
applies to the whole connection (same idea as session-scoped client settings in
other Trino clients). Only used when the server uses the spooling protocol.

##### `explicitPrepare`

```
Type:           string
Valid values:   "true", "false"
Default:        "true"
```

The `explicitPrepare` parameter controls how queries are sent to the Trino
server. When set to `false`, the client uses `EXECUTE IMMEDIATE` which sends
the query text in the HTTP request body instead of HTTP headers. This allows
sending large query text that would otherwise exceed HTTP header size limits.
When set to `true` (default), queries use explicit prepared statements sent via
HTTP headers.

##### `clientTags`

```
Type:           string
Valid values:   comma-separated list of tags (e.g. tag1,tag2)
Default:        empty
```

The `clientTags` parameter is optional and is used to identify Trino resource
groups. This helps with query tracking and resource management in Trino
clusters.

**DSN parameter example:**
```
clientTags=tag1,tag2
```

**Config struct example:**
```go
config := &Config{
    ServerURI:  "http://foobar@localhost:8080",
    ClientTags: []string{"tag1", "tag2", "tag3"},
}

dsn, err := config.FormatDSN()
```

**Query parameter example (overrides DSN client tags):**
```go
rows, err := db.Query(query, sql.Named("X-Trino-Client-Tags", "tag1,tag2,tag3"))
```

##### `trace_token`

```
Type:           string
Valid values:   any string
Default:        empty
```

The `trace_token` parameter is sent as the `X-Trino-Trace-Token` header and is
recorded by the coordinator, so queries made through the connection can be
correlated with the server logs and event listeners.

##### `client_info`

```
Type:           string
Valid values:   any string
Default:        empty
```

The `client_info` parameter is sent as the `X-Trino-Client-Info` header. It is
free-form metadata about the client, shown in the Trino web UI and passed to
event listeners.

##### `language`

```
Type:           string
Valid values:   a language tag, e.g. en-US
Default:        empty (the server default)
```

The `language` parameter is sent as the `X-Trino-Language` header and selects
the locale used by locale-sensitive functions.

##### `timezone`

```
Type:           string
Valid values:   an IANA time zone name (e.g. Europe/Warsaw), UTC, or an offset
                like +05:30
Default:        the local time zone of the client
```

The `timezone` parameter is sent as the `X-Trino-Time-Zone` header. The server
uses it to evaluate `current_timestamp`, `now()`, `date_trunc` and every other
function that depends on the session time zone, and the driver uses the same
zone to read `DATE`, `TIME` and `TIMESTAMP` values that do not carry a zone of
their own. Without the parameter the local zone is used, taken from the `TZ`
environment variable or from `/etc/localtime`; when neither names a zone, the
current UTC offset is sent instead. Set the parameter explicitly to make the
results independent of where the client runs, for example `timezone=UTC`.

Running `SET TIME ZONE` changes the zone for the rest of the session, and the
driver follows it when reading values.

**DSN parameter example:**
```
timezone=Asia%2FTokyo
```

**Config struct example:**
```go
config := &Config{
    ServerURI: "http://foobar@localhost:8080",
    TimeZone:  "Asia/Tokyo",
}

dsn, err := config.FormatDSN()
```

**Query parameter example (overrides the connection time zone):**
```go
rows, err := db.Query(query, sql.Named("X-Trino-Time-Zone", "Asia/Tokyo"))
```

#### `roles`

```
Type:           string
Format:         roles=catalog1:role1;catalog2=role2
Valid values:   A semicolon-separated list of catalog-to-role assignments,
                where each assignment maps a catalog to a role.
Default:        empty
```
The roles parameter defines authorization roles to assume for one or more catalogs during the Trino session.
A role selected with `SET ROLE` lasts until `database/sql` resets the
connection for its next caller, and no roles are sent while a `SET SESSION AUTHORIZATION` switch is active.

##### Example
``` go
c := &Config{
	ServerURI:         "https://foobar@localhost:8090",
	SessionProperties: map[string]string{"query_priority": "1"},
	Roles:             map[string]string{"catalog1": "role1", "catalog2": "role2"},
}

dsn, err := c.FormatDSN()
```

**Query parameter example (overrides DSN roles):**
```go
rows, err := db.Query(
    query,
    sql.Named("X-Trino-Role", map[string]string{
        "catalog1": "role1",
        "catalog2": "role2",
    }),
)
```

#### Examples

```
http://user@localhost:8080?source=hello&catalog=default&schema=foobar
```

```
https://user@localhost:8443?session_properties=query_max_run_time:10m;query_priority:2
```


```
http://user@localhost:8080?source=hello&catalog=default&schema=foobar&roles=catalog1:role1;catalog2:role2
```

## Data types

### Query arguments

When passing arguments to queries, the driver supports the following Go data
types:
* integers
* `float32` and `float64` - passed to Trino as `REAL` and `DOUBLE` literals
* `bool`
* `string`
* `[]byte`
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
* `time.Duration` - passed to Trino as an interval day to second. Because Trino
  does not support nanosecond precision for intervals, if the nanosecond part
  of the value is not zero, an error will be returned.

It's not yet possible to pass:
* `byte`
* `json.RawMessage`
* maps

To use the unsupported types, pass them as strings and use casts in the query,
like so:
```sql
SELECT *
FROM table
WHERE col_json = CAST(? AS JSON) OR col_timestamp = CAST(? AS TIMESTAMP)
```

### Response rows

When reading response rows, the driver supports most Trino data types, except:
* time and timestamps with precision - all time types are returned as
  `time.Time`. All precisions up to nanoseconds (`TIMESTAMP(9)` or `TIME(9)`)
  are supported (since this is the maximum precision Golang's `time.Time`
  supports). If a query returns columns defined with a greater precision,
  values are trimmed to 9 decimal digits. Use `CAST` to reduce the returned
  precision, or convert the value to a string that then can be parsed manually.
* `DATE`, `TIME` and `TIMESTAMP` without a time zone - returned as `time.Time`
  in the zone of the connection (see the `timezone` parameter), which is the
  zone the server used to produce them. Arrays of these types are scanned with
  `trino.NullSliceTime` and its 2D and 3D variants; set their `Location` field
  to the same zone, as they use `time.Local` by default.
* `DECIMAL` and `NUMBER` (Trino 480+) - returned as string; use
  `sql.NullString` for nullable columns
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
* `string` for other Trino types, as character, date, time, or timestamp.

> [!NOTE]
> `VARBINARY` columns are returned as base64-encoded strings when used within
> `ROW`, `MAP`, or `ARRAY` values.

## Transactions

Use `db.Begin` or `db.BeginTx` to run several statements in a single Trino
transaction:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer tx.Rollback()

if _, err := tx.ExecContext(ctx, "DELETE FROM reports WHERE day = DATE '2025-01-01'"); err != nil {
	return err
}
if _, err := tx.ExecContext(ctx, "INSERT INTO reports SELECT * FROM staging"); err != nil {
	return err
}
return tx.Commit()
```

`sql.TxOptions` is honoured. `Isolation` accepts `sql.LevelDefault`,
`sql.LevelReadUncommitted`, `sql.LevelReadCommitted`, `sql.LevelRepeatableRead`
and `sql.LevelSerializable`; any other level is rejected. `ReadOnly` starts a
`READ ONLY` transaction.

A statement that fails aborts the whole transaction on the server. `Rollback`
reports success in that case, while `Commit` returns an error.

Transaction control statements must go through the returned `*sql.Tx`. Sending
them directly, as in `db.Exec("START TRANSACTION")`, is rejected.

> [!NOTE]
> Transaction support depends on the connector. Connectors that only support
> writes in autocommit mode, such as `memory`, reject writes inside a
> multi-statement transaction, and the isolation levels a connector accepts
> vary.

## Spooling Protocol

The client supports the [Trino spooling
protocol](https://trino.io/docs/current/client/client-protocol.html#spooling-protocol),
which enables efficient retrieval of large result sets by downloading data in
segments, optionally in parallel and out-of-order.

If the Trino server has the spooling protocol enabled, the client will use it
by default with the `json` encoding.

While a spooled query is in progress, the client sends periodic **HEAD**
requests to the current `nextUri` (same URL used for result pages) so the
coordinator treats the client as still active, which helps avoid query
abandonment when result consumption is slow. The server must support this
endpoint (Trino 475+).

You can configure other encodings:

- Supported encodings: `json`, `json+lz4`, `json+zstd`

```go
rows, err := db.Query(query, sql.Named("encoding", "json+zstd"))
```

Or specify a list of supported encodings in order of preference:

```go
rows, err := db.Query(query, sql.Named("encoding", "json+zstd, json+lz4, json"))
```

### Configuration Options

You can tune the spooling protocol using the following parameters, passed as
`sql.Named` arguments to your query:

- **Spooling Worker Count**
  `sql.Named("spooling_worker_count", "N")`
  Sets the number of parallel workers used to download spooled segments.
  **Default:** `5`
  **Considerations:**
  - Increasing this value can improve throughput for large result sets,
    especially on high-latency networks.
  - Higher values increase parallelism but may also increase memory usage.

- **Max Out-of-Order Segments**
  `sql.Named("max_out_of_order_segments", "N")`
  Sets the maximum number of segments that can be downloaded and buffered
  out-of-order before blocking further downloads.
  **Default:** `10`
  **Considerations:**
  - Higher values increase the potential memory usage, but actual usage depends
    on download behavior and may be lower in practice.
  - Higher values reduce the chance that one slow or stalled segment will block
    the download of additional segments.
  - Lower values reduce memory usage but may limit parallelism and throughput.

**Note:**
It is **not allowed** to set `spooling_worker_count` higher than
`max_out_of_order_segments` — doing so will result in an error.

Each download worker must reserve a slot for the segment it fetches, and a slot
is only released when that segment can be processed in order. The total number
of slots corresponds to `max_out_of_order_segments`. If you configure more
workers than allowed out-of-order segments, the extra workers would immediately
block while waiting for a slot — defeating the purpose of parallelism and
potentially wasting resources.

#### Example: Customizing Spooling Parameters

```go
rows, err := db.Query(
    query,
    sql.Named("encoding", "json+zstd"),
    sql.Named("spooling_worker_count", "8"),
    sql.Named("max_out_of_order_segments", "20"),
)
```

## License

Apache License V2.0, as described in the [LICENSE](./LICENSE) file.

## Build

You can build the client code locally and run the unit tests, which need no
server, with the following command:

```
go test -v -race ./...
```

The integration tests, which start Trino in Docker, live in the `integration`
module. See [CONTRIBUTING.md](./CONTRIBUTING.md) for how to run them.

## Contributing

For contributing, development, and release guidelines, see
[CONTRIBUTING.md](./CONTRIBUTING.md).
