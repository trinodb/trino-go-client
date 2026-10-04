# Trino Go client

A [Trino](https://trino.io) client for the [Go](https://golang.org) programming
language. It is a driver for the standard `database/sql` package: your program
opens a `*sql.DB` and sends SQL statements to Trino over HTTP or HTTPS, and
the driver turns the results into Go values.

The driver authenticates with HTTP Basic credentials, Kerberos, JSON web
tokens or OAuth2, reads results through the spooling protocol when the server
offers it, runs transactions, and converts every Trino type, including nested
`ARRAY`, `MAP`, `ROW` and `VARIANT` values, into Go values.

[![Build Status](https://github.com/trinodb/trino-go-client/workflows/ci/badge.svg)](https://github.com/trinodb/trino-go-client/actions?query=workflow%3Aci+event%3Apush+branch%3Amaster)
[![GoDoc](https://godoc.org/github.com/trinodb/trino-go-client?status.svg)](https://godoc.org/github.com/trinodb/trino-go-client)

## Requirements

* Go 1.25.5 or newer
* Trino 372 or newer

## Installation

```bash
go get github.com/trinodb/trino-go-client/trino
```

## Quick start

Import the package for its side effect of registering the `trino` driver, then
open a database with a [DSN](#dsn):

```go
import (
	"database/sql"

	_ "github.com/trinodb/trino-go-client/trino"
)

db, err := sql.Open("trino", "http://user@localhost:8080?catalog=tpch&schema=sf1")
if err != nil {
	return err
}
defer db.Close()

rows, err := db.QueryContext(ctx, "SELECT name, regionkey FROM nation WHERE nationkey < ?", 5)
if err != nil {
	return err
}
defer rows.Close()
for rows.Next() {
	var name string
	var regionKey int64
	if err := rows.Scan(&name, &regionKey); err != nil {
		return err
	}
}
return rows.Err()
```

`sql.Open` returns an error for a DSN that does not parse, and does not
contact the server. Settings that a DSN cannot carry, such as an
`http.Client`, go in a [`trino.Config`](#config-and-connector) passed to
`trino.NewConnector` and `sql.OpenDB`.

Every request the driver sends carries a `User-Agent` such as
`trino-go-client/v0.333.0 os=linux arch=amd64 lang/go=go1.25.5`, with the
driver version taken from the module build information (`unknown` when it is
not available).

## Connecting

### DSN

The Data Source Name is a URL with a mandatory username, an optional password,
an optional catalog and schema path, and optional query string parameters:

```
http[s]://user[:pass]@host[:port][/catalog[/schema]][?parameters]
```

The path sets the catalog, or the catalog and schema, with the same syntax as
a [JDBC URL](https://trino.io/docs/current/client/jdbc.html#connecting):
`http://user@localhost:8080/tpch/sf1` is the same as
`http://user@localhost:8080?catalog=tpch&schema=sf1`. An empty path or a lone
`/` sets neither, and one trailing slash is ignored. A path with more than two
segments, or with an empty catalog or schema such as `//sf1`, is an error. Set
each of the catalog and schema either in the path or with the `catalog` and
`schema` parameters, not both: `/tpch?schema=sf1` is valid, but
`/tpch?catalog=tpch` is an error. The path is not a prefix for a server behind
a reverse proxy; requests always go to `/v1/statement` on the host.

A password enables [HTTP Basic authentication](#http-basic-authentication)
and requires HTTPS. The [parameters](#parameters) are case-sensitive. Values
with characters that have a meaning in a URL, such as `/`, `:` or `;`, must be
URL-encoded, which
[`Config.FormatDSN`](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config.FormatDSN)
does for you:

```go
dsn, err := (&trino.Config{
	ServerURI:         "https://user@localhost:8443",
	Catalog:           "tpch",
	SessionProperties: map[string]string{"query_max_run_time": "10m"},
}).FormatDSN()
```

Examples:

```
http://user@localhost:8080?source=hello&catalog=default&schema=foobar
https://user:secret@localhost:8443?session_properties=query_max_run_time:10m;query_priority:2
http://user@localhost:8080/tpch/sf1?roles=catalog1:role1;catalog2:role2
```

### Config and Connector

Every DSN parameter has a field on
[`trino.Config`](https://godoc.org/github.com/trinodb/trino-go-client/trino#Config),
and a few settings exist only there, because a string cannot carry them: an
`http.Client`, a logged-in Kerberos client, and the callbacks of external
authentication. Pass the `Config` to `trino.NewConnector` and open the
database with
[`sql.OpenDB`](https://pkg.go.dev/database/sql#OpenDB):

```go
connector, err := trino.NewConnector(&trino.Config{
	ServerURI:  "https://user@localhost:8443",
	Catalog:    "default",
	HTTPClient: &http.Client{Timeout: time.Minute},
})
if err != nil {
	return err
}
db := sql.OpenDB(connector)
```

`NewConnector` validates the `Config` and copies it, so later changes to it
have no effect. `Config.ServerURI` must not have a path; set `Config.Catalog`
and `Config.Schema` instead.

`HTTPClient` is used for every request. Its transport decides how to reach and
verify the coordinator, so it cannot be combined with `custom_client`, the
certificate parameters, `SSLVerification`, `httpProxy` or `socksProxy`;
`Config.TLSConfig` builds the `tls.Config` those settings would produce, for
the client's own transport. As with the default client, the driver does not
follow redirects with `HTTPClient`, except for [spooled segment
downloads](#spooling-protocol). `Config.FormatDSN` returns an error when a
field that a DSN cannot carry is set.

### Parameters

| Parameter | `Config` field | Default | Purpose |
|---|---|---|---|
| [`source`](#source) | `Source` | empty | Name of the application, for query tracking |
| [`catalog`](#catalog) | `Catalog` | empty | Default catalog |
| [`schema`](#schema) | `Schema` | empty | Default schema |
| [`session_properties`](#session_properties) | `SessionProperties` | empty | Session properties |
| [`extra_credentials`](#extra_credentials) | `ExtraCredentials` | empty | Credentials for connectors |
| [`roles`](#roles) | `Roles` | empty | Catalog roles |
| [`clientTags`](#clienttags) | `ClientTags` | empty | Tags for resource groups |
| [`resourceEstimates`](#resourceestimates) | `ResourceEstimates` | empty | Expected resource usage, for resource groups |
| [`trace_token`](#trace_token) | `TraceToken` | empty | Correlation token for the server logs |
| [`client_info`](#client_info) | `ClientInfo` | empty | Free-form description of the client |
| [`language`](#language) | `Language` | server default | Locale for locale-sensitive functions |
| [`timezone`](#timezone) | `TimeZone` | local zone | Session time zone |
| [`query_timeout`](#query_timeout) | `QueryTimeout` | 10h | Timeout for queries without a context deadline |
| [`request_retry_timeout`](#request_retry_timeout) | `RequestRetryTimeout` | 2m | How long one HTTP request is retried |
| [`request_retry_max_attempts`](#request_retry_max_attempts) | `RequestRetryMaxAttempts` | 20 | How many times one HTTP request is sent |
| [`heartbeat_interval`](#heartbeat_interval) | `HeartbeatInterval` | 30s | Spooling heartbeat interval |
| [`explicitPrepare`](#explicitprepare) | `DisableExplicitPrepare` | `true` | Prepared statements through headers or `EXECUTE IMMEDIATE` |
| [`accessToken`](#accesstoken) | `AccessToken` | empty | JWT sent as a bearer token |
| [`forwardAuthorizationHeader`](#forwardauthorizationheader) | `ForwardAuthorizationHeader` | `false` | Allow a per-query `accessToken` argument |
| [`externalAuthentication`](#externalauthentication) | `ExternalAuthentication` | `false` | Log in through the browser |
| [`externalAuthenticationTimeout`](#externalauthenticationtimeout) | `ExternalAuthenticationTimeout` | 2m | How long to wait for the login |
| [`KerberosEnabled`](#kerberosenabled) | `KerberosEnabled` | `false` | Kerberos authentication |
| [`KerberosKeytabPath`](#kerberoskeytabpath) | `KerberosKeytabPath` | empty | Keytab to log in with |
| [`KerberosCredentialCachePath`](#kerberoscredentialcachepath) | `KerberosCredentialCachePath` | `KRB5CCNAME` | Credential cache to reuse a ticket from |
| [`KerberosPrincipal`](#kerberosprincipal) | `KerberosPrincipal` | empty | Principal to authenticate as |
| [`KerberosRealm`](#kerberosrealm) | `KerberosRealm` | empty | Realm of the principal |
| [`KerberosConfigPath`](#kerberosconfigpath) | `KerberosConfigPath` | empty | krb5 configuration file |
| [`KerberosRemoteServiceName`](#kerberosremoteservicename) | `KerberosRemoteServiceName` | `trino` | Service name of the coordinator |
| [`KerberosServicePrincipalPattern`](#kerberosserviceprincipalpattern) | `KerberosServicePrincipalPattern` | `${SERVICE}@${HOST}` | Service principal of the coordinator |
| [`KerberosUseCanonicalHostname`](#kerberosusecanonicalhostname) | `KerberosDisableCanonicalHostname` | `true` | Resolve the coordinator host before building the principal |
| [`SSLCertPath`, `SSLCert`](#sslcertpath--sslcert) | `SSLCertPath`, `SSLCert` | system trust store | Additional CA certificate |
| [`SSLClientCertPath`, `SSLClientCert`, `SSLClientKeyPath`, `SSLClientKey`](#sslclientcertpath--sslclientcert-and-sslclientkeypath--sslclientkey) | same names | empty | Client certificate for mutual TLS |
| [`SSLVerification`](#sslverification) | `SSLVerification` | `FULL` | How the server certificate is verified |
| [`httpProxy`, `socksProxy`](#httpproxy--socksproxy) | `HTTPProxy`, `SOCKSProxy` | environment | Proxy for every request |
| [`custom_client`](#custom_client) | `CustomClientName` | empty | A registered `http.Client` |

#### `source`

```
Type:           string
Valid values:   string describing the source of the connection to Trino
Default:        empty
```

The `source` parameter is sent as the `X-Trino-Source` header and names the
application in the Trino web UI, the query history and event listeners, so
administrators can trace a query back to the program that ran it.

#### `catalog`

```
Type:           string
Valid values:   the name of a catalog configured in the Trino server
Default:        empty
```

The `catalog` parameter defines the Trino catalog where schemas exist to
organize tables. It can also be set as the first segment of the DSN path.

#### `schema`

```
Type:           string
Valid values:   the name of an existing schema in the catalog
Default:        empty
```

The `schema` parameter defines the Trino schema where tables exist. This is
also known as namespace in some environments. It can also be set as the
second segment of the DSN path, after the catalog.

#### `session_properties`

```
Type:           string
Valid values:   semicolon-separated list of key:value session properties
Default:        empty
```

The `session_properties` parameter must contain valid parameters accepted by
the Trino server. Run `SHOW SESSION` in Trino to get the current list.
Property names, like `extra_credentials` and `roles` keys, must be non-empty
printable ASCII without spaces, `=` or `,`; values are URL-encoded, so they
may contain `=` and `,`. `SET SESSION` changes a property for the rest of the
session, see [Session state and the connection
pool](#session-state-and-the-connection-pool).

#### `extra_credentials`

```
Type:           string
Valid values:   semicolon-separated list of key:value pairs
Default:        empty
```

The `extra_credentials` parameter passes credentials to connectors that
authenticate to the underlying system with the user's identity, as described
for each connector in the Trino documentation. Each pair is sent as an
`X-Trino-Extra-Credential` header with the statement request only, not with
the requests that fetch result pages. Keys follow the same rules as session
property names.

#### `roles`

```
Type:           string
Valid values:   semicolon-separated list of catalog:role pairs
Default:        empty
```

The `roles` parameter selects a role to assume in each listed catalog for the
whole session. A role selected with `SET ROLE` lasts until `database/sql`
resets the connection for its next caller, and no roles are sent while a `SET
SESSION AUTHORIZATION` switch is active. Roles can also be set for one query
with the `X-Trino-Role` [named argument](#per-query-settings).

```go
c := &trino.Config{
	ServerURI: "https://foobar@localhost:8443",
	Roles:     map[string]string{"catalog1": "role1", "catalog2": "role2"},
}
dsn, err := c.FormatDSN()
```

#### `clientTags`

```
Type:           string
Valid values:   comma-separated list of tags (e.g. tag1,tag2)
Default:        empty
```

The `clientTags` parameter is sent as the `X-Trino-Client-Tags` header. Resource
group selectors can match on the tags, so they route queries to resource groups
and help with query tracking. A tag cannot contain `,`. The `X-Trino-Client-Tags`
[named argument](#per-query-settings) overrides the tags for one query.

```go
config := &trino.Config{
	ServerURI:  "http://foobar@localhost:8080",
	ClientTags: []string{"tag1", "tag2", "tag3"},
}
dsn, err := config.FormatDSN()
```

#### `resourceEstimates`

```
Type:           string
Valid values:   semicolon-separated list of resource:estimate pairs
Default:        empty
```

The `resourceEstimates` parameter tells the coordinator how much each query is
expected to use, and is sent as one `X-Trino-Resource-Estimate` header per
estimate with every statement. Resource group selectors can match on these
estimates to route queries, for example to keep long or memory-hungry queries
out of an interactive group. The server accepts these resources, ignoring case:

- `EXECUTION_TIME` and `CPU_TIME`, a duration such as `90s`, `10m` or `1.5h`
  (units `ns`, `us`, `ms`, `s`, `m`, `h`, `d`)
- `PEAK_MEMORY`, a data size such as `512MB` or `1.5GB` (units `B`, `kB`,
  `MB`, `GB`, `TB`, `PB`, all powers of 1024)

The values are sent as given. The server rejects a query with an unknown
resource or a value it cannot parse, so a Go `time.Duration` must not be passed
through `String()`, which produces `1h30m0s`. Use `trino.FormatDuration` and
`trino.FormatDataSize` instead, which produce values the server accepts, such
as `1.50h` and `1.50GB`. They use the largest unit that represents the value
exactly with two decimal places, so they never round: `time.Hour + time.Second`
becomes `3601.00s`. The driver rejects names that are empty, contain `=` or
`,`, or contain characters other than printable ASCII. The
`X-Trino-Resource-Estimate` [named argument](#per-query-settings) adds to or
replaces the estimates for one query.

```go
config := &trino.Config{
	ServerURI: "http://foobar@localhost:8080",
	ResourceEstimates: map[string]string{
		"EXECUTION_TIME": "10m",
		"CPU_TIME":       trino.FormatDuration(90 * time.Minute), // 1.50h
		"PEAK_MEMORY":    trino.FormatDataSize(1 << 30),          // 1GB
	},
}
dsn, err := config.FormatDSN()
```

#### `trace_token`

```
Type:           string
Valid values:   any string
Default:        empty
```

The `trace_token` parameter is sent as the `X-Trino-Trace-Token` header and is
recorded by the coordinator, so queries made through the connection can be
correlated with the server logs and event listeners.

#### `client_info`

```
Type:           string
Valid values:   any string
Default:        empty
```

The `client_info` parameter is sent as the `X-Trino-Client-Info` header. It is
free-form metadata about the client, shown in the Trino web UI and passed to
event listeners.

#### `language`

```
Type:           string
Valid values:   a language tag, e.g. en-US
Default:        empty (the server default)
```

The `language` parameter is sent as the `X-Trino-Language` header and selects
the locale used by locale-sensitive functions.

#### `timezone`

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
driver follows it when reading values. The `X-Trino-Time-Zone` [named
argument](#per-query-settings) overrides the zone for one query.

```go
config := &trino.Config{
	ServerURI: "http://foobar@localhost:8080",
	TimeZone:  "Asia/Tokyo",
}
dsn, err := config.FormatDSN()
```

#### `query_timeout`

```
Type:           time.Duration
Valid values:   duration string (e.g. 30s, 5m)
Default:        unset (10h, trino.DefaultQueryTimeout, for queries whose
                context has no deadline)
```

The `query_timeout` parameter bounds how long a query may run before the driver
cancels it on the server and returns an error. When set, it applies to every
query, even one whose context has a deadline. When unset, the context deadline
applies, and a context without one gets `trino.DefaultQueryTimeout`. See
[Cancellation and timeouts](#cancellation-and-timeouts).

#### `request_retry_timeout`

```
Type:           time.Duration
Valid values:   duration string
Default:        2m (trino.DefaultRequestRetryTimeout)
```

The `request_retry_timeout` parameter sets how long a single HTTP request is
retried on a `429`, `502`, `503` or `504` response or a network error before
the query fails. Only idempotent requests are retried on network errors after
the connection is established. A `Retry-After` header on a `429` or `503`
response, in seconds or as an HTTP date, sets the wait before the next attempt
instead of the default backoff; the wait never extends past this timeout. The
value must be positive.

#### `request_retry_max_attempts`

```
Type:           int
Valid values:   positive integer
Default:        20 (trino.DefaultRequestRetryMaxAttempts)
```

The `request_retry_max_attempts` parameter sets how many times a single HTTP
request is sent before the query fails. Retrying stops at whichever of
`request_retry_timeout` and `request_retry_max_attempts` is reached first; with
the defaults, the timeout is reached first.

#### `heartbeat_interval`

```
Type:           time.Duration
Valid values:   positive duration string (e.g. 30s, 1m)
Default:        unset (30s)
```

The `heartbeat_interval` parameter sets how often the driver sends a `HEAD`
heartbeat to the current `nextUri` while a spooled query is in progress, so
the coordinator does not abandon a query whose results are consumed slowly.
It applies to the whole connection and is only used with the [spooling
protocol](#spooling-protocol).

#### `explicitPrepare`

```
Type:           bool
Valid values:   true, false
Default:        true
```

The `explicitPrepare` parameter controls how statements with arguments reach
the server. With `true`, the driver prepares the statement through the
`X-Trino-Prepared-Statement` header and runs `EXECUTE`. With `false`, it runs
`EXECUTE IMMEDIATE`, which carries the statement text in the request body
instead of a header, so statements larger than the server's header size limit
still work. The `Config` field is `DisableExplicitPrepare`, whose zero value
keeps explicit prepare on.

#### `accessToken`

```
Type:           string
Valid values:   a JSON web token
Default:        empty
```

The `accessToken` parameter is sent as a bearer token in the `Authorization`
header of every request, see [JSON web token
authentication](#json-web-token-authentication).

#### `forwardAuthorizationHeader`

```
Type:           bool
Valid values:   true, false
Default:        false
```

The `forwardAuthorizationHeader` parameter allows the `accessToken` [named
argument](#per-query-settings) to set the bearer token of one query, see
[Authorization header forwarding](#authorization-header-forwarding).

#### `externalAuthentication`

```
Type:           bool
Valid values:   true, false
Default:        false
```

The `externalAuthentication` parameter enables [external
authentication](#external-authentication). It requires HTTPS.

#### `externalAuthenticationTimeout`

```
Type:           time.Duration
Valid values:   positive duration string (e.g. 5m)
Default:        unset (2m)
```

The `externalAuthenticationTimeout` parameter sets how long the driver waits for
the user to log in.

#### `KerberosEnabled`

```
Type:           bool
Valid values:   true, false
Default:        false
```

The `KerberosEnabled` parameter turns on [Kerberos
authentication](#kerberos-authentication).

#### `KerberosKeytabPath`

```
Type:           string
Valid values:   a filesystem path
Default:        empty (a credential cache is used)
```

The `KerberosKeytabPath` parameter names the keytab the driver logs in with,
as `KerberosPrincipal` in `KerberosRealm`. It cannot be combined with
`KerberosCredentialCachePath`.

#### `KerberosCredentialCachePath`

```
Type:           string
Valid values:   a filesystem path, optionally prefixed with FILE:
Default:        empty (KRB5CCNAME, then /tmp/krb5cc_<uid>, when no keytab is given)
```

A Kerberos credential cache holding a ticket for the user, used instead of a
keytab; it cannot be combined with `KerberosKeytabPath`. Only file caches can
be read: when `KRB5CCNAME` names another type, such as `KEYRING:`, `KCM:` or
macOS's default `API:`, run `kinit -c FILE:/path` and set this parameter. The
cache is read when a connection opens, so connections opened after `kinit`
refreshes it use the new ticket.

#### `KerberosPrincipal`

```
Type:           string
Valid values:   a Kerberos principal name, without the realm
Default:        empty (the principal of the credential cache)
```

The `KerberosPrincipal` parameter names the user to authenticate as. With a
keytab, the driver logs in as this principal. With a credential cache or a
`KerberosClient` it is optional, and when set must match the principal the
cache or client holds.

#### `KerberosRealm`

```
Type:           string
Valid values:   a Kerberos realm
Default:        empty (the realm of the credential cache)
```

The `KerberosRealm` parameter is the realm of `KerberosPrincipal`. With a
credential cache or a `KerberosClient` it must match their realm when set.

#### `KerberosConfigPath`

```
Type:           string
Valid values:   a filesystem path
Default:        empty
```

The `KerberosConfigPath` parameter names the krb5 configuration file, usually
`/etc/krb5.conf`, which lists the KDCs and maps host names to realms.

#### `KerberosRemoteServiceName`

```
Type:           string
Valid values:   a Kerberos service name
Default:        trino
```

The `KerberosRemoteServiceName` parameter is the service name of the
coordinator, substituted for `${SERVICE}` in
[`KerberosServicePrincipalPattern`](#kerberosserviceprincipalpattern).

#### `KerberosServicePrincipalPattern`

```
Type:           string
Valid values:   a service principal with optional ${SERVICE} and ${HOST} placeholders
Default:        ${SERVICE}@${HOST}
```

The service principal of the coordinator. `${SERVICE}` is replaced with
`KerberosRemoteServiceName` and `${HOST}` with the lowercased host of the
request URL. A `service@host` result names the Kerberos principal
`service/host`, so the default asks for a ticket for `trino/<host>`; a result
without `@`, such as `HTTP/${HOST}`, is used as the principal name. The realm
of the service comes from `domain_realm` in the krb5 configuration, or is the
user's realm, and cannot be part of the pattern.

#### `KerberosUseCanonicalHostname`

```
Type:           bool
Valid values:   true, false
Default:        true
```

`${HOST}` in
[`KerberosServicePrincipalPattern`](#kerberosserviceprincipalpattern) is the
canonical name of the coordinator host instead of the host in the URL, for a
coordinator reached through a DNS alias or a load balancer. The host is
resolved to an address, following CNAME records, and the first address is
looked up in reverse DNS; without a reverse record the address itself is used.
`localhost` and loopback addresses are replaced with the canonical name of the
local machine, and requests fail with `Fully qualified name of localhost
should not resolve to 'localhost'` when that resolves to `localhost` too, as
on many laptops. The name is resolved once per connection and host. Set it to
`false`, or set `Config.KerberosDisableCanonicalHostname`, to use the URL host
as is.

Earlier versions of this driver always used the URL host. If you reach the
coordinator through a DNS alias and its service principal is registered for
that alias, set `KerberosUseCanonicalHostname=false` to keep asking for the
same ticket.

#### `SSLCertPath` / `SSLCert`

```
Type:           string
Valid values:   a filesystem path (SSLCertPath) or PEM-encoded certificate content (SSLCert)
Default:        empty (the system trust store is used)
```

An additional CA certificate the driver should trust, as a file path or as
inline PEM content; only one of the pair may be set. Requires HTTPS, and
cannot be combined with `custom_client` or `HTTPClient`, whose transports have
their own TLS configuration.

#### `SSLClientCertPath` / `SSLClientCert` and `SSLClientKeyPath` / `SSLClientKey`

```
Type:           string
Valid values:   a filesystem path (...Path) or PEM-encoded content
Default:        empty (no client certificate is presented)
```

A PEM client certificate and its private key, for TLS client authentication
(mutual TLS). The certificate and the key must each be given as either a path
or inline content, not both, and the certificate and key must be set
together. Requires HTTPS, and cannot be combined with `custom_client` or
`HTTPClient`.

```go
db, err := sql.Open("trino", "https://user@localhost:8443"+
	"?SSLClientCertPath=/path/to/client-cert.pem"+
	"&SSLClientKeyPath=/path/to/client-key.pem")
```

#### `SSLVerification`

```
Type:           string
Valid values:   "FULL", "CA", "NONE"
Default:        "FULL"
```

Controls how the driver validates the coordinator's TLS certificate. `FULL`
validates the certificate chain and the hostname. `CA` validates the
certificate chain but not the hostname. `NONE` disables certificate validation
entirely and must only be used for development, since it also allows a network
attacker to intercept the connection. Requires HTTPS, and cannot be combined
with `custom_client` or `HTTPClient`.

#### `httpProxy` / `socksProxy`

```
Type:           string
Valid values:   host:port
Default:        empty (the proxy environment variables apply)
```

Sends every request through an HTTP proxy (`httpProxy`) or a SOCKS5 proxy
(`socksProxy`). Only one of the two may be set. HTTPS requests reach the
coordinator through an HTTP `CONNECT` tunnel, and the SOCKS5 proxy resolves
the coordinator's host name. Neither can be combined with `custom_client` or
`HTTPClient`; see [Proxy](#proxy).

```go
db, err := sql.Open("trino", "http://user@localhost:8080?socksProxy=localhost:1080")
```

#### `custom_client`

```
Type:           string
Valid values:   the name of a client previously registered to the driver
Default:        empty (defaults to http.DefaultClient)
```

The `custom_client` parameter selects an `http.Client` registered with
`trino.RegisterCustomClient` for every request, see [Custom HTTP
client](#custom-http-client). Its transport decides how to verify the
coordinator and how to reach it, so the driver rejects `SSLCert`,
`SSLCertPath`, the client certificate parameters, `SSLVerification`,
`httpProxy` and `socksProxy` combined with it, instead of ignoring them.

## Authentication

### HTTP Basic authentication

A password in the DSN, or in `Config.ServerURI`, enables HTTP Basic
authentication: the driver sets the `Authorization` header in every request to
Trino. It **requires HTTPS**; a password over `http` is an error.

### Kerberos authentication

Kerberos authentication is enabled with `KerberosEnabled=true` and configured
with the `Kerberos*` [parameters](#parameters), on the DSN or on `Config`. The
coordinator only offers Kerberos authentication over HTTPS.

The driver logs in with the keytab in `KerberosKeytabPath`, or reuses a ticket
from a credential cache, such as the one `kinit` writes. Without a keytab it
reads the cache in `KerberosCredentialCachePath`, then the one named by the
`KRB5CCNAME` environment variable, then `/tmp/krb5cc_<uid>`. The principal and
realm come from the cache; when `KerberosPrincipal` or `KerberosRealm` are set,
they must match it.

```go
db, err := sql.Open("trino", "https://user@localhost:8443"+
	"?KerberosEnabled=true"+
	"&KerberosConfigPath=/etc/krb5.conf"+
	"&KerberosCredentialCachePath=/tmp/krb5cc_1000")
```

A program that already holds a logged-in
[gokrb5](https://pkg.go.dev/github.com/jcmturner/gokrb5/v8/client) client, for
example one that authenticates its own users, can pass it in
`Config.KerberosClient` with `trino.NewConnector`. All connections of the
`Connector` share it and the driver never logs it in, renews or destroys it. It
requires `KerberosEnabled` and cannot be combined with `KerberosKeytabPath` or
`KerberosCredentialCachePath`. When `KerberosPrincipal` or `KerberosRealm` are
set, they must match the client's credentials.

```go
connector, err := trino.NewConnector(&trino.Config{
	ServerURI:       "https://user@localhost:8443",
	KerberosEnabled: true,
	KerberosClient:  kerberosClient,
})
```

The ticket is requested for the service principal that
[`KerberosServicePrincipalPattern`](#kerberosserviceprincipalpattern) and
[`KerberosUseCanonicalHostname`](#kerberosusecanonicalhostname) produce. See
[Kerberos authentication](https://trino.io/docs/current/security/kerberos.html)
in the Trino documentation for the server side.

### JSON web token authentication

A JWT in the [`accessToken`](#accesstoken) parameter, or in `Config.AccessToken`,
is sent as a bearer token in the `Authorization` header of every request. See
[JWT authentication](https://trino.io/docs/current/security/jwt.html) in the
Trino documentation for the server side.

### External authentication

With `externalAuthentication=true`, a coordinator that uses
[OAuth2 authentication](https://trino.io/docs/current/security/oauth2.html)
logs the user in through the browser. When the coordinator rejects a request,
the driver passes the login URL to `Config.RedirectHandler`, waits for the
token until `externalAuthenticationTimeout` (default 2m) passes, and retries
the request with it, so a query running when the token expires continues. The
token is kept in `Config.TokenCache` and shared by all connections of a
`Connector`. By default, `RedirectHandler` is `trino.OpenBrowser`, and the
cache holds the token in memory.

```go
connector, err := trino.NewConnector(&trino.Config{
	ServerURI:              "https://trino.example.com:8443",
	ExternalAuthentication: true,
	RedirectHandler: func(ctx context.Context, redirectURL *url.URL) error {
		fmt.Println("Log in at", redirectURL)
		return nil
	},
})
```

Implement `trino.TokenCache` to keep the token between runs of a program.
External authentication **requires HTTPS**, and cannot be combined with a
password, Kerberos, or `forwardAuthorizationHeader`. An `AccessToken` is sent
until a token is cached.

### OAuth2 client credentials

A service can obtain tokens with the OAuth2 client credentials grant through an
`HTTPClient` from
[golang.org/x/oauth2](https://pkg.go.dev/golang.org/x/oauth2/clientcredentials),
which adds the token to every request and renews it when it expires:

```go
import "golang.org/x/oauth2/clientcredentials"

credentials := clientcredentials.Config{
	ClientID:     "trino-client",
	ClientSecret: secret,
	TokenURL:     "https://idp.example.com/oauth2/token",
}
connector, err := trino.NewConnector(&trino.Config{
	ServerURI:  "https://trino.example.com:8443",
	HTTPClient: credentials.Client(context.Background()),
})
```

### Authorization header forwarding

A program that holds a different token for each of its users can send it with
each query: set `forwardAuthorizationHeader=true`, or
`Config.ForwardAuthorizationHeader`, and pass the token as the `accessToken`
[named argument](#per-query-settings). It overrides the `AccessToken` of the
connection for that query.

```go
rows, err := db.Query("SELECT * FROM foobar", sql.Named("accessToken", userToken))
```

Using the `accessToken` named argument without enabling
`forwardAuthorizationHeader` returns an error, so the token is never sent as
part of the query text, where Trino would persist it in the query history.

### Per-query user and session authorization

A query can run as a user other than the one that authenticated to the
coordinator, when [system access
control](https://trino.io/docs/current/develop/system-access-control.html)
allows the impersonation. Pass the user as the `X-Trino-User` [named
argument](#per-query-settings); it is sent as a header and is not part of the
query:

```go
db.Query("SELECT * FROM foobar WHERE id=?", 1, sql.Named("X-Trino-User", "Alice"))
```

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

## Per-query settings

`database/sql` has no way to pass settings for one query, so the driver reads
them from [`sql.Named`](https://pkg.go.dev/database/sql#Named) arguments. They
are consumed by the driver and never reach the query text, and their position
among the other arguments does not matter. Any argument whose name starts with
`X-Trino-` is sent as a request header with that name, which overrides the
connection's value of that header for the query. The arguments with a special
meaning are:

| Name | Value | Purpose |
|---|---|---|
| `X-Trino-User` | `string` | Run the query as another user, see [Per-query user](#per-query-user-and-session-authorization) |
| `X-Trino-Role` | `map[string]string` | Catalog roles for the query, formatted like [`roles`](#roles) |
| `X-Trino-Resource-Estimate` | `map[string]string` | Estimates added to or replacing the connection's [`resourceEstimates`](#resourceestimates) |
| `X-Trino-Client-Tags` | `string` | Comma-separated tags replacing [`clientTags`](#clienttags) |
| `X-Trino-Time-Zone` | `string` | Time zone replacing [`timezone`](#timezone), also for reading the results |
| `X-Trino-Progress-Callback` | `trino.ProgressUpdater` | Query id and statistics, see [Query id and progress](#query-id-and-progress) |
| `X-Trino-Progress-Callback-Period` | `time.Duration` | How often the callback runs |
| `warnings` | `*trino.Warnings` | Collects the warnings of the query, see [Query warnings](#query-warnings) |
| `accessToken` | `string` | Bearer token for the query, see [Authorization header forwarding](#authorization-header-forwarding) |
| `encoding` | `string` | Spooled result encodings, see [Spooling protocol](#spooling-protocol) |
| `spooling_worker_count` | `string` | Parallel segment downloads, a number |
| `max_out_of_order_segments` | `string` | Buffered out-of-order segments, a number |

### Query id and progress

`database/sql` has no way to expose the Trino query id or the statistics the
coordinator reports while a query runs. The driver makes them available through
a callback passed as a pair of named arguments:

* `X-Trino-Progress-Callback` - a value implementing the `trino.ProgressUpdater`
  interface
* `X-Trino-Progress-Callback-Period` - a `time.Duration` limiting how often the
  callback is invoked while the query state does not change

Both must be set together. The callback receives a `trino.QueryProgressInfo`
with the `QueryId` and the current query statistics. It is invoked when the
query is submitted, when a result page is received, and once more when the
query finishes.

For a query using the spooling protocol, `FailedSegmentAcknowledgments` counts
the segments the driver read but could not acknowledge. Those segments stay in
storage until they expire. Acknowledgments can finish after the query does, so
closing the rows waits for them and, if any failed since the last invocation,
invokes the callback once more with the final count.

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

### Query warnings

The coordinator can attach warnings to a query, such as deprecation notices,
which `database/sql` also has no way to expose. Pass a `*trino.Warnings` as
the `warnings` named argument; once the rows are consumed, `All` returns every
distinct warning the coordinator reported, in the order it first reported it.
Without a `warnings` argument, warnings the coordinator reports are dropped.

```go
var warnings trino.Warnings
rows, err := db.Query("SELECT * FROM foobar", sql.Named("warnings", &warnings))
// ... consume rows ...
for _, w := range warnings.All() {
	log.Printf("warning %s: %s", w.Name, w.Message)
}
```

## Sessions and transactions

### Session state and the connection pool

Statements that change the session, such as `USE`, `SET PATH`, `SET SESSION`,
`SET TIME ZONE`, `SET ROLE`, `PREPARE` and `SET SESSION AUTHORIZATION`, change
it on the underlying connection only. When `database/sql` hands that connection
to the next caller, the driver puts back the user, catalog, schema, session
properties, roles and time zone from the DSN, and drops the path and prepared
statements. To run several statements in one session, use one
[`sql.Conn`](https://godoc.org/database/sql#Conn):

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

if _, err := conn.ExecContext(ctx, "USE tpch.tiny"); err != nil {
	return err
}
// Reads tpch.tiny.nation. A db.Query here would use the DSN catalog and schema.
rows, err := conn.QueryContext(ctx, "SELECT * FROM nation")
```

### Transactions

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

### Connection validation and server version

Opening a connection does not contact the server. `db.PingContext` fetches the
coordinator's `/v1/info` and fails when the server cannot be reached, answers
with an error, or is still starting up. Since that endpoint does not require
authentication, the ping then sends `HEAD /v1/statement` with the connection's
credentials and fails when the server rejects them. No query is started.
Servers older than Trino 469 do not support that request, so for them a
successful ping does not prove the credentials are accepted. With [external
authentication](#external-authentication), that request starts the login flow
when no valid token is cached, so a ping can open the browser.

Before returning a connection to the pool, `database/sql` asks the driver
whether it can be reused, without contacting the server. A connection whose
Kerberos client failed to authenticate a request is dropped, so the next one
reloads the keytab or credential cache, for example after `kinit` renewed an
expired ticket; a statement that failed that way before reaching the server
is retried on a new connection.

The same information, including the server version, is available from
`trino.Conn.ServerInfo` through
[`sql.Conn.Raw`](https://godoc.org/database/sql#Conn.Raw):

```go
conn, err := db.Conn(ctx)
if err != nil {
	return err
}
defer conn.Close()

var info trino.ServerInfo
err = conn.Raw(func(driverConn any) error {
	info, err = driverConn.(*trino.Conn).ServerInfo(ctx)
	return err
})
if err != nil {
	return err
}
log.Printf("Trino %s in %s, up for %s", info.NodeVersion, info.Environment, info.Uptime)
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
* `VARIANT` values; pass the JSON text as a string and convert it with
  `CAST(json_parse(?) AS VARIANT)`

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
  zone the server used to produce them. When scanning arrays or maps of these
  types with `trino.NullSlice` or `trino.NullMapOf`, or the deprecated
  `trino.NullSliceTime` and its 2D and 3D variants, set their `Location` field
  to the same zone, as they use `time.Local` by default.
* `DECIMAL` and `NUMBER` (Trino 480+) - returned as string; use
  `sql.NullString` for nullable columns
* `IPADDRESS` - returned as string
* `INTERVAL YEAR TO MONTH` and `INTERVAL DAY TO SECOND` - returned as string
* `UUID` - returned as string
* `VARIANT` (Trino 481+) - returned as `trino.Variant`, see
  [VARIANT](#variant)
* `Geometry`, `SphericalGeography` and `color` - returned as string
* `BingTile` and `KdbTree` - returned as `map[string]interface{}`, the JSON
  object the server sent
* any other type, like `HyperLogLog`, `SetDigest`, `QDigest`, and `TDigest` -
  returned as `[]byte`, decoded from the base64 form the server sends, the
  same as `VARBINARY`

For reading nullable columns, use `trino.NullTime` or similar structs from
the `database/sql` package, like `sql.NullInt64`.

### ARRAY, MAP and ROW

To read `ARRAY`, `MAP` and `ROW` values, use the generic scanners, which nest
to any depth:

* `trino.NullSlice[T]` for an `ARRAY`, with its elements in `Slice`
* `trino.NullMapOf[K, V]` for a `MAP`, with its entries in `Map`
* `trino.NullRow[T]` for a `ROW`, with its fields stored in the struct `T`
  in `Row`

Each one has a `Valid` field that is `false` for a `NULL` value, at every
level, so a `NULL` inner array is told apart from an empty one:

```go
var tags trino.NullSlice[trino.NullSlice[sql.NullString]]
err := db.QueryRow("SELECT ARRAY[ARRAY['a', NULL], NULL]").Scan(&tags)
// tags.Slice[0].Slice == []sql.NullString{{String: "a", Valid: true}, {}}
// tags.Slice[1].Valid == false

var scores trino.NullMapOf[string, trino.NullSlice[int64]]
err = db.QueryRow("SELECT MAP(ARRAY['a'], ARRAY[ARRAY[BIGINT '1', 2]])").Scan(&scores)
// scores.Map["a"].Slice == []int64{1, 2}
```

Elements, map keys and values, and row fields can be `bool`, `string`,
`int64` and the narrower integer types, `float64`, `float32`, `time.Time`,
`[]byte`, `map[string]interface{}`, `[]interface{}` or `interface{}`, the
nullable types from `database/sql` (`sql.NullBool`, `sql.NullString`,
`sql.NullInt64`, `sql.NullInt32`, `sql.NullInt16`, `sql.NullFloat64`,
`sql.NullTime`), `trino.NullTime`, `trino.NullBinary`, `trino.Variant`,
another generic scanner, or any other type that implements `sql.Scanner`. A
`NULL` element scanned into a plain type that cannot hold it, like `int64`, is
an error. Set the `Location` field of `trino.NullSlice` and `trino.NullMapOf`
to the zone of the connection for elements without a time zone, as they use
`time.Local` by default; it is passed down to nested scanners. The map scanner
is called `NullMapOf` because `trino.NullMap` is the name of the older,
non-generic map scanner.

`trino.NullRow[T]` maps each `ROW` field to an exported field of the struct
`T`, by the name in a `trino:"name"` struct tag, or by the Go field name,
preferring an exact match and falling back to one ignoring case. A field
tagged `trino:"-"` is skipped. An anonymous `ROW` field is named `field<i>`,
where `i` is its zero-based position, so it maps to a struct field called
`Field0`, `Field1`, and so on. A `ROW` field without a matching struct field
is an error; a struct field without a matching `ROW` field keeps its zero
value. Use `trino.NullRow` for nested rows too:

```go
type Point struct {
	X     int64 `trino:"x"`
	Label sql.NullString
}

var points trino.NullSlice[trino.NullRow[Point]]
err := db.QueryRow("SELECT ARRAY[CAST(ROW(1, 'a') AS ROW(x INTEGER, label VARCHAR)), NULL]").Scan(&points)
// points.Slice[0].Row == Point{X: 1, Label: sql.NullString{String: "a", Valid: true}}
// points.Slice[1].Valid == false
```

A `ROW` whose fields are not known ahead of time scans into a `trino.Row`,
which keeps the field names and converts each value the way a plain column of
that type would be converted, including nested `ROW` and `VARIANT` values:

```go
var row trino.Row
err := db.QueryRow("SELECT CAST(ROW(1, 'a') AS ROW(x INTEGER, y VARCHAR))").Scan(&row)
// row.Valid == true
// row.Len() == 2
// row.Name(0) == "x", row.Value(0) == int64(1)
// row.Field("y") == "a", true
```

An `ARRAY` or `MAP` scanned into an `interface{}` holds the decoded JSON
response, `[]interface{}` or `map[string]interface{}`, with `ROW` and
`VARIANT` elements converted into `trino.Row` and `trino.Variant` and every
other element left as the JSON value, so a `VARBINARY` element is still a
base64 string. Prefer the generic scanners, which convert every element.

`sql.ColumnType.ScanType()` reports the generic scanners, instantiated with
the scan types of the elements, keys and values, like
`trino.NullSlice[sql.NullInt32]` for an `ARRAY(INTEGER)`,
`trino.NullMapOf[sql.NullString, sql.NullInt64]` for a
`MAP(VARCHAR, BIGINT)` and `trino.NullSlice[trino.Row]` for an
`ARRAY(ROW(...))`, so a value created with `reflect.New` from it can be
passed to `Scan()`. A `ROW` reports `trino.Row`, as `trino.NullRow` needs a
struct type. The reported types cover arrays of up to three dimensions, maps
of scalar values, and arrays of such maps; a column nested deeper reports
`interface{}` for the values below the deepest level covered, like
`trino.NullMapOf[sql.NullString, interface{}]` for a
`MAP(VARCHAR, ARRAY(BIGINT))`. A map key that is not comparable in Go, like
`VARBINARY`, is reported as `interface{}`.

The following hand-written scanners are deprecated in favor of the generic
ones, but keep working:

* `trino.NullMap`, replaced by `trino.NullMapOf[string, interface{}]`
* `trino.NullSliceBool`, `trino.NullSliceString`, `trino.NullSliceInt64`,
  `trino.NullSliceFloat64`, `trino.NullSliceTime` and `trino.NullSliceMap`,
  replaced by `trino.NullSlice[T]` with `sql.NullBool`, `sql.NullString`,
  `sql.NullInt64`, `sql.NullFloat64`, `trino.NullTime` or
  `trino.NullMapOf[string, interface{}]` elements
* their two and three dimensional variants, like `trino.NullSlice2Bool` and
  `trino.NullSlice3Bool`, replaced by nested `trino.NullSlice` values; unlike
  the generic ones, they turn a `NULL` inner array into an empty one

### VARIANT

To read a `VARIANT` value, scan into a `trino.Variant`:

```go
var v trino.Variant
err := db.QueryRow(`SELECT CAST(JSON '{"a": 1, "b": [true, null]}' AS VARIANT)`).Scan(&v)
// v.Valid == true
// v.Type() == trino.VariantObject
// v.Value() == map[string]interface{}{"a": int64(1), "b": []interface{}{true, nil}}
// v.String() == `{"a":1,"b":[true,null]}`
```

The driver announces the `VARIANT_BINARY` client capability, so the server
sends each value in its binary encoding and `Variant` keeps the value's
type. `Type` returns one of the `Variant*` constants and `Value` converts the
value into a Go value:
* `nil` for a `VARIANT` null
* `bool`
* `int64` for every integer width
* `float32` and `float64`
* `trino.Numeric` for a decimal, in plain notation like `-12.345`
* `string`, and a string like `12151fd2-7586-11e9-8f9e-2a86e4085a59` for a
  UUID
* `[]byte`
* `time.Time` for dates, times and timestamps. Those without a time zone are
  in the zone of the connection, like `DATE`, `TIME` and `TIMESTAMP`
  columns, and timestamps with a time zone are in UTC
* `[]interface{}` for an array and `map[string]interface{}` for an object,
  holding converted values

`String` and `MarshalJSON` return the JSON text the server returns for
`CAST(v AS JSON)`, writing types JSON has no equivalent for as strings,
like `"2020-01-02"` for a date. A SQL `NULL` scans with `Valid` set to
`false`, while a `VARIANT` null, like `CAST(JSON 'null' AS VARIANT)`, is
valid and has the type `trino.VariantNull`.

A `VARIANT` column does not scan into a `string` or `sql.NullString`, as it
did when the driver did not announce the `VARIANT` capabilities and the
server sent `VARIANT` columns as `JSON`: `database/sql` only converts strings,
byte slices and numbers into a string. Scan into a `trino.Variant` and call
`String`, or use `CAST(v AS JSON)` in the query. An `ARRAY(VARIANT)` scans
into a `trino.NullSlice[trino.Variant]`, or still into a
`trino.NullSliceString` holding the JSON text of each element.

## Errors

A query the server rejects or fails returns a `*trino.ErrQueryFailed`, whose
`StatusCode` is the HTTP status of the response that reported the failure
(`200` for a query that failed while running) and which wraps the error
details. For a failure the server reports, the wrapped error is a
`*trino.ErrTrino` with the `ErrorCode`, `ErrorName`, `ErrorType`, `SqlState`,
`Message`, `ErrorLocation` and `FailureInfo` from the
[Trino error](https://trino.io/docs/current/develop/client-protocol.html):

```go
rows, err := db.QueryContext(ctx, "SELECT 1 FORM dual")
var trinoErr *trino.ErrTrino
if errors.As(err, &trinoErr) {
	log.Printf("%s (%d) at line %d: %s", trinoErr.ErrorName, trinoErr.ErrorCode, trinoErr.ErrorLocation.LineNumber, trinoErr.Message)
}
```

Other errors the driver returns:

* `trino.ErrQueryCancelled` when the server reports the query was cancelled,
  for example from the web UI.
* `trino.UnsupportedArgError` for a query argument of a type the driver cannot
  serialize, see [Query arguments](#query-arguments).
* `*trino.SegmentExpiredError` when a spooled segment could not be downloaded
  after its expiration time, see [Spooling protocol](#spooling-protocol).
* A `*trino.ErrQueryFailed` without a `*trino.ErrTrino` inside for an HTTP
  error, such as `401 Unauthorized`, or when the retries of a request were
  exhausted, see [`request_retry_timeout`](#request_retry_timeout).

### Cancellation and timeouts

A query runs until its results are consumed, its context is cancelled or its
timeout passes. When the context of a `QueryContext` or `ExecContext` call is
cancelled or reaches its deadline, the driver cancels the query on the server
with a `DELETE` request, giving it `trino.DefaultCancelQueryTimeout` (30s), and
`rows.Err()` returns the context error. Closing the rows before the last page
was read cancels the query the same way, so a program that stops reading early
does not leave the query running. A query whose context has no deadline gets
[`query_timeout`](#query_timeout), or `trino.DefaultQueryTimeout` (10h) when
that is not set.

## Spooling protocol

The driver supports the [Trino spooling
protocol](https://trino.io/docs/current/client/client-protocol.html#spooling-protocol),
which enables efficient retrieval of large result sets by downloading data in
segments, optionally in parallel and out-of-order.

If the Trino server has the spooling protocol enabled, the driver uses it by
default with the `json` encoding. You can request other encodings for a query
with the `encoding` named argument, as one encoding or a list in order of
preference:

- Supported encodings: `json`, `json+lz4`, `json+zstd`

```go
rows, err := db.Query(query, sql.Named("encoding", "json+zstd"))
rows, err := db.Query(query, sql.Named("encoding", "json+zstd, json+lz4, json"))
```

While a spooled query is in progress, the driver sends periodic `HEAD`
requests to the current `nextUri` (same URL used for result pages) so the
coordinator treats the client as still active, which helps avoid query
abandonment when result consumption is slow. The server must support this
endpoint (Trino 475+); see [`heartbeat_interval`](#heartbeat_interval).

Segment downloads follow redirects, which the coordinator answers them with in
the `coordinator_storage_redirect` and `worker_proxy` values of
`protocol.spooling.retrieval-mode`, sending the client to the storage or to a
worker. A segment request carries only the headers the server listed for the
segment, never the `X-Trino-*` session headers or credentials, so following
the redirect does not leak them; the redirect target needs the segment
headers, such as the encryption key, so they are kept. The `CheckRedirect`
policy of an `HTTPClient` or `custom_client` applies, and the default client
stops after 10 redirects.

Spooled segments stay in storage only until the `expiresAt` time the server
reports for each of them, so rows read too slowly can outlive their segments.
When a segment download fails after that time, the error is a
`*trino.SegmentExpiredError` carrying the expiration time and wrapping the
download error, so `errors.As` still finds the `*trino.ErrQueryFailed` with the
HTTP status. A download that fails before the segment expires returns the
download error as before.

### Configuration options

You can tune the spooling protocol using the following parameters, passed as
`sql.Named` arguments to your query:

- **Spooling worker count**
  `sql.Named("spooling_worker_count", "N")`
  Sets the number of parallel workers used to download spooled segments.
  **Default:** `5`
  **Considerations:**
  - Increasing this value can improve throughput for large result sets,
    especially on high-latency networks.
  - Higher values increase parallelism but may also increase memory usage.

- **Max out-of-order segments**
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

It is not allowed to set `spooling_worker_count` higher than
`max_out_of_order_segments`; doing so results in an error. Each download
worker must reserve a slot for the segment it fetches, and a slot is only
released when that segment can be processed in order. The total number of
slots corresponds to `max_out_of_order_segments`. If you configure more
workers than allowed out-of-order segments, the extra workers would immediately
block while waiting for a slot, defeating the purpose of parallelism and
potentially wasting resources.

```go
rows, err := db.Query(
	query,
	sql.Named("encoding", "json+zstd"),
	sql.Named("spooling_worker_count", "8"),
	sql.Named("max_out_of_order_segments", "20"),
)
```

## Networking

### Custom HTTP client

Beyond the [TLS](#sslcertpath--sslcert) and [proxy](#httpproxy--socksproxy)
parameters, the driver can send every request through an `http.Client` of your
own, with tuned connection pools, timeouts, instrumentation or an
authenticating transport. Pass it as `Config.HTTPClient` to `NewConnector`, or
register it under a name with `trino.RegisterCustomClient` and select it with
the [`custom_client`](#custom_client) DSN parameter:

```go
foobarClient := &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			// your config here...
		},
	},
}
trino.RegisterCustomClient("foobar", foobarClient)
db, err := sql.Open("trino", "https://user@localhost:8443?custom_client=foobar")
```

The default client does not follow HTTP redirects, because a redirect would
carry the `X-Trino-*` headers, including extra credentials, to a host other
than the one in the DSN, and a `301`, `302` or `303` response would turn the
statement `POST` into a `GET` without the query. A redirect response fails the
query instead. An `HTTPClient` is treated the same way, while a registered
`custom_client` follows its own `CheckRedirect` policy, which allows redirects
unless set. Spooled segment downloads follow redirects with every client, see
[Spooling protocol](#spooling-protocol).

A custom client can also add OpenTelemetry instrumentation. The
[otelhttp](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp)
package provides a transport wrapper that creates spans for HTTP requests and
propagates the trace ID in HTTP headers:

```go
otelClient := &http.Client{
	Transport: otelhttp.NewTransport(http.DefaultTransport),
}
trino.RegisterCustomClient("otel", otelClient)
db, err := sql.Open("trino", "https://user@localhost:8443?custom_client=otel")
```

### Proxy

By default the driver takes its proxy from the `HTTP_PROXY`, `HTTPS_PROXY`
and `NO_PROXY` environment variables, as described for
[`http.ProxyFromEnvironment`](https://pkg.go.dev/net/http#ProxyFromEnvironment).
Requests to `localhost` and loopback addresses never use them. To send every
request through a specific proxy instead, set the [`httpProxy` or
`socksProxy`](#httpproxy--socksproxy) parameter, or `Config.HTTPProxy` or
`Config.SOCKSProxy`:

```go
db, err := sql.Open("trino", "https://user@trino.example.com:8443?httpProxy=proxy.example.com:3128")
```

An `HTTPClient` or a `custom_client` uses its own transport, so its proxy
must be configured there, for example with
[`http.ProxyURL`](https://pkg.go.dev/net/http#ProxyURL); the driver rejects
`httpProxy` and `socksProxy` combined with either.

### DNS resolution

There is no DSN parameter for DNS resolution. To resolve the coordinator's
host name differently, for example with a specific DNS server, give the
transport of an `HTTPClient` a dialer with its own
[`net.Resolver`](https://pkg.go.dev/net#Resolver):

```go
resolver := &net.Resolver{
	PreferGo: true,
	Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, "10.0.0.53:53")
	},
}
transport := http.DefaultTransport.(*http.Transport).Clone()
transport.DialContext = (&net.Dialer{Resolver: resolver}).DialContext
connector, err := trino.NewConnector(&trino.Config{
	ServerURI:  "https://user@trino.example.com:8443",
	HTTPClient: &http.Client{Transport: transport},
})
```

A dialer that maps host names to addresses on its own can replace
`DialContext` in the same way.

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
