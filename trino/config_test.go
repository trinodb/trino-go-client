package trino

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatDSN(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name: "session properties",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
			},
			want: "http://foobar@localhost:8080?session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "trace token, client info and language",
			config: &Config{
				ServerURI:  "http://foobar@localhost:8080",
				TraceToken: "trace-123",
				ClientInfo: "batch job #7",
				Language:   "en-US",
			},
			want: "http://foobar@localhost:8080?client_info=batch+job+%237&language=en-US&source=trino-go-client&trace_token=trace-123",
		},
		{
			name: "time zone",
			config: &Config{
				ServerURI: "http://foobar@localhost:8080",
				TimeZone:  "Asia/Tokyo",
			},
			want: "http://foobar@localhost:8080?source=trino-go-client&timezone=Asia%2FTokyo",
		},
		{
			name: "explicit prepare disabled",
			config: &Config{
				ServerURI:              "https://foobar@localhost:8090",
				DisableExplicitPrepare: true,
			},
			want: "https://foobar@localhost:8090?explicitPrepare=false&source=trino-go-client",
		},
		{
			name: "multiple client tags",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				ClientTags:        []string{"test1", "test2", "test3"},
			},
			want: "http://foobar@localhost:8080?clientTags=test1%2Ctest2%2Ctest3&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "single client tag",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				ClientTags:        []string{"test1"},
			},
			want: "http://foobar@localhost:8080?clientTags=test1&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "client tags with special characters",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				ClientTags:        []string{"foo %20", "bar=test", "baz#tag"},
			},
			want: "http://foobar@localhost:8080?clientTags=foo+%2520%2Cbar%3Dtest%2Cbaz%23tag&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "SSL cert path",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				SSLCertPath:       "cert.pem",
			},
			want: "https://foobar@localhost:8080?SSLCertPath=cert.pem&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "SSL cert",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				SSLCert:           sampleCertificatePEM,
			},
			want: "https://foobar@localhost:8080?SSLCert=" + url.QueryEscape(sampleCertificatePEM) + "&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "https without SSL cert",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
			},
			want: "https://foobar@localhost:8080?session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "SSL client cert and key path",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8080",
				SSLClientCertPath: "client.pem",
				SSLClientKeyPath:  "client-key.pem",
			},
			want: "https://foobar@localhost:8080?SSLClientCertPath=client.pem&SSLClientKeyPath=client-key.pem&source=trino-go-client",
		},
		{
			name: "SSL client cert and key string",
			config: &Config{
				ServerURI:     "https://foobar@localhost:8080",
				SSLClientCert: sampleCertificatePEM,
				SSLClientKey:  "sample-key",
			},
			want: "https://foobar@localhost:8080?SSLClientCert=" + url.QueryEscape(sampleCertificatePEM) + "&SSLClientKey=sample-key&source=trino-go-client",
		},
		{
			name: "SSL verification mode CA",
			config: &Config{
				ServerURI:       "https://foobar@localhost:8080",
				SSLVerification: SSLVerificationCA,
			},
			want: "https://foobar@localhost:8080?SSLVerification=CA&source=trino-go-client",
		},
		{
			name: "extra credentials",
			config: &Config{
				ServerURI:        "http://foobar@localhost:8080",
				ExtraCredentials: map[string]string{"token": "mYtOkEn", "otherToken": "oThErToKeN%*!#@special"},
			},
			want: "http://foobar@localhost:8080?extra_credentials=otherToken%3AoThErToKeN%25%2A%21%23%40special%3Btoken%3AmYtOkEn&source=trino-go-client",
		},
		{
			name: "kerberos",
			config: &Config{
				ServerURI:                 "https://foobar@localhost:8090",
				SessionProperties:         map[string]string{"query_priority": "1"},
				KerberosEnabled:           true,
				KerberosKeytabPath:        "/opt/test.keytab",
				KerberosPrincipal:         "trino/testhost",
				KerberosRealm:             "example.com",
				KerberosConfigPath:        "/etc/krb5.conf",
				KerberosRemoteServiceName: "service",
				SSLCertPath:               "/tmp/test.cert",
			},
			want: "https://foobar@localhost:8090?KerberosConfigPath=%2Fetc%2Fkrb5.conf&KerberosEnabled=true&KerberosKeytabPath=%2Fopt%2Ftest.keytab&KerberosPrincipal=trino%2Ftesthost&KerberosRealm=example.com&KerberosRemoteServiceName=service&SSLCertPath=%2Ftmp%2Ftest.cert&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "multiple catalog roles",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SessionProperties: map[string]string{"query_priority": "1"},
				Roles:             map[string]string{"catalog1": "role1", "catalog2": "role2"},
			},
			want: "https://foobar@localhost:8090?roles=catalog1%3Arole1%3Bcatalog2%3Arole2&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "single catalog role",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SessionProperties: map[string]string{"query_priority": "1"},
				Roles:             map[string]string{"catalog1": "role1"},
			},
			want: "https://foobar@localhost:8090?roles=catalog1%3Arole1&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "access token",
			config: &Config{
				ServerURI:   "https://foobar@localhost:8090",
				AccessToken: "token",
			},
			want: "https://foobar@localhost:8090?accessToken=token&source=trino-go-client",
		},
		{
			name: "query timeout",
			config: &Config{
				ServerURI:    "https://foobar@localhost:8090",
				QueryTimeout: ptr(10 * time.Second),
			},
			want: "https://foobar@localhost:8090?query_timeout=10s&source=trino-go-client",
		},
		{
			name: "forward authorization header",
			config: &Config{
				ServerURI:                  "https://foobar@localhost:8090",
				ForwardAuthorizationHeader: true,
			},
			want: "https://foobar@localhost:8090?forwardAuthorizationHeader=true&source=trino-go-client",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.config.FormatDSN()

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFormatDSNRejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		config *Config
	}{
		{
			name:   "malformed URL",
			config: &Config{ServerURI: ":("},
		},
		{
			name: "kerberos without TLS",
			config: &Config{
				ServerURI:       "http://foobar@localhost:8090",
				KerberosEnabled: true,
			},
		},
		{
			name:   "password without TLS",
			config: &Config{ServerURI: "http://user:secret@localhost:8080"},
		},
		{
			name: "client certificate without key",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SSLClientCertPath: "client.pem",
			},
		},
		{
			name: "client key without certificate",
			config: &Config{
				ServerURI:        "https://foobar@localhost:8090",
				SSLClientKeyPath: "client-key.pem",
			},
		},
		{
			name: "client certificate without TLS",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8090",
				SSLClientCertPath: "client.pem",
				SSLClientKeyPath:  "client-key.pem",
			},
		},
		{
			name: "client certificate path and string together",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SSLClientCertPath: "client.pem",
				SSLClientCert:     sampleCertificatePEM,
				SSLClientKeyPath:  "client-key.pem",
			},
		},
		{
			name: "client key path and string together",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SSLClientCertPath: "client.pem",
				SSLClientKeyPath:  "client-key.pem",
				SSLClientKey:      "sample-key",
			},
		},
		{
			name: "unknown SSL verification mode",
			config: &Config{
				ServerURI:       "https://foobar@localhost:8090",
				SSLVerification: "bogus",
			},
		},
		{
			name: "SSL verification without TLS",
			config: &Config{
				ServerURI:       "http://foobar@localhost:8090",
				SSLVerification: SSLVerificationNone,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.config.FormatDSN()

			require.Error(t, err)
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

const sampleCertificatePEM = `-----BEGIN CERTIFICATE-----
MIIFijCCA3ICCQDngXKCZFwSazANBgkqhkiG9w0BAQsFADCBhjELMAkGA1UEBhMC
WFgxEjAQBgNVBAgMCVN0YXRlTmFtZTERMA8GA1UEBwwIQ2l0eU5hbWUxFDASBgNV
BAoMC0NvbXBhbnlOYW1lMRswGQYDVQQLDBJDb21wYW55U2VjdGlvbk5hbWUxHTAb
BgNVBAMMFENvbW1vbk5hbWVPckhvc3RuYW1lMB4XDTIzMDUxNzE2MzQ0MloXDTMz
MDUxNDE2MzQ0MlowgYYxCzAJBgNVBAYTAlhYMRIwEAYDVQQIDAlTdGF0ZU5hbWUx
ETAPBgNVBAcMCENpdHlOYW1lMRQwEgYDVQQKDAtDb21wYW55TmFtZTEbMBkGA1UE
CwwSQ29tcGFueVNlY3Rpb25OYW1lMR0wGwYDVQQDDBRDb21tb25OYW1lT3JIb3N0
bmFtZTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAKzz/SIuOiHZbUAH
xCWrMaiJybdHHHl0smCu50XKvl/ZkszO1c4aES8/Vohw44ttaE+GOknTSGPka356
NqwdPYMjnXN0d5HY5T5nOfgLxGD/1iCHACrT4gkd1asJ7eFaUgud0a+e9+oG53Vh
Z3QV8+5JaWPuBMudJ8EOtrPMd0dJKVzeExTbpQLJ9HdIsHc6DXqshACd8Iy+ezqf
OoYMYyJMAHO86MZrTs3t9AwUADlvntrwwObVrZ3v43IOKwJTRnpImmVlkouKrGn/
HKzRmJEJ6hJQXhuhqI/0rr61XR8aa8Gs0FqtTTMJ32+PciPPzFtFVLAeA417lYz+
uXZ6IpTLK4oDH8Q6gJY80GYqcGc+01ZY90W2L+odTz9P74vnTvsUgSjOcy7prJ0+
WxoeBNPvkLeetX9WDZW4XaR++HVO1qelNJQqeB6Nver9MJdKkXvR3OxT6iluqXfA
l9JJ57tnzspSrttjWG4kwwiaGn/4xPqd95Hp0r1WAK8U0Cqtvz+Zw9jl341tC1Ya
K1KFIErZYf0KX8ZiYvmkHaTRxYiCmFnnfLtGdrAWkacisLKMhjeb9LXwC/TVtvio
a+ofiW2DX80pQptkfNJs9P19ZFEojPAEFHiZFpz5yZSxHglxIsdIhRsuy5xb/KTo
zey3tsKQJaFIah+aHKjyn3uZx2IRAgMBAAEwDQYJKoZIhvcNAQELBQADggIBAIs5
sbCMB6bT0hcNFqFRCI/BL23m5jwdL9kNWDlEQxBvErtzTC+uStGrCqwV+qu49QAZ
64kUolbzFyq/hQFpHd+9EzNkZGbiOf5toWaBUP6jaZzqYPdfDW+AwIA7iPHcqwH1
iWX2zuAWAICy4H+S4oa/ShOPc8BrrnS8k5f1NpergOhd+wl+szuXJN9Tjli3wd/k
L7f86xvZfOrEbss8YP4QE0+mKh6G71NLEVQ4SV7yIE2hCNLDFWS2ltGVRLv6CDaQ
fXIQrZx2Khvpj+HI/hrwm1wV8Cg5w2IvB831YjTSepSoos0Cc/qYC78zqol/NbwL
7TdHtuZKukDrisRiCDdoKFmS1/IUVeVR2352CG8G3Zo0wwfzoKLxLUtunnrKMmmO
r2jXykqP2hb1dApBNFM7FoaJ7a0j6EcURW8wYl4I+b9ymftPnnZ8mgrjwvLh5ETj
RgGsIBychLZoc1WWTZWu62+mvmSJnzEIFfaiSeYZLaL6qFHm6kqsAUn4s1Looj8/
XoCNjMecchWbpHGCPwMFH1k2smxu7bKk/RJNuWSVn1IPUceJnOBHZGj92aJGZpjr
8j39T3dK9F2r5rHwjZpeEIhyhbLw6pYKif+lBgAWJD3waG0ycwURA02/POHN4CpT
FKu5ZAlRfb2aYegr49DHhzoVAdInWQmP+5EZEUD1
-----END CERTIFICATE-----`

func TestParseDSNToConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config *Config
	}{
		{
			name: "HTTP with custom client and full configuration",
			config: &Config{
				ServerURI:                  "http://foobar@localhost:8080",
				Source:                     "trino-go-client",
				Catalog:                    "test_catalog",
				Schema:                     "test_schema",
				SessionProperties:          map[string]string{"session_property_one": "1", "session_property_two": "2"},
				ExtraCredentials:           map[string]string{"extra_credential_one": "1", "extra_credential_two": "2"},
				ClientTags:                 []string{"tag1", "tag2", "tag3"},
				TraceToken:                 "trace-token",
				ClientInfo:                 "client info",
				Language:                   "pl-PL",
				TimeZone:                   "Asia/Tokyo",
				CustomClientName:           "client_name",
				AccessToken:                "token_test",
				DisableExplicitPrepare:     true,
				ForwardAuthorizationHeader: true,
				QueryTimeout:               &[]time.Duration{5 * time.Minute}[0],
				HeartbeatInterval:          &[]time.Duration{2 * time.Minute}[0],
				Roles:                      map[string]string{"catalog1": "role1", "catalog2": "role2"},
			},
		},
		{
			name: "HTTPS with Kerberos and SSL cert path",
			config: &Config{
				ServerURI:                  "https://foobar@localhost:8080",
				Source:                     "trino-go-client",
				Catalog:                    "test_catalog",
				Schema:                     "test_schema",
				SessionProperties:          map[string]string{"session_property_one": "1", "session_property_two": "2"},
				ExtraCredentials:           map[string]string{"extra_credential_one": "1", "extra_credential_two": "2"},
				ClientTags:                 []string{"tag1", "tag2", "tag3"},
				KerberosEnabled:            true, // Requires HTTPS
				KerberosKeytabPath:         "kerberos-path",
				KerberosPrincipal:          "kerberos-pricipal",
				KerberosRemoteServiceName:  "kerberos-remote-service-name",
				KerberosRealm:              "kerberos-realm",
				KerberosConfigPath:         "kerberos-config-path",
				SSLCertPath:                "ssl-cert-path",
				AccessToken:                "token_test",
				DisableExplicitPrepare:     true,
				ForwardAuthorizationHeader: true,
				QueryTimeout:               &[]time.Duration{5 * time.Minute}[0],
				HeartbeatInterval:          &[]time.Duration{2 * time.Minute}[0],
			},
		},
		{
			name: "HTTPS with SSL cert string (alternative to cert path)",
			config: &Config{
				ServerURI: "https://localhost:8080",
				Source:    "trino-go-client",
				SSLCert:   "-----BEGIN CERTIFICATE-----\ntest-cert-data\n-----END CERTIFICATE-----",
			},
		},
		{
			name: "HTTPS with client certificate path and CA verification",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8080",
				Source:            "trino-go-client",
				SSLClientCertPath: "client-cert-path",
				SSLClientKeyPath:  "client-key-path",
				SSLVerification:   SSLVerificationCA,
			},
		},
		{
			name: "HTTPS with inline client certificate and key",
			config: &Config{
				ServerURI:     "https://localhost:8080",
				Source:        "trino-go-client",
				SSLClientCert: "-----BEGIN CERTIFICATE-----\ntest-client-cert-data\n-----END CERTIFICATE-----",
				SSLClientKey:  "-----BEGIN PRIVATE KEY-----\ntest-client-key-data\n-----END PRIVATE KEY-----",
			},
		},
		{
			name: "HTTP with explicit default boolean values",
			config: &Config{
				ServerURI:                  "http://localhost:8080",
				Source:                     "trino-go-client",
				DisableExplicitPrepare:     false,
				ForwardAuthorizationHeader: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := tt.config.FormatDSN()
			require.NoError(t, err)
			got, err := ParseDSN(dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.config, got)
		})
	}
}

func TestParseDSNToConfigAllFieldsHandled(t *testing.T) {
	t.Parallel()
	complexDSN := "https://user:pass@localhost:8080/?" +
		"source=test-source&" +
		"catalog=test_catalog&" +
		"schema=test_schema&" +
		"session_properties=prop1%3Avalue1%3Bprop2%3Avalue2&" +
		"extra_credentials=cred1%3Asecret1%3Bcred2%3Asecret2&" +
		"clientTags=tag1%2Ctag2%2Ctag3&" +
		"trace_token=trace-123&" +
		"client_info=test%20client%20info&" +
		"language=en-US&" +
		"timezone=Asia%2FTokyo&" +
		"custom_client=test_client&" +
		"KerberosEnabled=true&" +
		"KerberosKeytabPath=/path/to/keytab&" +
		"KerberosPrincipal=user%40REALM.COM&" +
		"KerberosRemoteServiceName=trino-service&" +
		"KerberosRealm=REALM.COM&" +
		"KerberosConfigPath=/etc/krb5.conf&" +
		"SSLCertPath=/path/to/cert.pem&" +
		"SSLCert=-----BEGIN%20CERTIFICATE-----test-cert-----END%20CERTIFICATE-----&" +
		"SSLClientCertPath=/path/to/client-cert.pem&" +
		"SSLClientCert=-----BEGIN%20CERTIFICATE-----test-client-cert-----END%20CERTIFICATE-----&" +
		"SSLClientKeyPath=/path/to/client-key.pem&" +
		"SSLClientKey=-----BEGIN%20PRIVATE%20KEY-----test-client-key-----END%20PRIVATE%20KEY-----&" +
		"SSLVerification=CA&" +
		"accessToken=jwt-token-here&" +
		"explicitPrepare=false&" +
		"forwardAuthorizationHeader=true&" +
		"query_timeout=5m30s&" +
		"heartbeat_interval=45s&" +
		"roles=catalog1%3Arole1%3Bcatalog2%3Arole2"

	config, err := ParseDSN(complexDSN)
	require.NoError(t, err)
	require.NotNil(t, config)

	v := reflect.ValueOf(config).Elem()
	configType := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldName := configType.Field(i).Name
		fieldType := field.Type()

		switch fieldType.Kind() {
		case reflect.String:
			assert.NotEmpty(t, field.String(), "Field %s should not be empty - add it to the test DSN and ParseDSNToConfig", fieldName)
		case reflect.Slice:
			assert.Greater(t, field.Len(), 0, "Field %s should not be empty slice - add it to the test DSN and ParseDSNToConfig", fieldName)
		case reflect.Map:
			assert.Greater(t, field.Len(), 0, "Field %s should not be empty map - add it to the test DSN and ParseDSNToConfig", fieldName)
		case reflect.Bool:
			assert.True(t, field.Bool(), "Field %s should be true - add it to the test DSN and ParseDSNToConfig", fieldName)
		case reflect.Ptr:
			assert.NotNil(t, field.Interface(), "Field %s should not be nil - add it to the test DSN and ParseDSNToConfig", fieldName)
		}
	}

	assert.Equal(t, "https://user:pass@localhost:8080", config.ServerURI)
	assert.Equal(t, "test-source", config.Source)
	assert.Equal(t, "test_catalog", config.Catalog)
	assert.Equal(t, "test_schema", config.Schema)
	assert.Equal(t, map[string]string{"prop1": "value1", "prop2": "value2"}, config.SessionProperties)
	assert.Equal(t, map[string]string{"cred1": "secret1", "cred2": "secret2"}, config.ExtraCredentials)
	assert.Equal(t, []string{"tag1", "tag2", "tag3"}, config.ClientTags)
	assert.Equal(t, "trace-123", config.TraceToken)
	assert.Equal(t, "test client info", config.ClientInfo)
	assert.Equal(t, "en-US", config.Language)
	assert.Equal(t, "Asia/Tokyo", config.TimeZone)
	assert.Equal(t, "test_client", config.CustomClientName)
	assert.Equal(t, true, config.KerberosEnabled)
	assert.Equal(t, "/path/to/keytab", config.KerberosKeytabPath)
	assert.Equal(t, "user@REALM.COM", config.KerberosPrincipal)
	assert.Equal(t, "trino-service", config.KerberosRemoteServiceName)
	assert.Equal(t, "REALM.COM", config.KerberosRealm)
	assert.Equal(t, "/etc/krb5.conf", config.KerberosConfigPath)
	assert.Equal(t, "/path/to/cert.pem", config.SSLCertPath)
	assert.Equal(t, "-----BEGIN CERTIFICATE-----test-cert-----END CERTIFICATE-----", config.SSLCert)
	assert.Equal(t, "/path/to/client-cert.pem", config.SSLClientCertPath)
	assert.Equal(t, "-----BEGIN CERTIFICATE-----test-client-cert-----END CERTIFICATE-----", config.SSLClientCert)
	assert.Equal(t, "/path/to/client-key.pem", config.SSLClientKeyPath)
	assert.Equal(t, "-----BEGIN PRIVATE KEY-----test-client-key-----END PRIVATE KEY-----", config.SSLClientKey)
	assert.Equal(t, SSLVerificationCA, config.SSLVerification)
	assert.Equal(t, "jwt-token-here", config.AccessToken)
	assert.Equal(t, true, config.DisableExplicitPrepare)
	assert.Equal(t, true, config.ForwardAuthorizationHeader)
	assert.NotNil(t, config.QueryTimeout)
	assert.Equal(t, 5*time.Minute+30*time.Second, *config.QueryTimeout)
	assert.NotNil(t, config.HeartbeatInterval)
	assert.Equal(t, 45*time.Second, *config.HeartbeatInterval)
	assert.Equal(t, map[string]string{"catalog1": "role1", "catalog2": "role2"}, config.Roles)
}

func TestParseDSNPasswordRequiresTLS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		wantErr string
	}{
		{
			name:    "http with password",
			dsn:     "http://user:secret@localhost:8080",
			wantErr: "trino: TLS/SSL is required for authentication with username and password",
		},
		{
			name: "http with username only",
			dsn:  "http://user@localhost:8080",
		},
		{
			name: "https with password",
			dsn:  "https://user:secret@localhost:8080",
		},
		{
			name: "http with empty password",
			dsn:  "http://user:@localhost:8080",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDSN(tt.dsn)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestInvalidExtraCredentials(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		credentials map[string]string
		wantErr     string
	}{
		{
			name:        "empty key",
			credentials: map[string]string{"": "emptyKey"},
			wantErr:     "trino: extra_credentials key is empty",
		},
		{
			name:        "empty value",
			credentials: map[string]string{"valid": "a", "emptyValue": ""},
			wantErr:     "trino: extra_credentials value is empty",
		},
		{
			name:        "unprintable key",
			credentials: map[string]string{"😊": "unprintableKey"},
			wantErr:     "trino: extra_credentials key '😊' contains spaces or is not printable ASCII",
		},
		{
			name:        "unprintable value",
			credentials: map[string]string{"unprintableValue": "😊"},
			wantErr:     "trino: extra_credentials value for key 'unprintableValue' contains spaces or is not printable ASCII",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				ServerURI:        "http://foobar@localhost:8080",
				ExtraCredentials: tc.credentials,
			}
			dsn, err := c.FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			err = db.Ping()

			assert.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestConnErrorDSN(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		dsn  string
	}{
		{name: "malformed", dsn: "://"},
		{name: "unknown client", dsn: "http://localhost?custom_client=unknown"},
		{name: "http password", dsn: "http://user:secret@localhost:8080"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("trino", tc.dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			_, err = db.Query("SELECT 1")

			assert.Errorf(t, err, "test dsn is supposed to fail: %s", tc.dsn)
		})
	}
}

func TestRegisterCustomClientReserved(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"true", "false"} {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			require.Errorf(t,
				RegisterCustomClient(tc, &http.Client{}),
				"client key name supposed to fail: %s", tc)
		})
	}
}

func TestHeartbeatIntervalDSNParse(t *testing.T) {
	t.Parallel()
	base := "http://user@127.0.0.1:9"

	for _, tc := range []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "invalid duration", query: "heartbeat_interval=not_a_duration", wantErr: "invalid duration for heartbeat_interval"},
		{name: "zero", query: "heartbeat_interval=0s", wantErr: "heartbeat_interval must be positive"},
		{name: "negative", query: "heartbeat_interval=-30s", wantErr: "heartbeat_interval must be positive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDSN(base + "/?" + tc.query)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	t.Run("valid", func(t *testing.T) {
		cfg, err := ParseDSN(base + "/?heartbeat_interval=750ms")
		require.NoError(t, err)
		require.NotNil(t, cfg.HeartbeatInterval)
		assert.Equal(t, 750*time.Millisecond, *cfg.HeartbeatInterval)
	})
}

func TestHeartbeatIntervalFormatDSNRoundTrip(t *testing.T) {
	t.Parallel()
	hb := 90 * time.Second
	c := &Config{
		ServerURI:         "http://user@localhost:8080",
		HeartbeatInterval: &hb,
	}
	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	got, err := ParseDSN(dsn)
	require.NoError(t, err)
	require.NotNil(t, got.HeartbeatInterval)
	assert.Equal(t, 90*time.Second, *got.HeartbeatInterval)
}

func TestHeartbeatIntervalPingRejectsInvalidDSN(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("trino", "http://user@127.0.0.1:9/?heartbeat_interval=0s")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	err = db.Ping()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "heartbeat_interval must be positive")
}

func TestSSLCertPath(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("trino", "https://localhost:9?SSLCertPath=/tmp/invalid_test.cert")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	want := "Error loading SSL Cert File"
	err = db.Ping()
	require.Error(t, err)
	require.Contains(t, err.Error(), want)
}

func TestSSLCertTrustsServer(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	certPath := filepath.Join(t.TempDir(), "certificate.pem")
	require.NoError(t, os.WriteFile(certPath, []byte(fc.certificatePEM()), 0o600))

	cases := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{name: "inline certificate", config: Config{SSLCert: fc.certificatePEM()}},
		{name: "certificate path", config: Config{SSLCertPath: certPath}},
		{name: "no certificate", wantErr: "certificate"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.config
			config.ServerURI = fc.url()
			dsn, err := config.FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			rows, err := db.Query("SELECT 1")

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []int{1}, collectInts(t, rows))
		})
	}
}

func TestSSLVerificationModeInvalid(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("trino", "https://localhost:9?SSLVerification=bogus")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	err = db.Ping()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid SSL verification mode")
}

func TestSSLClientCertKeyPairingRejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		query string
	}{
		{name: "cert without key", query: "SSLClientCert=" + url.QueryEscape(sampleCertificatePEM)},
		{name: "key without cert", query: "SSLClientKey=sample-key"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("trino", "https://localhost:9?"+tc.query)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			err = db.Ping()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be specified together")
		})
	}
}

func TestSSLClientCertificate(t *testing.T) {
	t.Parallel()
	fc := newFakeMTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))

	cases := []struct {
		name       string
		withClient bool
	}{
		{name: "with client certificate", withClient: true},
		{name: "without client certificate", withClient: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := Config{ServerURI: fc.url(), SSLCert: fc.certificatePEM()}
			if tc.withClient {
				config.SSLClientCert = fc.clientCertificatePEM()
				config.SSLClientKey = fc.clientPrivateKeyPEM()
			}
			dsn, err := config.FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			rows, err := db.Query("SELECT 1")

			if !tc.withClient {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []int{1}, collectInts(t, rows))
		})
	}
}

// The coordinator's certificate names a host other than the one dialed.
func TestSSLVerificationModes(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinatorForHost(t, "example.com")
	fc.respond(statementPage(), resultPage([][]any{{1}}))

	cases := []struct {
		name    string
		mode    string
		trusted bool
		wantErr string
	}{
		{name: "full rejects hostname mismatch", mode: SSLVerificationFull, trusted: true, wantErr: "certificate"},
		{name: "ca skips hostname check", mode: SSLVerificationCA, trusted: true},
		{name: "ca rejects untrusted chain", mode: SSLVerificationCA, wantErr: "certificate"},
		{name: "none skips all checks", mode: SSLVerificationNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := Config{ServerURI: fc.url(), SSLVerification: tc.mode}
			if tc.trusted {
				config.SSLCert = fc.certificatePEM()
			}
			dsn, err := config.FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			rows, err := db.Query("SELECT 1")

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []int{1}, collectInts(t, rows))
		})
	}
}
