package trino

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	c := &Config{
		ServerURI:         "http://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "http://foobar@localhost:8080?session_properties=query_priority%3A1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestPreserveExplicitPrepareQueryParameterConfig(t *testing.T) {
	c := &Config{
		ServerURI:              "https://foobar@localhost:8090",
		DisableExplicitPrepare: true,
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8090?explicitPrepare=false&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestParseDSNToConfig(t *testing.T) {
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
	complexDSN := "https://user:pass@localhost:8080/?" +
		"source=test-source&" +
		"catalog=test_catalog&" +
		"schema=test_schema&" +
		"session_properties=prop1%3Avalue1%3Bprop2%3Avalue2&" +
		"extra_credentials=cred1%3Asecret1%3Bcred2%3Asecret2&" +
		"clientTags=tag1%2Ctag2%2Ctag3&" +
		"custom_client=test_client&" +
		"KerberosEnabled=true&" +
		"KerberosKeytabPath=/path/to/keytab&" +
		"KerberosPrincipal=user%40REALM.COM&" +
		"KerberosRemoteServiceName=trino-service&" +
		"KerberosRealm=REALM.COM&" +
		"KerberosConfigPath=/etc/krb5.conf&" +
		"SSLCertPath=/path/to/cert.pem&" +
		"SSLCert=-----BEGIN%20CERTIFICATE-----test-cert-----END%20CERTIFICATE-----&" +
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
	assert.Equal(t, "test_client", config.CustomClientName)
	assert.Equal(t, true, config.KerberosEnabled)
	assert.Equal(t, "/path/to/keytab", config.KerberosKeytabPath)
	assert.Equal(t, "user@REALM.COM", config.KerberosPrincipal)
	assert.Equal(t, "trino-service", config.KerberosRemoteServiceName)
	assert.Equal(t, "REALM.COM", config.KerberosRealm)
	assert.Equal(t, "/etc/krb5.conf", config.KerberosConfigPath)
	assert.Equal(t, "/path/to/cert.pem", config.SSLCertPath)
	assert.Equal(t, "-----BEGIN CERTIFICATE-----test-cert-----END CERTIFICATE-----", config.SSLCert)
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

func TestFormatDSNPasswordRequiresTLS(t *testing.T) {
	c := &Config{ServerURI: "http://user:secret@localhost:8080"}
	_, err := c.FormatDSN()
	assert.EqualError(t, err, "trino: TLS/SSL is required for authentication with username and password")
}

func TestConfigFormatDSNTags(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name: "multiple tags",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				ClientTags:        []string{"test1", "test2", "test3"},
			},
			want: "http://foobar@localhost:8080?clientTags=test1%2Ctest2%2Ctest3&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "single tag",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				ClientTags:        []string{"test1"},
			},
			want: "http://foobar@localhost:8080?clientTags=test1&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "multiple tags with special characters",
			config: &Config{
				ServerURI:         "http://foobar@localhost:8080",
				SessionProperties: map[string]string{"query_priority": "1"},
				ClientTags:        []string{"foo %20", "bar=test", "baz#tag"},
			},
			want: "http://foobar@localhost:8080?clientTags=foo+%2520%2Cbar%3Dtest%2Cbaz%23tag&session_properties=query_priority%3A1&source=trino-go-client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.config.FormatDSN()

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

		})
	}
}

func TestConfigSSLCertPath(t *testing.T) {
	c := &Config{
		ServerURI:         "https://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
		SSLCertPath:       "cert.pem",
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8080?SSLCertPath=cert.pem&session_properties=query_priority%3A1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestConfigSSLCert(t *testing.T) {
	sslCert := `-----BEGIN CERTIFICATE-----
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
	c := &Config{
		ServerURI:         "https://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
		SSLCert:           sslCert,
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8080?SSLCert=" + url.QueryEscape(sslCert) + "&session_properties=query_priority%3A1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestExtraCredentials(t *testing.T) {
	c := &Config{
		ServerURI:        "http://foobar@localhost:8080",
		ExtraCredentials: map[string]string{"token": "mYtOkEn", "otherToken": "oThErToKeN%*!#@special"},
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "http://foobar@localhost:8080?extra_credentials=otherToken%3AoThErToKeN%25%2A%21%23%40special%3Btoken%3AmYtOkEn&source=trino-go-client"
	assert.Equal(t, want, dsn)
}

func TestInvalidExtraCredentials(t *testing.T) {
	testcases := []struct {
		Name        string
		Credentials map[string]string
		Error       string
	}{
		{
			Name:        "Empty key",
			Credentials: map[string]string{"": "emptyKey"},
			Error:       "trino: extra_credentials key is empty",
		},
		{
			Name:        "Empty value",
			Credentials: map[string]string{"valid": "a", "emptyValue": ""},
			Error:       "trino: extra_credentials value is empty",
		},
		{
			Name:        "Unprintable key",
			Credentials: map[string]string{"😊": "unprintableKey"},
			Error:       "trino: extra_credentials key '😊' contains spaces or is not printable ASCII",
		},
		{
			Name:        "Unprintable value",
			Credentials: map[string]string{"unprintableValue": "😊"},
			Error:       "trino: extra_credentials value for key 'unprintableValue' contains spaces or is not printable ASCII",
		},
	}

	for _, tc := range testcases {

		t.Run(tc.Name, func(t *testing.T) {
			c := &Config{
				ServerURI:        "http://foobar@localhost:8080",
				ExtraCredentials: tc.Credentials,
			}
			dsn, err := c.FormatDSN()
			require.NoError(t, err)
			db, err := sql.Open("trino", dsn)
			require.NoError(t, err)
			err = db.Ping()
			assert.EqualError(t, err, tc.Error)
		})
	}
}

func TestConfigWithoutSSLCertPath(t *testing.T) {
	c := &Config{
		ServerURI:         "https://foobar@localhost:8080",
		SessionProperties: map[string]string{"query_priority": "1"},
	}
	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8080?session_properties=query_priority%3A1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestKerberosConfig(t *testing.T) {
	c := &Config{
		ServerURI:                 "https://foobar@localhost:8090",
		SessionProperties:         map[string]string{"query_priority": "1"},
		KerberosEnabled:           true,
		KerberosKeytabPath:        "/opt/test.keytab",
		KerberosPrincipal:         "trino/testhost",
		KerberosRealm:             "example.com",
		KerberosConfigPath:        "/etc/krb5.conf",
		KerberosRemoteServiceName: "service",
		SSLCertPath:               "/tmp/test.cert",
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8090?KerberosConfigPath=%2Fetc%2Fkrb5.conf&KerberosEnabled=true&KerberosKeytabPath=%2Fopt%2Ftest.keytab&KerberosPrincipal=trino%2Ftesthost&KerberosRealm=example.com&KerberosRemoteServiceName=service&SSLCertPath=%2Ftmp%2Ftest.cert&session_properties=query_priority%3A1&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestFormatDSNWithRoles(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		wantDSN     string
		expectError bool
	}{
		{
			name: "Multiple catalog roles",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SessionProperties: map[string]string{"query_priority": "1"},
				Roles:             map[string]string{"catalog1": "role1", "catalog2": "role2"},
			},
			wantDSN: "https://foobar@localhost:8090?roles=catalog1%3Arole1%3Bcatalog2%3Arole2&session_properties=query_priority%3A1&source=trino-go-client",
		},
		{
			name: "Single catalog role",
			config: &Config{
				ServerURI:         "https://foobar@localhost:8090",
				SessionProperties: map[string]string{"query_priority": "1"},
				Roles:             map[string]string{"catalog1": "role1"},
			},
			wantDSN: "https://foobar@localhost:8090?roles=catalog1%3Arole1&session_properties=query_priority%3A1&source=trino-go-client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := tt.config.FormatDSN()
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDSN, dsn)
			}
		})
	}
}

func TestInvalidKerberosConfig(t *testing.T) {
	c := &Config{
		ServerURI:       "http://foobar@localhost:8090",
		KerberosEnabled: true,
	}

	_, err := c.FormatDSN()
	assert.Error(t, err, "dsn generated from invalid secure url, since kerberos enabled must has SSL enabled")
}

func TestAccessTokenConfig(t *testing.T) {
	c := &Config{
		ServerURI:   "https://foobar@localhost:8090",
		AccessToken: "token",
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8090?accessToken=token&source=trino-go-client"

	assert.Equal(t, want, dsn)
}

func TestConfigWithMalformedURL(t *testing.T) {
	_, err := (&Config{ServerURI: ":("}).FormatDSN()
	assert.Error(t, err, "dsn generated from malformed url")
}

func TestConnErrorDSN(t *testing.T) {
	testcases := []struct {
		Name string
		DSN  string
	}{
		{Name: "malformed", DSN: "://"},
		{Name: "unknown_client", DSN: "http://localhost?custom_client=unknown"},
		{Name: "http_password", DSN: "http://user:secret@localhost:8080"},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			db, err := sql.Open("trino", tc.DSN)
			require.NoError(t, err)

			_, err = db.Query("SELECT 1")
			assert.Errorf(t, err, "test dsn is supposed to fail: %s", tc.DSN)

			if err == nil {
				require.NoError(t, db.Close())
			}
		})
	}
}

func TestRegisterCustomClientReserved(t *testing.T) {
	for _, tc := range []string{"true", "false"} {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			require.Errorf(t,
				RegisterCustomClient(tc, &http.Client{}),
				"client key name supposed to fail: %s", tc)
		})
	}
}

func TestQueryTimeout(t *testing.T) {
	timeout := 10 * time.Second
	c := &Config{
		ServerURI:    "https://foobar@localhost:8090",
		QueryTimeout: &timeout,
	}
	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8090?query_timeout=10s&source=trino-go-client"
	assert.Equal(t, want, dsn)
}

func TestHeartbeatIntervalDSNParse(t *testing.T) {
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
	db, err := sql.Open("trino", "http://user@127.0.0.1:9/?heartbeat_interval=0s")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	err = db.Ping()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "heartbeat_interval must be positive")
}

func TestSSLCertPath(t *testing.T) {
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

func TestWithoutSSLCertPath(t *testing.T) {
	db, err := sql.Open("trino", "https://localhost:9")
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	assert.NoError(t, db.Ping())
}

func TestForwardAuthorizationHeaderConfig(t *testing.T) {
	c := &Config{
		ServerURI:                  "https://foobar@localhost:8090",
		ForwardAuthorizationHeader: true,
	}

	dsn, err := c.FormatDSN()
	require.NoError(t, err)

	want := "https://foobar@localhost:8090?forwardAuthorizationHeader=true&source=trino-go-client"

	assert.Equal(t, want, dsn)
}
