package trino

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/iana/nametype"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testRealm = "EXAMPLE.COM"

func TestKerberosCredentialCache(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	krb5Files := newKerberosTestFiles(t, "alice", "trino/127.0.0.1")

	db := openKerberos(t, fc, Config{
		KerberosConfigPath:          krb5Files.config,
		KerberosCredentialCachePath: krb5Files.credentialCache,
	})
	rows, err := db.Query("SELECT 1")

	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Equal(t, "trino/127.0.0.1", requestedServicePrincipal(t, fc.capturedRequests()[0].header))
}

func TestKerberosCredentialCacheFromEnvironment(t *testing.T) {
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	krb5Files := newKerberosTestFiles(t, "alice", "trino/127.0.0.1")
	t.Setenv("KRB5CCNAME", "FILE:"+krb5Files.credentialCache)

	db := openKerberos(t, fc, Config{KerberosConfigPath: krb5Files.config})
	rows, err := db.Query("SELECT 1")

	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))
	assert.Equal(t, "trino/127.0.0.1", requestedServicePrincipal(t, fc.capturedRequests()[0].header))
}

func TestKerberosCredentialCachePrincipal(t *testing.T) {
	t.Parallel()
	krb5Files := newKerberosTestFiles(t, "alice", "trino/127.0.0.1")

	cases := []struct {
		name      string
		principal string
		realm     string
		wantErr   string
	}{
		{name: "matching name", principal: "alice"},
		{name: "matching name with realm", principal: "alice@" + testRealm, realm: testRealm},
		{name: "other principal", principal: "bob", wantErr: "holds a ticket for alice@EXAMPLE.COM, not for bob"},
		{name: "other realm", realm: "OTHER.COM", wantErr: "holds a ticket for realm EXAMPLE.COM, not for OTHER.COM"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := newKerberosClient(&Config{
				KerberosConfigPath:          krb5Files.config,
				KerberosCredentialCachePath: krb5Files.credentialCache,
				KerberosPrincipal:           tc.principal,
				KerberosRealm:               tc.realm,
			})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestKerberosCredentialCacheErrors(t *testing.T) {
	t.Parallel()
	krb5Files := newKerberosTestFiles(t, "alice", "trino/127.0.0.1")
	corrupt := filepath.Join(t.TempDir(), "corrupt")
	realmLengthPastEnd := []byte{5, 4, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0x7f, 0xff, 0xff, 0xff}
	require.NoError(t, os.WriteFile(corrupt, realmLengthPastEnd, 0o600))
	expired := filepath.Join(t.TempDir(), "expired")
	writeCredentialCache(t, expired, "alice", time.Now().Add(-time.Hour), "krbtgt/"+testRealm)

	cases := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "missing", path: filepath.Join(t.TempDir(), "missing"), wantErr: "Error loading Kerberos credential cache"},
		{name: "corrupt", path: corrupt, wantErr: "malformed credential cache"},
		{name: "expired", path: expired, wantErr: "Error login to KDC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := newKerberosClient(&Config{
				KerberosConfigPath:          krb5Files.config,
				KerberosCredentialCachePath: tc.path,
			})
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestKerberosCredentialCachePathDefault(t *testing.T) {
	cases := []struct {
		name       string
		krb5ccname string
		want       string
		wantErr    string
	}{
		{name: "file type", krb5ccname: "FILE:/tmp/cache", want: "/tmp/cache"},
		{name: "no type", krb5ccname: "/tmp/cache", want: "/tmp/cache"},
		{name: "windows drive", krb5ccname: `C:\cache`, want: `C:\cache`},
		{name: "keyring type", krb5ccname: "KEYRING:persistent:1000", wantErr: "unsupported Kerberos credential cache type KEYRING"},
		{name: "unset", want: "/tmp/krb5cc_" + strconv.Itoa(os.Getuid())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getuid() < 0 && tc.krb5ccname == "" {
				t.Skip("no user id on this platform")
			}
			t.Setenv("KRB5CCNAME", tc.krb5ccname)
			path, err := (&Config{}).kerberosCredentialCachePath()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, path)
		})
	}
}

func TestKerberosKeytabAndCredentialCacheExclusive(t *testing.T) {
	t.Parallel()
	conf := &Config{
		ServerURI:                   "https://localhost:8443",
		KerberosEnabled:             true,
		KerberosKeytabPath:          "/etc/trino.keytab",
		KerberosCredentialCachePath: "/tmp/krb5cc_1000",
	}
	want := "KerberosKeytabPath and KerberosCredentialCachePath cannot be specified together"

	_, err := NewConnector(conf)
	require.ErrorContains(t, err, want)

	db, err := sql.Open("trino", "https://localhost:8443?KerberosEnabled=true&KerberosKeytabPath=/etc/trino.keytab&KerberosCredentialCachePath=/tmp/krb5cc_1000")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.ErrorContains(t, db.Ping(), want)
}

func TestKerberosCredentialCacheDSNRoundTrip(t *testing.T) {
	t.Parallel()
	conf := &Config{
		ServerURI:                   "https://localhost:8443",
		KerberosEnabled:             true,
		KerberosCredentialCachePath: "/tmp/krb5cc_1000",
	}

	dsn, err := conf.FormatDSN()
	require.NoError(t, err)
	parsed, err := ParseDSN(dsn)
	require.NoError(t, err)

	assert.Equal(t, "/tmp/krb5cc_1000", parsed.KerberosCredentialCachePath)
}

func openKerberos(t *testing.T, fc *fakeCoordinator, conf Config) *sql.DB {
	t.Helper()
	conf.ServerURI = fc.url()
	conf.SSLCert = fc.certificatePEM()
	conf.KerberosEnabled = true
	connector, err := NewConnector(&conf)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

type kerberosTestFiles struct {
	config          string
	credentialCache string
}

// newKerberosTestFiles writes a krb5.conf and a credential cache holding a
// TGT for user and a ticket for each service, so the driver builds SPNEGO
// tokens without contacting the unreachable KDC.
func newKerberosTestFiles(t *testing.T, user string, services ...string) kerberosTestFiles {
	t.Helper()
	dir := t.TempDir()
	files := kerberosTestFiles{
		config:          filepath.Join(dir, "krb5.conf"),
		credentialCache: filepath.Join(dir, "krb5cc"),
	}
	krb5Config := "[libdefaults]\n" +
		"  default_realm = " + testRealm + "\n" +
		"  dns_lookup_kdc = false\n" +
		"[realms]\n" +
		"  " + testRealm + " = {\n" +
		"    kdc = 127.0.0.1:1\n" +
		"  }\n"
	require.NoError(t, os.WriteFile(files.config, []byte(krb5Config), 0o600))
	writeCredentialCache(t, files.credentialCache, user, time.Now().Add(time.Hour), append([]string{"krbtgt/" + testRealm}, services...)...)
	return files
}

// writeCredentialCache writes a version 4 cache in the format gokrb5 reads,
// with one credential per service, all ending at endTime.
func writeCredentialCache(t *testing.T, path, user string, endTime time.Time, services ...string) {
	t.Helper()
	var cache bytes.Buffer
	write := func(v any) { require.NoError(t, binary.Write(&cache, binary.BigEndian, v)) }
	writeData := func(b []byte) {
		write(uint32(len(b)))
		cache.Write(b)
	}
	writePrincipal := func(name types.PrincipalName) {
		write(name.NameType)
		write(uint32(len(name.NameString)))
		writeData([]byte(testRealm))
		for _, component := range name.NameString {
			writeData([]byte(component))
		}
	}
	client := types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, user)

	write([]byte{5, 4})
	write(uint16(0))
	writePrincipal(client)
	for _, service := range services {
		serviceName := types.NewPrincipalName(nametype.KRB_NT_SRV_INST, service)
		ticket := messages.Ticket{
			TktVNO:  5,
			Realm:   testRealm,
			SName:   serviceName,
			EncPart: types.EncryptedData{EType: etypeID.AES256_CTS_HMAC_SHA1_96, KVNO: 1, Cipher: []byte("opaque")},
		}
		ticketBytes, err := ticket.Marshal()
		require.NoError(t, err)
		sessionKey := make([]byte, 32)
		_, err = rand.Read(sessionKey)
		require.NoError(t, err)

		writePrincipal(client)
		writePrincipal(serviceName)
		write(uint16(etypeID.AES256_CTS_HMAC_SHA1_96))
		writeData(sessionKey)
		startTime := uint32(endTime.Add(-2 * time.Hour).Unix())
		write([]uint32{startTime, startTime, uint32(endTime.Unix()), uint32(endTime.Unix())})
		write(uint8(0))
		write(uint32(0))
		write(uint32(0))
		write(uint32(0))
		writeData(ticketBytes)
		writeData(nil)
	}
	require.NoError(t, os.WriteFile(path, cache.Bytes(), 0o600))
}

// requestedServicePrincipal returns the service the SPNEGO token in the
// Authorization header carries a ticket for.
func requestedServicePrincipal(t *testing.T, header http.Header) string {
	t.Helper()
	encoded, ok := strings.CutPrefix(header.Get(authorizationHeader), "Negotiate ")
	require.True(t, ok, "no SPNEGO token in %q", header.Get(authorizationHeader))
	token, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	var spnegoToken spnego.SPNEGOToken
	require.NoError(t, spnegoToken.Unmarshal(token))
	var krb5Token spnego.KRB5Token
	require.NoError(t, krb5Token.Unmarshal(spnegoToken.NegTokenInit.MechTokenBytes))
	return krb5Token.APReq.Ticket.SName.PrincipalNameString()
}
