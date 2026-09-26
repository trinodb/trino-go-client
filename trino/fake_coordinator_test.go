package trino

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const fakeQueryID = "fake-query"

// fakeCoordinator is an in-process stand-in for a Trino coordinator. It serves
// the pages of a single query, spooled segment downloads and their
// acknowledgements, heartbeats, and query cancellation, and records every
// request it receives so tests can assert on what the driver sent.
type fakeCoordinator struct {
	t      testing.TB
	server *httptest.Server

	mu         sync.Mutex
	pages      []page
	beforePage func(index int, r *http.Request)
	statement  func(w http.ResponseWriter, r *http.Request, query string)
	downloads  map[string]http.HandlerFunc
	heartbeat  http.HandlerFunc
	requests   []capturedRequest
	acks       []string
}

type capturedRequest struct {
	method string
	path   string
	header http.Header
	body   []byte
}

// page is one response in the sequence a query returns: the initial POST to
// /v1/statement, then one GET per nextUri.
type page struct {
	header   http.Header
	response func(baseURL string) any
}

func newFakeCoordinator(t testing.TB) *fakeCoordinator {
	t.Helper()
	return startFakeCoordinator(t, httptest.NewServer)
}

// newFakeTLSCoordinator serves over HTTPS with a certificate the driver
// trusts only when told to through SSLCert or SSLCertPath.
func newFakeTLSCoordinator(t testing.TB) *fakeCoordinator {
	t.Helper()
	return startFakeCoordinator(t, httptest.NewTLSServer)
}

func startFakeCoordinator(t testing.TB, start func(http.Handler) *httptest.Server) *fakeCoordinator {
	t.Helper()
	fc := &fakeCoordinator{
		t:         t,
		downloads: map[string]http.HandlerFunc{},
	}
	fc.server = start(fc)
	t.Cleanup(fc.server.Close)
	return fc
}

// newFakeTLSCoordinatorForHost serves a certificate for hostname, not for
// the address the driver dials.
func newFakeTLSCoordinatorForHost(t testing.TB, hostname string) *fakeCoordinator {
	t.Helper()
	cert, _, _ := generateSelfSignedCert(t, hostname, []string{hostname}, nil)
	fc := &fakeCoordinator{t: t, downloads: map[string]http.HandlerFunc{}}
	server := httptest.NewUnstartedServer(fc)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	t.Cleanup(server.Close)
	fc.server = server
	return fc
}

type fakeMTLSCoordinator struct {
	*fakeCoordinator
	clientCertPEM string
	clientKeyPEM  string
}

// newFakeMTLSCoordinator requires the client certificate it generates.
func newFakeMTLSCoordinator(t testing.TB) *fakeMTLSCoordinator {
	t.Helper()
	serverCert, _, _ := generateSelfSignedCert(t, "fake-coordinator", nil, []net.IP{net.ParseIP("127.0.0.1")})
	_, clientCertPEM, clientKeyPEM := generateSelfSignedCert(t, "fake-client", nil, nil)

	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM([]byte(clientCertPEM))

	fc := &fakeCoordinator{t: t, downloads: map[string]http.HandlerFunc{}}
	server := httptest.NewUnstartedServer(fc)
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	fc.server = server

	return &fakeMTLSCoordinator{fakeCoordinator: fc, clientCertPEM: clientCertPEM, clientKeyPEM: clientKeyPEM}
}

func (fc *fakeMTLSCoordinator) clientCertificatePEM() string { return fc.clientCertPEM }
func (fc *fakeMTLSCoordinator) clientPrivateKeyPEM() string  { return fc.clientKeyPEM }

// generateSelfSignedCert returns the certificate and its PEM and key PEM.
func generateSelfSignedCert(t testing.TB, commonName string, dnsNames []string, ips []net.IP) (cert tls.Certificate, certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           ips,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEMBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEMBytes := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	cert, err = tls.X509KeyPair(certPEMBytes, keyPEMBytes)
	require.NoError(t, err)

	return cert, string(certPEMBytes), string(keyPEMBytes)
}

func (fc *fakeCoordinator) url() string {
	return fc.server.URL
}

// certificatePEM returns the server certificate of a TLS fake.
func (fc *fakeCoordinator) certificatePEM() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fc.server.Certificate().Raw}))
}

// open returns a database handle pointed at the fake; params is appended to
// the DSN and must start with "?" when not empty.
func (fc *fakeCoordinator) open(t testing.TB, params string) *sql.DB {
	t.Helper()
	db, err := sql.Open("trino", fc.url()+params)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// respond sets the pages served for the next query. Every page but the last
// gets a nextUri pointing at the following one.
func (fc *fakeCoordinator) respond(pages ...page) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.pages = pages
}

// serveSegment makes a spooled segment download return body with status 200.
func (fc *fakeCoordinator) serveSegment(name string, body []byte) {
	fc.handleSegment(name, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
}

// handleSegment routes a spooled segment download to handler.
func (fc *fakeCoordinator) handleSegment(name string, handler http.HandlerFunc) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.downloads[name] = handler
}

// onStatement answers the initial POST itself, for queries whose response
// depends on the statement text rather than on a fixed sequence of pages, as
// transaction control does.
func (fc *fakeCoordinator) onStatement(handler func(w http.ResponseWriter, r *http.Request, query string)) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.statement = handler
}

// onPage runs hook before page index is served, so a test can block or
// cancel a page fetch at a known point.
func (fc *fakeCoordinator) onPage(hook func(index int, r *http.Request)) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.beforePage = hook
}

// onHeartbeat routes every HEAD request to handler; the default answers 200.
func (fc *fakeCoordinator) onHeartbeat(handler http.HandlerFunc) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.heartbeat = handler
}

// capturedRequests returns a copy of every request received so far.
func (fc *fakeCoordinator) capturedRequests() []capturedRequest {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]capturedRequest(nil), fc.requests...)
}

// ackedSegments returns the names of the segments acknowledged so far.
func (fc *fakeCoordinator) ackedSegments() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return append([]string(nil), fc.acks...)
}

func (fc *fakeCoordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	fc.mu.Lock()
	fc.requests = append(fc.requests, capturedRequest{
		method: r.Method,
		path:   r.URL.Path,
		header: r.Header.Clone(),
		body:   body,
	})
	heartbeat := fc.heartbeat
	statement := fc.statement
	fc.mu.Unlock()

	switch {
	case r.Method == http.MethodHead:
		if heartbeat == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		heartbeat(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/query/"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
		if statement != nil {
			statement(w, r, string(body))
			return
		}
		fc.servePage(w, r, 0)
	case strings.HasPrefix(r.URL.Path, "/v1/statement/"+fakeQueryID+"/"):
		index, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/v1/statement/"+fakeQueryID+"/"))
		if err != nil {
			fc.unexpected(w, r)
			return
		}
		fc.servePage(w, r, index)
	case strings.HasPrefix(r.URL.Path, "/v1/spooled/download/"):
		fc.serveDownload(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/spooled/ack/"):
		fc.mu.Lock()
		fc.acks = append(fc.acks, strings.TrimPrefix(r.URL.Path, "/v1/spooled/ack/"))
		fc.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	default:
		fc.unexpected(w, r)
	}
}

func (fc *fakeCoordinator) servePage(w http.ResponseWriter, r *http.Request, index int) {
	fc.mu.Lock()
	pages := fc.pages
	beforePage := fc.beforePage
	fc.mu.Unlock()
	if beforePage != nil {
		beforePage(index, r)
	}
	if index >= len(pages) {
		fc.t.Errorf("fake coordinator: page %d requested but only %d pages were configured", index, len(pages))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	p := pages[index]
	for name, values := range p.header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	response := p.response(fc.url())
	if index+1 < len(pages) {
		setNextURI(response, fc.url()+"/v1/statement/"+fakeQueryID+"/"+strconv.Itoa(index+1))
	}
	fc.writeJSON(w, response)
}

func (fc *fakeCoordinator) serveDownload(w http.ResponseWriter, r *http.Request) {
	fc.mu.Lock()
	handler, ok := fc.downloads[strings.TrimPrefix(r.URL.Path, "/v1/spooled/download/")]
	fc.mu.Unlock()
	if !ok {
		fc.unexpected(w, r)
		return
	}
	handler(w, r)
}

func (fc *fakeCoordinator) unexpected(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
	fc.writeJSON(w, ErrTrino{ErrorName: "Unexpected request", Message: r.Method + " " + r.URL.Path})
}

// writeJSON writes v as the response, encoding any warnings in the
// coordinator's nested wire shape. Warning only implements UnmarshalJSON
// (decoding real responses is what the driver needs); re-encoding them here
// is test-only scaffolding, kept out of the public API.
func (fc *fakeCoordinator) writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(wireEncode(v)); err != nil {
		fc.t.Errorf("fake coordinator: encoding response: %v", err)
	}
}

func setNextURI(response any, nextURI string) {
	switch r := response.(type) {
	case *stmtResponse:
		r.NextURI = nextURI
	case *queryResponse:
		r.NextURI = nextURI
	}
}

// setWarnings attaches ws to response; only *stmtResponse and *queryResponse
// carry warnings.
func setWarnings(response any, ws []Warning) {
	switch r := response.(type) {
	case *stmtResponse:
		r.Warnings = ws
	case *queryResponse:
		r.Warnings = ws
	}
}

// withWarnings attaches ws to the page's response, for tests that assert on
// when warnings are collected.
func (p page) withWarnings(ws ...Warning) page {
	inner := p.response
	p.response = func(baseURL string) any {
		response := inner(baseURL)
		setWarnings(response, ws)
		return response
	}
	return p
}

// wireEncode returns v as is, unless it is a *stmtResponse or *queryResponse
// carrying warnings, in which case it returns a shadow value whose Warnings
// field encodes in the coordinator's nested io.trino.client.Warning shape.
func wireEncode(v any) any {
	switch r := v.(type) {
	case *stmtResponse:
		if len(r.Warnings) == 0 {
			return r
		}
		return struct {
			*stmtResponse
			Warnings []warningWire `json:"warnings"`
		}{r, toWireWarnings(r.Warnings)}
	case *queryResponse:
		if len(r.Warnings) == 0 {
			return r
		}
		return struct {
			*queryResponse
			Warnings []warningWire `json:"warnings"`
		}{r, toWireWarnings(r.Warnings)}
	default:
		return v
	}
}

func toWireWarnings(ws []Warning) []warningWire {
	wires := make([]warningWire, len(ws))
	for i, w := range ws {
		wires[i].WarningCode.Code = w.Code
		wires[i].WarningCode.Name = w.Name
		wires[i].Message = w.Message
	}
	return wires
}

// pageOf serves response as is; its nextUri is filled in by the fake.
func pageOf(response any) page {
	return page{response: func(string) any { return response }}
}

// statementPage is the response to the initial POST: a query ID and nothing else.
func statementPage() page {
	return pageOf(&stmtResponse{ID: fakeQueryID})
}

// resultPage serves one integer column with the given data, which is either
// a [][]interface{} for the direct protocol or a map for the spooling one.
func resultPage(data any) page {
	return columnsPage([]queryColumn{integerColumn("_col0")}, data)
}

func columnsPage(columns []queryColumn, data any) page {
	return pageOf(&queryResponse{
		ID:      fakeQueryID,
		Columns: columns,
		Data:    data,
	})
}

// spooledPage serves the segments under the spooling protocol. Relative uri
// and ackUri values are resolved against the fake's address when served.
func spooledPage(encoding string, segments ...map[string]any) page {
	return page{response: func(baseURL string) any {
		resolved := make([]map[string]any, 0, len(segments))
		for _, segment := range segments {
			resolved = append(resolved, resolveSegmentURIs(segment, baseURL))
		}
		return &queryResponse{
			ID:      fakeQueryID,
			Columns: []queryColumn{integerColumn("_col0")},
			Data: map[string]any{
				"encoding": encoding,
				"segments": resolved,
			},
		}
	}}
}

// emptyPage is a final page that carries no columns and no data.
func emptyPage() page {
	return pageOf(&queryResponse{})
}

func (p page) withHeader(name, value string) page {
	header := p.header.Clone()
	if header == nil {
		header = http.Header{}
	}
	header.Add(name, value)
	p.header = header
	return p
}

func integerColumn(name string) queryColumn {
	return queryColumn{
		Name: name,
		Type: "integer",
		TypeSignature: typeSignature{
			RawType:   "integer",
			Arguments: []typeArgument{},
		},
	}
}

func timestampColumn(name string) queryColumn {
	return queryColumn{
		Name: name,
		Type: "timestamp(3)",
		TypeSignature: typeSignature{
			RawType:   "timestamp",
			Arguments: []typeArgument{{Kind: KIND_LONG, Value: json.RawMessage("3")}},
		},
	}
}

// spooledSegment describes a segment downloaded from the fake under name.
func spooledSegment(name string, metadata any) map[string]any {
	return map[string]any{
		"type":     "spooled",
		"uri":      "/v1/spooled/download/" + name,
		"ackUri":   "/v1/spooled/ack/" + name,
		"metadata": metadata,
		"headers":  map[string]any{"test": []any{"test"}},
	}
}

// inlineSegment describes a segment whose base64 encoded data travels with the page.
func inlineSegment(data any, metadata any) map[string]any {
	return map[string]any{
		"type":     "inline",
		"data":     data,
		"metadata": metadata,
	}
}

func withField(segment map[string]any, key string, value any) map[string]any {
	copied := copySegment(segment)
	copied[key] = value
	return copied
}

func withoutField(segment map[string]any, keys ...string) map[string]any {
	copied := copySegment(segment)
	for _, key := range keys {
		delete(copied, key)
	}
	return copied
}

func copySegment(segment map[string]any) map[string]any {
	copied := make(map[string]any, len(segment))
	for key, value := range segment {
		copied[key] = value
	}
	return copied
}

func resolveSegmentURIs(segment map[string]any, baseURL string) map[string]any {
	resolved := copySegment(segment)
	for _, key := range []string{"uri", "ackUri"} {
		if value, ok := resolved[key].(string); ok && strings.HasPrefix(value, "/") {
			resolved[key] = baseURL + value
		}
	}
	return resolved
}

// collectInts scans every remaining row as a single integer column.
func collectInts(t testing.TB, rows *sql.Rows) []int {
	t.Helper()
	var results []int
	for rows.Next() {
		var value int
		require.NoError(t, rows.Scan(&value))
		results = append(results, value)
	}
	return results
}
