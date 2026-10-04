package trino

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPProxy(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	proxy := newFakeHTTPProxy(t)

	db := fc.open(t, "?httpProxy="+proxy.address())
	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))

	requests := proxy.received()
	require.NotEmpty(t, requests)
	assert.Equal(t, http.MethodPost+" "+fc.url()+"/v1/statement", requests[0])
	assert.Len(t, requests, len(fc.capturedRequests()), "every request goes through the proxy")
}

func TestHTTPProxyTunnelsTLS(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	proxy := newFakeHTTPProxy(t)

	connector, err := NewConnector(&Config{ServerURI: fc.url(), SSLCert: fc.certificatePEM(), HTTPProxy: proxy.address()})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))

	coordinator, err := url.Parse(fc.url())
	require.NoError(t, err)
	assert.Contains(t, proxy.received(), http.MethodConnect+" "+coordinator.Host)
}

func TestSOCKSProxy(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	proxy := newFakeSOCKS5Proxy(t)

	db := fc.open(t, "?socksProxy="+proxy.address())
	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))

	coordinator, err := url.Parse(fc.url())
	require.NoError(t, err)
	assert.Contains(t, proxy.received(), coordinator.Host)
}

func TestProxyRejected(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		config  *Config
		wantErr string
	}{
		"both proxies": {
			config:  &Config{ServerURI: "http://localhost", HTTPProxy: "proxy:8080", SOCKSProxy: "proxy:1080"},
			wantErr: "socksProxy cannot be used when httpProxy is set",
		},
		"HTTPClient": {
			config:  &Config{ServerURI: "http://localhost", HTTPProxy: "proxy:8080", HTTPClient: &http.Client{}},
			wantErr: "cannot be combined with HTTPClient or a custom client",
		},
		"custom client": {
			config:  &Config{ServerURI: "http://localhost", SOCKSProxy: "proxy:1080", CustomClientName: "any"},
			wantErr: "cannot be combined with HTTPClient or a custom client",
		},
		"missing port": {
			config:  &Config{ServerURI: "http://localhost", HTTPProxy: "proxy"},
			wantErr: `httpProxy must be host:port, got "proxy"`,
		},
		"invalid port": {
			config:  &Config{ServerURI: "http://localhost", SOCKSProxy: "proxy:socks"},
			wantErr: `socksProxy must be host:port, got "proxy:socks"`,
		},
		"missing host": {
			config:  &Config{ServerURI: "http://localhost", HTTPProxy: ":8080"},
			wantErr: `httpProxy must be host:port, got ":8080"`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewConnector(tc.config)
			assert.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestProxyDSNRejected(t *testing.T) {
	t.Parallel()
	_, err := ParseDSN("http://localhost:8080?httpProxy=proxy")
	assert.ErrorContains(t, err, `httpProxy must be host:port, got "proxy"`)

	_, err = ParseDSN("http://localhost:8080?socksProxy=proxy%3A99999")
	assert.ErrorContains(t, err, `socksProxy must be host:port, got "proxy:99999"`)

	db, err := sql.Open("trino", "http://localhost:8080?httpProxy=proxy%3A8080&socksProxy=proxy%3A1080")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assert.ErrorContains(t, db.Ping(), "socksProxy cannot be used when httpProxy is set")
}

// Without a proxy, http.DefaultTransport keeps taking it from the environment.
func TestProxyTransport(t *testing.T) {
	t.Parallel()
	conn, err := newConnFromConfig(&Config{ServerURI: "http://localhost:8080"}, nil)
	require.NoError(t, err)
	assert.Nil(t, conn.httpClient.Transport, "http.DefaultTransport applies")

	conn, err = newConnFromConfig(&Config{ServerURI: "http://localhost:8080", HTTPProxy: "proxy:3128"}, nil)
	require.NoError(t, err)
	transport, ok := conn.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	proxyURL, err := transport.Proxy(httptest.NewRequest(http.MethodGet, "http://localhost:8080/v1/statement", nil))
	require.NoError(t, err)
	assert.Equal(t, "http://proxy:3128", proxyURL.String())
}

// fakeProxy records the requests, or the CONNECT and SOCKS5 targets, it
// forwards.
type fakeProxy struct {
	listener net.Addr
	mu       sync.Mutex
	requests []string
}

func (p *fakeProxy) address() string {
	return p.listener.String()
}

func (p *fakeProxy) record(request string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, request)
}

func (p *fakeProxy) received() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.requests...)
}

func newFakeHTTPProxy(t testing.TB) *fakeProxy {
	t.Helper()
	proxy := &fakeProxy{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			proxy.record(r.Method + " " + r.Host)
			tunnel(w, r.Host)
			return
		}
		proxy.record(r.Method + " " + r.URL.String())
		forward(w, r)
	}))
	t.Cleanup(server.Close)
	proxy.listener = server.Listener.Addr()
	return proxy
}

// forward sends a request with an absolute URI on to its target.
func forward(w http.ResponseWriter, r *http.Request) {
	outgoing := r.Clone(r.Context())
	outgoing.RequestURI = ""
	resp, err := http.DefaultTransport.RoundTrip(outgoing)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for name, values := range resp.Header {
		w.Header()[name] = values
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func tunnel(w http.ResponseWriter, target string) {
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
	client, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		upstream.Close()
		return
	}
	pipe(client, upstream)
}

// newFakeSOCKS5Proxy accepts the no-authentication method and the CONNECT
// command only, which is all the driver uses.
func newFakeSOCKS5Proxy(t testing.TB) *fakeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	proxy := &fakeProxy{listener: listener.Addr()}
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go proxy.serveSOCKS5(client)
		}
	}()
	return proxy
}

func (p *fakeProxy) serveSOCKS5(client net.Conn) {
	target, err := readSOCKS5Connect(client)
	if err != nil {
		client.Close()
		return
	}
	p.record(target)
	upstream, err := net.Dial("tcp", target)
	if err != nil {
		_, _ = client.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
		client.Close()
		return
	}
	if _, err := client.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		client.Close()
		upstream.Close()
		return
	}
	pipe(client, upstream)
}

// readSOCKS5Connect completes the RFC 1928 greeting and returns the
// host:port of the CONNECT request.
func readSOCKS5Connect(client net.Conn) (string, error) {
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(client, greeting); err != nil {
		return "", err
	}
	if _, err := io.ReadFull(client, make([]byte, greeting[1])); err != nil {
		return "", err
	}
	if _, err := client.Write([]byte{5, 0}); err != nil {
		return "", err
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		return "", err
	}
	if header[1] != 1 {
		return "", errors.New("only CONNECT is supported")
	}
	var host string
	switch header[3] {
	case 1:
		address := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(client, address); err != nil {
			return "", err
		}
		host = net.IP(address).String()
	case 3:
		length := make([]byte, 1)
		if _, err := io.ReadFull(client, length); err != nil {
			return "", err
		}
		name := make([]byte, length[0])
		if _, err := io.ReadFull(client, name); err != nil {
			return "", err
		}
		host = string(name)
	default:
		return "", errors.New("unsupported address type")
	}
	port := make([]byte, 2)
	if _, err := io.ReadFull(client, port); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port)))), nil
}

func pipe(client, upstream net.Conn) {
	go func() {
		_, _ = io.Copy(upstream, client)
		upstream.Close()
	}()
	_, _ = io.Copy(client, upstream)
	client.Close()
}
