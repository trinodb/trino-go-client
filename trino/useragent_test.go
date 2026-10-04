package trino

import (
	"regexp"
	"runtime/debug"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserAgentOnEveryRequest(t *testing.T) {
	t.Parallel()
	fc := newFakeCoordinator(t)
	fc.respond(statementPage(), spooledPage("json",
		spooledSegment("seg0", map[string]any{"segmentSize": 7, "rowOffset": 0, "rowsCount": 1}),
	))
	fc.serveSegment("seg0", []byte("[[100]]"))
	db := fc.open(t, "")

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{100}, collectInts(t, rows))
	require.NoError(t, rows.Err())
	require.Eventually(t, func() bool {
		return slices.Contains(fc.ackedSegments(), "seg0")
	}, 5*time.Second, time.Millisecond)

	userAgents := map[string]string{}
	for _, req := range fc.capturedRequests() {
		userAgents[req.method+" "+req.path] = req.header.Get("User-Agent")
	}
	assert.Equal(t, map[string]string{
		"POST /v1/statement":                      userAgent,
		"GET /v1/statement/" + fakeQueryID + "/1": userAgent,
		"GET /v1/spooled/download/seg0":           userAgent,
		"GET /v1/spooled/ack/seg0":                userAgent,
	}, userAgents)
}

func TestUserAgentOnExternalAuthenticationRequests(t *testing.T) {
	t.Parallel()
	fc := newFakeTLSCoordinator(t)
	fc.respond(statementPage(), resultPage([][]any{{1}}))
	newFakeExternalAuth(fc, "token1")
	db := openExternalAuth(t, fc, &Config{})

	rows, err := db.Query("SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, collectInts(t, rows))

	requests := fc.capturedRequests()
	require.Len(t, requests, 5, "rejected statement, token poll, token delete, retried statement, nextUri")
	for _, req := range requests {
		assert.Equal(t, userAgent, req.header.Get("User-Agent"), "%s %s", req.method, req.path)
	}
}

func TestUserAgentFormat(t *testing.T) {
	t.Parallel()
	assert.Regexp(t, regexp.MustCompile(`^trino-go-client/v1\.2\.3 os=[a-zA-Z0-9_.-]+ arch=[a-zA-Z0-9_.-]+ lang/go=[a-zA-Z0-9_.-]+$`), createUserAgent("v1.2.3"))
	// unit tests are built from this module's checkout, which has no version
	assert.Equal(t, createUserAgent("unknown"), userAgent)
}

func TestDriverVersion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{name: "no build information", ok: false, want: "unknown"},
		{
			name: "dependency",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: "github.com/stretchr/testify", Version: "v1.10.0"},
					{Path: "github.com/trinodb/trino-go-client", Version: "v0.333.0"},
				},
			},
			ok:   true,
			want: "v0.333.0",
		},
		{
			name: "dependency replaced by another version",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{
					Path:    "github.com/trinodb/trino-go-client",
					Version: "v0.333.0",
					Replace: &debug.Module{Path: "github.com/example/trino-go-client", Version: "v0.334.0-fork"},
				}},
			},
			ok:   true,
			want: "v0.334.0-fork",
		},
		{
			name: "dependency replaced by a local directory",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{
					Path:    "github.com/trinodb/trino-go-client",
					Version: "v0.333.0",
					Replace: &debug.Module{Path: "../trino-go-client"},
				}},
			},
			ok:   true,
			want: "unknown",
		},
		{
			name: "main module built from a checkout",
			info: &debug.BuildInfo{Main: debug.Module{Path: "github.com/trinodb/trino-go-client", Version: "(devel)"}},
			ok:   true,
			want: "unknown",
		},
		{
			name: "main module built from a tag",
			info: &debug.BuildInfo{Main: debug.Module{Path: "github.com/trinodb/trino-go-client", Version: "v0.333.0"}},
			ok:   true,
			want: "v0.333.0",
		},
		{
			name: "module missing",
			info: &debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}},
			ok:   true,
			want: "unknown",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, driverVersion(tc.info, tc.ok))
		})
	}
}
