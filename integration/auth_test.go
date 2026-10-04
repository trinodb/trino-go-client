package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

func TestRoleHeaderSupport(t *testing.T) {
	requireServerVersion(t, 458)
	tests := []struct {
		name         string
		config       trino.Config
		rawDSN       string
		query        string
		expectError  bool
		errorSubstr  string
		validateRows func(t *testing.T, rows *sql.Rows)
	}{
		{
			name: "Valid hive admin role via Config",
			config: trino.Config{
				ServerURI: integrationDSN(t),
				Roles:     map[string]string{"hive": "admin"},
			},
			query:       "SHOW ROLES FROM hive",
			expectError: false,
			validateRows: func(t *testing.T, rows *sql.Rows) {
				foundAdmin := false
				for rows.Next() {
					var roleName string
					err := rows.Scan(&roleName)
					require.NoError(t, err)
					if roleName == "admin" {
						foundAdmin = true
					}
				}
				require.True(t, foundAdmin, "Expected to find 'admin' role in SHOW ROLES output")
			},
		},
		{
			name: "Valid special roles via Config",
			config: trino.Config{
				ServerURI: integrationDSN(t),
				Roles:     map[string]string{"tpch": "NONE", "memory": "ALL"},
			},
			query:       "SELECT 1",
			expectError: false,
		},
		{
			name:        "Valid hive admin role via DSN, not encoded url",
			rawDSN:      integrationDSN(t) + "?roles=hive:admin",
			query:       "SHOW ROLES FROM hive",
			expectError: false,
			validateRows: func(t *testing.T, rows *sql.Rows) {
				foundAdmin := false
				for rows.Next() {
					var roleName string
					err := rows.Scan(&roleName)
					require.NoError(t, err)
					if roleName == "admin" {
						foundAdmin = true
					}
				}
				require.True(t, foundAdmin, "Expected to find 'admin' role in SHOW ROLES output")
			},
		},
		{
			name:        "Valid roles via DSN, url encoded",
			rawDSN:      integrationDSN(t) + "?roles=hive:admin",
			query:       "SHOW ROLES FROM hive",
			expectError: false,
			validateRows: func(t *testing.T, rows *sql.Rows) {
				foundAdmin := false
				for rows.Next() {
					var roleName string
					err := rows.Scan(&roleName)
					require.NoError(t, err)
					if roleName == "admin" {
						foundAdmin = true
					}
				}
				require.True(t, foundAdmin, "Expected to find 'admin' role in SHOW ROLES output")
			},
		},
		{
			name: "No role - should fail to show roles",
			config: trino.Config{
				ServerURI: integrationDSN(t),
			},
			query:       "SHOW ROLES FROM hive",
			expectError: true,
			errorSubstr: "Access Denied",
		},
		{
			name: "Wrong role - should fail to show roles",
			config: trino.Config{
				ServerURI: integrationDSN(t),
				Roles:     map[string]string{"hive": "ALL"},
			},
			query:       "SHOW ROLES FROM hive",
			expectError: true,
			errorSubstr: "Access Denied",
		},
		{
			name: "Non-existent catalog role",
			config: trino.Config{
				ServerURI: integrationDSN(t),
				Roles:     map[string]string{"not-exist-catalog": "role1"},
			},
			query:       "SELECT 1",
			expectError: true,
			errorSubstr: "USER_ERROR: Catalog",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dns string
			var err error

			if tt.rawDSN != "" {
				dns = tt.rawDSN
			} else {
				dns, err = tt.config.FormatDSN()
				require.NoError(t, err)
			}

			db := integrationOpen(t, dns)

			rows, err := db.Query(tt.query)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorSubstr != "" {
					require.Contains(t, err.Error(), tt.errorSubstr)
				}
			} else {
				require.NoError(t, err)
				if tt.validateRows != nil && rows != nil {
					defer rows.Close()
					tt.validateRows(t, rows)
				}
			}
		})
	}
}

// TestRoleNameWithHeaderSeparators selects a role whose name contains the
// characters the server splits X-Trino-Role on, so it only works when the
// whole selected role is URL-encoded.
func TestRoleNameWithHeaderSeparators(t *testing.T) {
	requireServerVersion(t, 458)
	const role = "go_client,role=x}y"
	adminDSN, err := (&trino.Config{ServerURI: integrationDSN(t), Roles: map[string]string{"hive": "admin"}}).FormatDSN()
	require.NoError(t, err)
	admin := integrationOpen(t, adminDSN)
	_, err = admin.Exec(`CREATE ROLE "` + role + `" IN hive`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.Exec(`DROP ROLE "` + role + `" IN hive`)
		require.NoError(t, err)
	})
	_, err = admin.Exec(`GRANT "` + role + `" TO USER test IN hive`)
	require.NoError(t, err)

	t.Run("Config", func(t *testing.T) {
		dsn, err := (&trino.Config{ServerURI: integrationDSN(t), Roles: map[string]string{"hive": role}}).FormatDSN()
		require.NoError(t, err)
		db := integrationOpen(t, dsn)
		assert.Contains(t, currentHiveRoles(t, db), role)
	})
	t.Run("named argument", func(t *testing.T) {
		db := integrationOpen(t)
		assert.Contains(t, currentHiveRoles(t, db, sql.Named("X-Trino-Role", map[string]string{"hive": role})), role)
	})
	t.Run("SET ROLE", func(t *testing.T) {
		db := integrationOpen(t)
		db.SetMaxOpenConns(1)
		_, err := db.Exec(`SET ROLE "` + role + `" IN hive`)
		require.NoError(t, err)
		assert.Contains(t, currentHiveRoles(t, db), role)
	})
}

func currentHiveRoles(t *testing.T, db *sql.DB, args ...any) []string {
	t.Helper()
	rows, err := db.Query("SHOW CURRENT ROLES FROM hive", args...)
	require.NoError(t, err)
	defer rows.Close()
	var roles []string
	for rows.Next() {
		var role string
		require.NoError(t, rows.Scan(&role))
		roles = append(roles, role)
	}
	require.NoError(t, rows.Err())
	return roles
}

func TestIntegrationAccessToken(t *testing.T) {
	if tlsServer == "" {
		t.Skip("Skipping access token test when using a custom integration server.")
	}

	accessToken, err := generateToken()
	require.NoError(t, err)

	dsn := tlsServer + "&accessToken=" + accessToken

	db := integrationOpen(t, dsn)

	rows, err := db.Query("SHOW CATALOGS")
	require.NoError(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.GreaterOrEqual(t, count, 1, "not enough rows returned")
}

func generateToken() (string, error) {
	privateKeyPEM, err := os.ReadFile(filepath.Join(secretsDir, "private_key.pem"))
	if err != nil {
		return "", fmt.Errorf("error reading private key file: %w", err)
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return "", fmt.Errorf("error parsing private key: %w", err)
	}

	// Subject must be 'test'
	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * 365 * time.Hour)),
		Issuer:    "gotrino",
		Subject:   "test",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signedToken, err := token.SignedString(privateKey)

	if err != nil {
		return "", fmt.Errorf("error generating token: %w", err)
	}

	return signedToken, nil
}

func TestIntegrationTLS(t *testing.T) {
	if tlsServer == "" {
		t.Skip("Skipping TLS test when using a custom integration server.")
	}

	dsn := tlsServer
	db := integrationOpen(t, dsn)
	row := db.QueryRow("SELECT 1")
	var count int
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, 1, count)
}

func TestDsnClientTags(t *testing.T) {
	tests := []struct {
		name         string
		dsnSuffix    string
		source       string
		expectedTags []string
	}{
		{
			name:         "Single tag",
			dsnSuffix:    "?clientTags=test&source=single-tag-test",
			source:       "single-tag-test",
			expectedTags: []string{"test"},
		},
		{
			name:         "Multiple tags with special characters",
			dsnSuffix:    "?clientTags=foo+%2520%2Cbar%3Dtest%2Cbaz%23tag&source=multiple-tags-test-special-characters",
			source:       "multiple-tags-test-special-characters",
			expectedTags: []string{"foo %20", "bar=test", "baz#tag"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := integrationDSN(t) + tt.dsnSuffix
			db := integrationOpen(t, dsn)

			query := "SELECT 1"
			rows, err := db.Query(query)
			require.NoError(t, err)
			defer rows.Close()

			rows.Next()
			require.NoError(t, rows.Err())

			var queryID string
			err = db.QueryRowContext(context.Background(),
				"SELECT query_id FROM system.runtime.queries WHERE source = ? AND query = ?", tt.source, query,
			).Scan(&queryID)
			require.NoError(t, err)

			queryInfo, err := getQueryInfo(dsn, queryID)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedTags, queryInfo.Session.ClientTags, "client tags")
		})
	}
}

func TestParametersClientTags(t *testing.T) {
	tests := []struct {
		name         string
		dsnSuffix    string
		Tags         string
		source       string
		expectedTags []string
	}{
		{
			name:         "Single tag",
			dsnSuffix:    "?clientTags=query-parameter-single-tag-test&source=query-parameter-single-tag-test",
			Tags:         "single-tag",
			source:       "query-parameter-single-tag-test",
			expectedTags: []string{"single-tag"},
		},
		{
			name:         "Multiple tags with special characters",
			dsnSuffix:    "?clientTags=query-parameter-multiple-tags-test&source=query-parameter-multiple-tags-test",
			Tags:         "foo %20,bar=test,baz#tag",
			source:       "query-parameter-multiple-tags-test",
			expectedTags: []string{"foo %20", "bar=test", "baz#tag"},
		},
		{
			name:         "Override dsn tags",
			dsnSuffix:    "?clientTags=foo%2B%2520%3Bbar%3Dtest%3Bbaz%23tag&source=query-parameter-override-tags",
			Tags:         "query-parameter-override-tag-test",
			source:       "query-parameter-override-tags",
			expectedTags: []string{"query-parameter-override-tag-test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := integrationDSN(t) + tt.dsnSuffix
			db := integrationOpen(t, dsn)

			query := "SELECT 1"
			rows, err := db.Query(query, sql.Named("X-Trino-Client-Tags", tt.Tags))
			require.NoError(t, err)
			defer rows.Close()

			rows.Next()
			require.NoError(t, rows.Err())

			var queryID string
			err = db.QueryRowContext(context.Background(),
				"SELECT query_id FROM system.runtime.queries WHERE source = ? AND query = ?", tt.source, query,
			).Scan(&queryID)
			require.NoError(t, err)

			queryInfo, err := getQueryInfo(dsn, queryID)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedTags, queryInfo.Session.ClientTags, "client tags")
		})
	}
}

type QuerySession struct {
	ClientTags        []string               `json:"clientTags"`
	ResourceEstimates QueryResourceEstimates `json:"resourceEstimates"`
}

// QueryResourceEstimates holds the durations in seconds.
type QueryResourceEstimates struct {
	ExecutionTime   float64 `json:"executionTime"`
	CPUTime         float64 `json:"cpuTime"`
	PeakMemoryBytes int64   `json:"peakMemoryBytes"`
}
type QueryInfo struct {
	Session QuerySession `json:"session"`
}

func getQueryInfo(dsn, queryId string) (QueryInfo, error) {

	serverURL, err := url.Parse(dsn)
	if err != nil {
		return QueryInfo{}, err
	}
	queryInfoURL := serverURL.Scheme + "://" + serverURL.Host + "/v1/query/" + url.PathEscape(queryId)

	req, err := http.NewRequest("GET", queryInfoURL, nil)
	if err != nil {
		return QueryInfo{}, err
	}
	req.Header.Set("X-Trino-User", serverURL.User.Username())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return QueryInfo{}, err
	}

	defer resp.Body.Close()

	var queryInfo QueryInfo
	if err := json.NewDecoder(resp.Body).Decode(&queryInfo); err != nil {
		return QueryInfo{}, err
	}

	return queryInfo, nil
}
