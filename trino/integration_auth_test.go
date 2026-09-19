package trino

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestRoleHeaderSupport(t *testing.T) {
	version, err := strconv.Atoi(*trinoImageTagFlag)
	if (err != nil && *trinoImageTagFlag != "latest") || (err == nil && version < 458) {
		t.Skip("Skipping test when not using Trino 458 or later.")
	}
	tests := []struct {
		name         string
		config       Config
		rawDSN       string
		query        string
		expectError  bool
		errorSubstr  string
		validateRows func(t *testing.T, rows *sql.Rows)
	}{
		{
			name: "Valid hive admin role via Config",
			config: Config{
				ServerURI: *integrationServerFlag,
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
			config: Config{
				ServerURI: *integrationServerFlag,
				Roles:     map[string]string{"tpch": "NONE", "memory": "ALL"},
			},
			query:       "SELECT 1",
			expectError: false,
		},
		{
			name: "Valid special roles via Config",
			config: Config{
				ServerURI: *integrationServerFlag,
				Roles:     map[string]string{"tpch": "NONE", "memory": "ALL"},
			},
			query:       "SELECT 1",
			expectError: false,
		},
		{
			name:        "Valid hive admin role via DSN, not encoded url",
			rawDSN:      *integrationServerFlag + "?roles=hive:admin",
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
			rawDSN:      *integrationServerFlag + "?roles=hive:admin",
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
			config: Config{
				ServerURI: *integrationServerFlag,
			},
			query:       "SHOW ROLES FROM hive",
			expectError: true,
			errorSubstr: "Access Denied",
		},
		{
			name: "Wrong role - should fail to show roles",
			config: Config{
				ServerURI: *integrationServerFlag,
				Roles:     map[string]string{"hive": "ALL"},
			},
			query:       "SHOW ROLES FROM hive",
			expectError: true,
			errorSubstr: "Access Denied",
		},
		{
			name: "Non-existent catalog role",
			config: Config{
				ServerURI: *integrationServerFlag,
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
				if err != nil {
					t.Fatal(err)
				}
			}

			db := integrationOpen(t, dns)
			defer db.Close()

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

func TestIntegrationAccessToken(t *testing.T) {
	if tlsServer == "" {
		t.Skip("Skipping access token test when using a custom integration server.")
	}

	accessToken, err := generateToken()
	if err != nil {
		t.Fatal(err)
	}

	dsn := tlsServer + "?accessToken=" + accessToken

	db := integrationOpen(t, dsn)

	defer db.Close()
	rows, err := db.Query("SHOW CATALOGS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count < 1 {
		t.Fatal("not enough rows returned:", count)
	}
}

func generateToken() (string, error) {
	privateKeyPEM, err := os.ReadFile("etc/secrets/private_key.pem")
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

	defer db.Close()
	row := db.QueryRow("SELECT 1")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("unexpected count=", count)
	}
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
			dsn := *integrationServerFlag + tt.dsnSuffix
			db := integrationOpen(t, dsn)
			defer db.Close()

			query := "SELECT 1"
			rows, err := db.Query(query)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			if rows.Next() {
			}

			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}

			var queryID string
			err = db.QueryRowContext(context.Background(),
				"SELECT query_id FROM system.runtime.queries WHERE source = ? AND query = ?", tt.source, query,
			).Scan(&queryID)
			if err != nil {
				t.Fatal(err)
			}

			queryInfo, err := getQueryInfo(dsn, queryID)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(queryInfo.Session.ClientTags, tt.expectedTags) {
				t.Fatalf("Expected client tags %v, got %v", tt.expectedTags, queryInfo.Session.ClientTags)
			}
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
			dsn := *integrationServerFlag + tt.dsnSuffix
			db := integrationOpen(t, dsn)
			defer db.Close()

			query := "SELECT 1"
			rows, err := db.Query(query, sql.Named(trinoTagsHeader, tt.Tags))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			if rows.Next() {
			}

			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}

			var queryID string
			err = db.QueryRowContext(context.Background(),
				"SELECT query_id FROM system.runtime.queries WHERE source = ? AND query = ?", tt.source, query,
			).Scan(&queryID)
			if err != nil {
				t.Fatal(err)
			}

			queryInfo, err := getQueryInfo(dsn, queryID)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(queryInfo.Session.ClientTags, tt.expectedTags) {
				t.Fatalf("Expected client tags %v, got %v", tt.expectedTags, queryInfo.Session.ClientTags)
			}
		})
	}
}

type QuerySession struct {
	ClientTags []string `json:"clientTags"`
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
