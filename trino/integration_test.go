// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ahmetb/dlog"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/golang-jwt/jwt/v5"
	dt "github.com/ory/dockertest/v3"
	docker "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

const (
	DockerLocalStackName = "localstack"
	bucketName           = "spooling"
	DockerTrinoName      = "trino-go-client-tests"
	MAXRetries           = 10
	TrinoNetwork         = "trino-network"
)

var (
	pool               *dt.Pool
	trinoResource      *dt.Resource
	localStackResource *dt.Resource

	trinoImageTagFlag = flag.String(
		"trino_image_tag",
		os.Getenv("TRINO_IMAGE_TAG"),
		"Docker image tag used for the Trino server container",
	)
	integrationServerFlag = flag.String(
		"trino_server_dsn",
		os.Getenv("TRINO_SERVER_DSN"),
		"dsn of the Trino server used for integration tests instead of starting a Docker container",
	)
	integrationServerQueryTimeout = flag.Duration(
		"trino_query_timeout",
		5*time.Second,
		"max duration for Trino queries to run before giving up",
	)
	noCleanup = flag.Bool(
		"no_cleanup",
		false,
		"do not delete containers on exit",
	)
	tlsServer = ""
)

func TestMain(m *testing.M) {
	flag.Parse()
	DefaultQueryTimeout = *integrationServerQueryTimeout
	DefaultCancelQueryTimeout = *integrationServerQueryTimeout
	if *trinoImageTagFlag == "" {
		*trinoImageTagFlag = "latest"
	}

	var spoolingProtocolSupported bool
	if *trinoImageTagFlag == "latest" {
		spoolingProtocolSupported = true
	} else {
		version, err := strconv.Atoi(*trinoImageTagFlag)
		if err != nil {
			log.Fatalf("Invalid trino_image_tag: %s", *trinoImageTagFlag)
		}
		spoolingProtocolSupported = version >= 466
	}

	var err error
	if *integrationServerFlag == "" && !testing.Short() {
		pool, err = dt.NewPool("")
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}
		pool.MaxWait = 1 * time.Minute

		networkID := getOrCreateNetwork(pool)

		wd, err := os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get working directory: %s", err)
		}

		var ok bool
		if spoolingProtocolSupported {
			localStackResource = getOrCreateLocalStack(pool, networkID)
		}

		trinoResource, ok = pool.ContainerByName(DockerTrinoName)

		if !ok {
			err = generateCerts(wd + "/etc/secrets")
			if err != nil {
				log.Fatalf("Could not generate TLS certificates: %s", err)
			}

			mounts := []string{
				wd + "/etc/catalog:/etc/trino/catalog",
				wd + "/etc/secrets:/etc/trino/secrets",
				wd + "/etc/jvm.config:/etc/trino/jvm.config",
				wd + "/etc/node.properties:/etc/trino/node.properties",
				wd + "/etc/password-authenticator.properties:/etc/trino/password-authenticator.properties",
			}

			if spoolingProtocolSupported {
				mounts = append(mounts, wd+"/etc/config.properties:/etc/trino/config.properties")
				mounts = append(mounts, wd+"/etc/spooling-manager.properties:/etc/trino/spooling-manager.properties")
			} else {
				mounts = append(mounts, wd+"/etc/config-pre-466version.properties:/etc/trino/config.properties")
			}
			trinoResource, err = pool.RunWithOptions(&dt.RunOptions{
				Name:       DockerTrinoName,
				Repository: "trinodb/trino",
				Tag:        *trinoImageTagFlag,
				Mounts:     mounts,
				ExposedPorts: []string{
					"8080/tcp",
					"8443/tcp",
				},
				NetworkID: networkID,
			}, func(hc *docker.HostConfig) {
				hc.Ulimits = []docker.ULimit{
					{
						Name: "nofile",
						Hard: 4096,
						Soft: 4096,
					},
				}
			})
			if err != nil {
				log.Fatalf("Could not start resource: %s", err)
			}
		} else if !trinoResource.Container.State.Running {
			pool.Client.StartContainer(trinoResource.Container.ID, nil)
		}

		waitForContainerHealth(trinoResource.Container.ID, "trino")

		*integrationServerFlag = "http://test@localhost:" + trinoResource.GetPort("8080/tcp")
		tlsServer = "https://admin:admin@localhost:" + trinoResource.GetPort("8443/tcp")

		http.DefaultTransport.(*http.Transport).TLSClientConfig, err = getTLSConfig(wd + "/etc/secrets")
		if err != nil {
			log.Fatalf("Failed to set the default TLS config: %s", err)
		}
	}

	code := m.Run()

	if !*noCleanup && pool != nil {
		if trinoResource != nil {
			if err := pool.Purge(trinoResource); err != nil {
				log.Fatalf("Could not purge resource: %s", err)
			}
		}

		if localStackResource != nil {
			if err := pool.Purge(localStackResource); err != nil {
				log.Fatalf("Could not purge LocalStack resource: %s", err)
			}
		}

		networkExists, networkID, err := networkExists(pool, TrinoNetwork)
		if err == nil && networkExists {
			if err := pool.Client.RemoveNetwork(networkID); err != nil {
				log.Fatalf("Could not remove Docker network: %s", err)
			}
		}
	}

	os.Exit(code)
}

func getOrCreateLocalStack(pool *dt.Pool, networkID string) *dt.Resource {
	resource, ok := pool.ContainerByName(DockerLocalStackName)
	if ok {
		return resource
	}

	newResource, err := setupLocalStack(pool, networkID)
	if err != nil {
		log.Fatalf("Failed to start LocalStack: %s", err)
	}

	return newResource
}

func getOrCreateNetwork(pool *dt.Pool) string {
	networkExists, networkID, err := networkExists(pool, TrinoNetwork)
	if err != nil {
		log.Fatalf("Could not check if Docker network exists: %s", err)
	}

	if networkExists {
		return networkID
	}

	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{
		Name: TrinoNetwork,
	})
	if err != nil {
		log.Fatalf("Could not create Docker network: %s", err)
	}

	return network.ID
}

func networkExists(pool *dt.Pool, networkName string) (bool, string, error) {
	networks, err := pool.Client.ListNetworks()
	if err != nil {
		return false, "", fmt.Errorf("could not list Docker networks: %w", err)
	}
	for _, network := range networks {
		if network.Name == networkName {
			return true, network.ID, nil
		}
	}
	return false, "", nil
}

func setupLocalStack(pool *dt.Pool, networkID string) (*dt.Resource, error) {
	localstackResource, err := pool.RunWithOptions(&dt.RunOptions{
		Name:       DockerLocalStackName,
		Repository: "localstack/localstack",
		Tag:        "latest",
		Env: []string{
			"SERVICES=s3",
			"region_name=us-east-1",
			"AWS_ACCESS_KEY_ID=test",
			"AWS_SECRET_ACCESS_KEY=test",
		},

		PortBindings: map[docker.Port][]docker.PortBinding{
			"4566/tcp": {{HostIP: "0.0.0.0", HostPort: "4566"}},
			"4571/tcp": {{HostIP: "0.0.0.0", HostPort: "4571"}},
		},

		NetworkID: networkID,
	})
	if err != nil {
		return nil, fmt.Errorf("could not start LocalStack: %w", err)
	}

	localstackPort := localstackResource.GetPort("4566/tcp")
	s3Endpoint := "http://localhost:" + localstackPort

	log.Println("LocalStack started at:", s3Endpoint)

	waitForContainerHealth(localstackResource.Container.ID, "localstack")

	for retry := 0; retry < MAXRetries; retry++ {
		err := createS3Bucket(s3Endpoint, "test", "test", bucketName)
		if err == nil {
			log.Println("S3 bucket created successfully")
			return localstackResource, nil
		}
		log.Printf("Failed to create S3 bucket, retrying... (%d/%d)\n", retry+1, MAXRetries)
		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("failed to create S3 bucket after multiple attempts: %w", err)
}

func createS3Bucket(endpoint, accessKey, secretKey, bucketName string) error {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.New(s3.Options{
		Credentials:  cfg.Credentials,
		Region:       "us-east-1",
		BaseEndpoint: &endpoint,
		UsePathStyle: *aws.Bool(true),
	})

	createBucketInput := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err = s3Client.CreateBucket(context.TODO(), createBucketInput)
	if err != nil {
		return fmt.Errorf("failed to create S3 bucket: %w", err)
	}

	log.Printf("Bucket %s created successfully!", bucketName)
	return nil
}

func waitForContainerHealth(containerID, containerName string) {
	if err := pool.Retry(func() error {
		c, err := pool.Client.InspectContainer(containerID)
		if err != nil {
			log.Fatalf("Failed to inspect container %s: %s", containerID, err)
		}
		if !c.State.Running {
			log.Fatalf("Container %s is not running: %s\nContainer logs:\n%s", containerID, c.State.String(), getLogs(trinoResource.Container.ID))
		}
		log.Printf("Waiting for %s container: %s\n", containerName, c.State.String())
		if c.State.Health.Status != "healthy" {
			return errors.New("Not ready")
		}
		return nil
	}); err != nil {
		log.Fatalf("Timed out waiting for container %s to get ready: %s\nContainer logs:\n%s", containerName, err, getLogs(containerID))
	}
}

func generateCerts(dir string) error {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %w", err)
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Trino Software Foundation"},
		},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("unable to marshal private key: %w", err)
	}
	privBlock := &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}
	err = writePEM(dir+"/private_key.pem", privBlock)
	if err != nil {
		return err
	}

	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return fmt.Errorf("unable to marshal public key: %w", err)
	}
	pubBlock := &pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes}
	err = writePEM(dir+"/public_key.pem", pubBlock)
	if err != nil {
		return err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}
	certBlock := &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}
	err = writePEM(dir+"/certificate.pem", certBlock)
	if err != nil {
		return err
	}

	err = writePEM(dir+"/certificate_with_key.pem", certBlock, privBlock, pubBlock)
	if err != nil {
		return err
	}

	return nil
}

func writePEM(filename string, blocks ...*pem.Block) error {
	// all files are world-readable, so they can be read inside the Trino container
	out, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to open %s for writing: %w", filename, err)
	}
	for _, block := range blocks {
		if err := pem.Encode(out, block); err != nil {
			return fmt.Errorf("failed to write %s data to %s: %w", block.Type, filename, err)
		}
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("error closing %s: %w", filename, err)
	}
	return nil
}

func getTLSConfig(dir string) (*tls.Config, error) {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to read the system cert pool: %s", err)
	}
	caCertPEM, err := os.ReadFile(dir + "/certificate.pem")
	if err != nil {
		return nil, fmt.Errorf("failed to read the certificate: %s", err)
	}
	ok := certPool.AppendCertsFromPEM(caCertPEM)
	if !ok {
		return nil, fmt.Errorf("failed to parse the certificate: %s", err)
	}
	return &tls.Config{
		RootCAs: certPool,
	}, nil
}

func getLogs(id string) []byte {
	var buf bytes.Buffer
	pool.Client.Logs(docker.LogsOptions{
		Container:    id,
		OutputStream: &buf,
		ErrorStream:  &buf,
		Stdout:       true,
		Stderr:       true,
		RawTerminal:  true,
	})
	logs, _ := io.ReadAll(dlog.NewReader(&buf))
	return logs
}

// integrationOpen opens a connection to the integration test server.
func integrationOpen(t *testing.T, dsn ...string) *sql.DB {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	target := *integrationServerFlag
	if len(dsn) > 0 {
		target = dsn[0]
	}
	db, err := sql.Open("trino", target)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// integration tests based on python tests:
// https://github.com/trinodb/trino-python-client/tree/master/integration_tests

type nodesRow struct {
	NodeID      string
	HTTPURI     string
	NodeVersion string
	Coordinator bool
	State       string
}

func TestIntegrationSelectQueryIterator(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM system.runtime.nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col nodesRow
		err = rows.Scan(
			&col.NodeID,
			&col.HTTPURI,
			&col.NodeVersion,
			&col.Coordinator,
			&col.State,
		)
		if err != nil {
			t.Fatal(err)
		}
		if col.NodeID != "test" {
			t.Errorf("Expected node_id == test but got %s", col.NodeID)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count < 1 {
		t.Error("no rows returned")
	}
}

func TestIntegrationSelectQueryNoResult(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	row := db.QueryRow("SELECT * FROM system.runtime.nodes where false")
	var col nodesRow
	err := row.Scan(
		&col.NodeID,
		&col.HTTPURI,
		&col.NodeVersion,
		&col.Coordinator,
		&col.State,
	)
	if err == nil {
		t.Fatalf("unexpected query returning data: %+v", col)
	}
}

func TestIntegrationSelectFailedQuery(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM catalog.schema.do_not_exist")
	if err == nil {
		rows.Close()
		t.Fatal("query to invalid catalog succeeded")
	}
	queryFailed, ok := err.(*ErrQueryFailed)
	if !ok {
		t.Fatal("unexpected error:", err)
	}
	trinoErr, ok := errors.Unwrap(queryFailed).(*ErrTrino)
	if !ok {
		t.Fatal("unexpected error:", trinoErr)
	}
	expected := ErrTrino{
		Message:   "line 1:15: Catalog 'catalog'",
		SqlState:  "",
		ErrorCode: 44,
		ErrorName: "CATALOG_NOT_FOUND",
		ErrorType: "USER_ERROR",
		ErrorLocation: ErrorLocation{
			LineNumber:   1,
			ColumnNumber: 15,
		},
		FailureInfo: FailureInfo{
			Type:    "io.trino.spi.TrinoException",
			Message: "line 1:15: Catalog 'catalog'",
		},
	}
	if !strings.HasPrefix(trinoErr.Message, expected.Message) {
		t.Fatalf("expected ErrTrino.Message to start with `%s`, got: %s", expected.Message, trinoErr.Message)
	}
	if trinoErr.SqlState != expected.SqlState {
		t.Fatalf("expected ErrTrino.SqlState to be `%s`, got: %s", expected.SqlState, trinoErr.SqlState)
	}
	if trinoErr.ErrorCode != expected.ErrorCode {
		t.Fatalf("expected ErrTrino.ErrorCode to be `%d`, got: %d", expected.ErrorCode, trinoErr.ErrorCode)
	}
	if trinoErr.ErrorName != expected.ErrorName {
		t.Fatalf("expected ErrTrino.ErrorName to be `%s`, got: %s", expected.ErrorName, trinoErr.ErrorName)
	}
	if trinoErr.ErrorType != expected.ErrorType {
		t.Fatalf("expected ErrTrino.ErrorType to be `%s`, got: %s", expected.ErrorType, trinoErr.ErrorType)
	}
	if trinoErr.ErrorLocation.LineNumber != expected.ErrorLocation.LineNumber {
		t.Fatalf("expected ErrTrino.ErrorLocation.LineNumber to be `%d`, got: %d", expected.ErrorLocation.LineNumber, trinoErr.ErrorLocation.LineNumber)
	}
	if trinoErr.ErrorLocation.ColumnNumber != expected.ErrorLocation.ColumnNumber {
		t.Fatalf("expected ErrTrino.ErrorLocation.ColumnNumber to be `%d`, got: %d", expected.ErrorLocation.ColumnNumber, trinoErr.ErrorLocation.ColumnNumber)
	}
	if trinoErr.FailureInfo.Type != expected.FailureInfo.Type {
		t.Fatalf("expected ErrTrino.FailureInfo.Type to be `%s`, got: %s", expected.FailureInfo.Type, trinoErr.FailureInfo.Type)
	}
	if !strings.HasPrefix(trinoErr.FailureInfo.Message, expected.FailureInfo.Message) {
		t.Fatalf("expected ErrTrino.FailureInfo.Message to start with `%s`, got: %s", expected.FailureInfo.Message, trinoErr.FailureInfo.Message)
	}
}

type tpchRow struct {
	CustKey    int
	Name       string
	Address    string
	NationKey  int
	Phone      string
	AcctBal    float64
	MktSegment string
	Comment    string
}

func TestIntegrationSelectTpch1000(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	rows, err := db.Query("SELECT * FROM tpch.sf1.customer LIMIT 1000")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var col tpchRow
		err = rows.Scan(
			&col.CustKey,
			&col.Name,
			&col.Address,
			&col.NationKey,
			&col.Phone,
			&col.AcctBal,
			&col.MktSegment,
			&col.Comment,
		)
		if err != nil {
			t.Fatal(err)
		}
		/*
			if col.CustKey == 1 && col.AcctBal != 711.56 {
				t.Fatal("unexpected acctbal for custkey=1:", col.AcctBal)
			}
		*/
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
	if count != 1000 {
		t.Fatal("not enough rows returned:", count)
	}
}

func TestIntegrationSelectCancelQuery(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	deadline := time.Now().Add(200 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT * FROM tpch.sf1.customer")
	if err != nil {
		goto handleErr
	}
	defer rows.Close()
	for rows.Next() {
		var col tpchRow
		err = rows.Scan(
			&col.CustKey,
			&col.Name,
			&col.Address,
			&col.NationKey,
			&col.Phone,
			&col.AcctBal,
			&col.MktSegment,
			&col.Comment,
		)
		if err != nil {
			break
		}
	}
	if err = rows.Err(); err == nil {
		t.Fatal("unexpected query with deadline succeeded")
	}
handleErr:
	errmsg := err.Error()
	for _, msg := range []string{"cancel", "deadline"} {
		if strings.Contains(errmsg, msg) {
			return
		}
	}
	t.Fatal("unexpected error:", err)
}

func TestIntegrationSessionProperties(t *testing.T) {
	dsn := *integrationServerFlag
	dsn += "?session_properties=query_max_run_time%3A10m%3Bquery_priority%3A2"
	db := integrationOpen(t, dsn)
	defer db.Close()
	rows, err := db.Query("SHOW SESSION")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		col := struct {
			Name        string
			Value       string
			Default     string
			Type        string
			Description string
		}{}
		err = rows.Scan(
			&col.Name,
			&col.Value,
			&col.Default,
			&col.Type,
			&col.Description,
		)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case col.Name == "query_max_run_time" && col.Value != "10m":
			t.Fatal("unexpected value for query_max_run_time:", col.Value)
		case col.Name == "query_priority" && col.Value != "2":
			t.Fatal("unexpected value for query_priority:", col.Value)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationTypeConversion(t *testing.T) {
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	dsn := *integrationServerFlag
	dsn += "?custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	var (
		goTime            time.Time
		nullTime          NullTime
		goBytes           []byte
		nullBytes         []byte
		goString          string
		nullString        sql.NullString
		nullStringSlice   NullSliceString
		nullStringSlice2  NullSlice2String
		nullStringSlice3  NullSlice3String
		nullInt64Slice    NullSliceInt64
		nullInt64Slice2   NullSlice2Int64
		nullInt64Slice3   NullSlice3Int64
		nullFloat64Slice  NullSliceFloat64
		nullFloat64Slice2 NullSlice2Float64
		nullFloat64Slice3 NullSlice3Float64
		goMap             map[string]interface{}
		nullMap           NullMap
		goRow             []interface{}
	)
	err = db.QueryRow(`
		SELECT
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST(NULL AS TIMESTAMP),
			CAST(X'FFFF0FFF3FFFFFFF' AS VARBINARY),
			CAST(NULL AS VARBINARY),
			CAST('string' AS VARCHAR),
			CAST(NULL AS VARCHAR),
			ARRAY['A', 'B', NULL],
			ARRAY[ARRAY['A'], NULL],
			ARRAY[ARRAY[ARRAY['A'], NULL], NULL],
			ARRAY[1, 2, NULL],
			ARRAY[ARRAY[1, 1, 1], NULL],
			ARRAY[ARRAY[ARRAY[1, 1, 1], NULL], NULL],
			ARRAY[1.0, 2.0, NULL],
			ARRAY[ARRAY[1.1, 1.1, 1.1], NULL],
			ARRAY[ARRAY[ARRAY[1.1, 1.1, 1.1], NULL], NULL],
			MAP(ARRAY['a', 'b'], ARRAY['c', 'd']),
			CAST(NULL AS MAP(ARRAY(INTEGER), ARRAY(INTEGER))),
			ROW(1, 'a', CAST('2017-07-10 01:02:03.004 UTC' AS TIMESTAMP(6) WITH TIME ZONE), ARRAY['c'])
	`).Scan(
		&goTime,
		&nullTime,
		&goBytes,
		&nullBytes,
		&goString,
		&nullString,
		&nullStringSlice,
		&nullStringSlice2,
		&nullStringSlice3,
		&nullInt64Slice,
		&nullInt64Slice2,
		&nullInt64Slice3,
		&nullFloat64Slice,
		&nullFloat64Slice2,
		&nullFloat64Slice3,
		&goMap,
		&nullMap,
		&goRow,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Compare the actual and expected values.
	expectedTime := time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC)
	if !goTime.Equal(expectedTime) {
		t.Errorf("expected GoTime to be %v, got %v", expectedTime, goTime)
	}

	expectedBytes := []byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff}
	if !bytes.Equal(goBytes, expectedBytes) {
		t.Errorf("expected GoBytes to be %v, got %v", expectedBytes, goBytes)
	}

	if nullBytes != nil {
		t.Errorf("expected NullBytes to be nil, got %v", nullBytes)
	}

	if goString != "string" {
		t.Errorf("expected GoString to be %q, got %q", "string", goString)
	}

	if nullString.Valid {
		t.Errorf("expected NullString.Valid to be false, got true")
	}

	if !reflect.DeepEqual(nullStringSlice.SliceString, []sql.NullString{{String: "A", Valid: true}, {String: "B", Valid: true}, {Valid: false}}) {
		t.Errorf("expected NullStringSlice.SliceString to be %v, got %v",
			[]sql.NullString{{String: "A", Valid: true}, {String: "B", Valid: true}, {Valid: false}},
			nullStringSlice.SliceString)
	}
	if !nullStringSlice.Valid {
		t.Errorf("expected NullStringSlice.Valid to be true, got false")
	}

	expectedSlice2String := [][]sql.NullString{{{String: "A", Valid: true}}, {}}
	if !reflect.DeepEqual(nullStringSlice2.Slice2String, expectedSlice2String) {
		t.Errorf("expected NullStringSlice2.Slice2String to be %v, got %v", expectedSlice2String, nullStringSlice2.Slice2String)
	}
	if !nullStringSlice2.Valid {
		t.Errorf("expected NullStringSlice2.Valid to be true, got false")
	}

	expectedSlice3String := [][][]sql.NullString{{{{String: "A", Valid: true}}, {}}, {}}
	if !reflect.DeepEqual(nullStringSlice3.Slice3String, expectedSlice3String) {
		t.Errorf("expected NullStringSlice3.Slice3String to be %v, got %v", expectedSlice3String, nullStringSlice3.Slice3String)
	}
	if !nullStringSlice3.Valid {
		t.Errorf("expected NullStringSlice3.Valid to be true, got false")
	}

	expectedSliceInt64 := []sql.NullInt64{{Int64: 1, Valid: true}, {Int64: 2, Valid: true}, {Valid: false}}
	if !reflect.DeepEqual(nullInt64Slice.SliceInt64, expectedSliceInt64) {
		t.Errorf("expected NullInt64Slice.SliceInt64 to be %v, got %v", expectedSliceInt64, nullInt64Slice.SliceInt64)
	}
	if !nullInt64Slice.Valid {
		t.Errorf("expected NullInt64Slice.Valid to be true, got false")
	}

	expectedSlice2Int64 := [][]sql.NullInt64{{{Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}}, {}}
	if !reflect.DeepEqual(nullInt64Slice2.Slice2Int64, expectedSlice2Int64) {
		t.Errorf("expected NullInt64Slice2.Slice2Int64 to be %v, got %v", expectedSlice2Int64, nullInt64Slice2.Slice2Int64)
	}
	if !nullInt64Slice2.Valid {
		t.Errorf("expected NullInt64Slice2.Valid to be true, got false")
	}

	expectedSlice3Int64 := [][][]sql.NullInt64{{{{Int64: 1, Valid: true}, {Int64: 1, Valid: true}, {Int64: 1, Valid: true}}, {}}, {}}
	if !reflect.DeepEqual(nullInt64Slice3.Slice3Int64, expectedSlice3Int64) {
		t.Errorf("expected NullInt64Slice3.Slice3Int64 to be %v, got %v", expectedSlice3Int64, nullInt64Slice3.Slice3Int64)
	}
	if !nullInt64Slice3.Valid {
		t.Errorf("expected NullInt64Slice3.Valid to be true, got false")
	}

	expectedSliceFloat64 := []sql.NullFloat64{{Float64: 1.0, Valid: true}, {Float64: 2.0, Valid: true}, {Valid: false}}
	if !reflect.DeepEqual(nullFloat64Slice.SliceFloat64, expectedSliceFloat64) {
		t.Errorf("expected NullFloat64Slice.SliceFloat64 to be %v, got %v", expectedSliceFloat64, nullFloat64Slice.SliceFloat64)
	}
	if !nullFloat64Slice.Valid {
		t.Errorf("expected NullFloat64Slice.Valid to be true, got false")
	}

	expectedSlice2Float64 := [][]sql.NullFloat64{{{Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}}, {}}
	if !reflect.DeepEqual(nullFloat64Slice2.Slice2Float64, expectedSlice2Float64) {
		t.Errorf("expected NullFloat64Slice2.Slice2Float64 to be %v, got %v", expectedSlice2Float64, nullFloat64Slice2.Slice2Float64)
	}
	if !nullFloat64Slice2.Valid {
		t.Errorf("expected NullFloat64Slice2.Valid to be true, got false")
	}

	expectedSlice3Float64 := [][][]sql.NullFloat64{{{{Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}, {Float64: 1.1, Valid: true}}, {}}, {}}
	if !reflect.DeepEqual(nullFloat64Slice3.Slice3Float64, expectedSlice3Float64) {
		t.Errorf("expected NullFloat64Slice3.Slice3Float64 to be %v, got %v", expectedSlice3Float64, nullFloat64Slice3.Slice3Float64)
	}
	if !nullFloat64Slice3.Valid {
		t.Errorf("expected NullFloat64Slice3.Valid to be true, got false")
	}

	expectedMap := map[string]interface{}{"a": "c", "b": "d"}
	if !reflect.DeepEqual(goMap, expectedMap) {
		t.Errorf("expected GoMap to be %v, got %v", expectedMap, goMap)
	}

	if nullMap.Valid {
		t.Errorf("expected NullMap.Valid to be false, got true")
	}

	expectedRow := []interface{}{json.Number("1"), "a", "2017-07-10 01:02:03.004000 UTC", []interface{}{"c"}}
	if !reflect.DeepEqual(goRow, expectedRow) {
		t.Errorf("expected GoRow to be %v, got %v", expectedRow, goRow)
	}
}

func TestComplexTypes(t *testing.T) {
	// This test has been created to showcase some issues with parsing
	// complex types. It is not intended to be a comprehensive test of
	// the parsing logic, but rather to provide a reference for future
	// changes to the parsing logic.
	//
	// The current implementation of the parsing logic reads the value
	// in the same format as the JSON response from Trino. This means
	// that we don't go further to parse values as their structured types.
	// For example, a row like `ROW(1, X'0000')` is read as
	// a list of a `json.Number(1)` and a base64-encoded string.
	t.Skip("skipping failing test")

	dsn := *integrationServerFlag
	db := integrationOpen(t, dsn)

	for _, tt := range []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "row containing scalar values",
			query:    `SELECT ROW(1, 'a', X'0000')`,
			expected: []interface{}{1, "a", []byte{0x00, 0x00}},
		},
		{
			name:     "nested row",
			query:    `SELECT ROW(ROW(1, 'a'), ROW(2, 'b'))`,
			expected: []interface{}{[]interface{}{1, "a"}, []interface{}{2, "b"}},
		},
		{
			name:     "map with scalar values",
			query:    `SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2])`,
			expected: map[string]interface{}{"a": 1, "b": 2},
		},
		{
			name:     "map with nested row",
			query:    `SELECT MAP(ARRAY['a', 'b'], ARRAY[ROW(1, 'a'), ROW(2, 'b')])`,
			expected: map[string]interface{}{"a": []interface{}{1, "a"}, "b": []interface{}{2, "b"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var result interface{}
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIntegrationArgsConversion(t *testing.T) {
	dsn := *integrationServerFlag
	db := integrationOpen(t, dsn)
	value := 0
	err := db.QueryRow(`
		SELECT 1 FROM (VALUES (
			CAST(1 AS TINYINT),
			CAST(1 AS SMALLINT),
			CAST(1 AS INTEGER),
			CAST(1 AS BIGINT),
			CAST(1 AS REAL),
			CAST(1 AS DOUBLE),
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST('string' AS VARCHAR),
			CAST(X'FFFF0FFF3FFFFFFF' AS VARBINARY),
			ARRAY['A', 'B']
			)) AS t(col_tiny, col_small, col_int, col_big, col_real, col_double, col_ts, col_varchar, col_varbinary, col_array )
			WHERE 1=1
			AND col_tiny = ?
			AND col_small = ?
			AND col_int = ?
			AND col_big = ?
			AND col_real = cast(? as real)
			AND col_double = cast(? as double)
			AND col_ts = ?
			AND col_varchar = ?
			AND col_varbinary = ?
			AND col_array = ?`,
		int16(1),
		int16(1),
		int32(1),
		int64(1),
		Numeric("1"),
		Numeric("1"),
		time.Date(2017, 7, 10, 1, 2, 3, 4*1000000, time.UTC),
		"string",
		[]byte{0xff, 0xff, 0x0f, 0xff, 0x3f, 0xff, 0xff, 0xff},
		[]string{"A", "B"},
	).Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationNoResults(t *testing.T) {
	db := integrationOpen(t)
	rows, err := db.Query("SELECT 1 LIMIT 0")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		t.Fatal(errors.New("Rows returned"))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}
func TestRoleHeaderSupport(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		rawDSN      string
		expectError bool
		errorSubstr string
	}{
		{
			name: "Valid roles via Config",
			config: Config{
				ServerURI: *integrationServerFlag,
				Roles:     map[string]string{"tpch": "role1", "memory": "role2"},
			},
			expectError: false,
		},
		{
			name:        "Valid roles via DSN, not encoded url",
			rawDSN:      *integrationServerFlag + "?roles=tpch:role1;memory:role2",
			expectError: false,
		},
		{
			name:        "Valid roles via DSN, url encoded",
			rawDSN:      *integrationServerFlag + "?roles%3Dtpch%3Arole1%3Bmemory%3Arole2",
			expectError: false,
		},
		{
			name: "Non-existent catalog role",
			config: Config{
				ServerURI: *integrationServerFlag,
				Roles:     map[string]string{"not-exist-catalog": "role1"},
			},
			expectError: true,
			errorSubstr: "USER_ERROR: Catalog",
		},
		{
			name:        "Invalid role format missing ROLE{}",
			rawDSN:      *integrationServerFlag + "?roles=catolog%3Drole1",
			expectError: true,
			errorSubstr: "Invalid role format: catolog=role1",
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
			_, err = db.Query("SELECT 1")

			if tt.expectError {
				require.Error(t, err)
				if tt.errorSubstr != "" {
					require.Contains(t, err.Error(), tt.errorSubstr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIntegrationQueryParametersSelect(t *testing.T) {
	scenarios := []struct {
		name          string
		query         string
		args          []interface{}
		expectedError error
		expectedRows  int
	}{
		{
			name:         "valid string as varchar",
			query:        "SELECT * FROM system.runtime.nodes WHERE system.runtime.nodes.node_id=?",
			args:         []interface{}{"test"},
			expectedRows: 1,
		},
		{
			name:         "valid int as bigint",
			query:        "SELECT * FROM tpch.sf1.customer WHERE custkey=? LIMIT 2",
			args:         []interface{}{int(1)},
			expectedRows: 1,
		},
		{
			name:          "invalid string as bigint",
			query:         "SELECT * FROM tpch.sf1.customer WHERE custkey=? LIMIT 2",
			args:          []interface{}{"1"},
			expectedError: errors.New(`trino: query failed (200 OK): "USER_ERROR: line 1:46: Cannot apply operator: bigint = varchar(1)"`),
		},
		{
			name:          "valid string as date",
			query:         "SELECT * FROM tpch.sf1.lineitem WHERE shipdate=? LIMIT 2",
			args:          []interface{}{"1995-01-27"},
			expectedError: errors.New(`trino: query failed (200 OK): "USER_ERROR: line 1:47: Cannot apply operator: date = varchar(10)"`),
		},
	}

	for i := range scenarios {
		scenario := scenarios[i]

		t.Run(scenario.name, func(t *testing.T) {
			db := integrationOpen(t)
			defer db.Close()

			rows, err := db.Query(scenario.query, scenario.args...)
			if err != nil {
				if scenario.expectedError == nil {
					t.Errorf("Unexpected err: %s", err)
					return
				}
				if err.Error() == scenario.expectedError.Error() {
					return
				}
				t.Errorf("Expected err to be %s but got %s", scenario.expectedError, err)
			}

			if scenario.expectedError != nil {
				t.Error("missing expected error")
				return
			}

			defer rows.Close()

			var count int
			for rows.Next() {
				count++
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != scenario.expectedRows {
				t.Errorf("expecting %d rows, got %d", scenario.expectedRows, count)
			}
		})
	}
}

func TestIntegrationQueryNextAfterClose(t *testing.T) {
	// NOTE: This is testing invalid behaviour. It ensures that we don't
	// panic if we call driverRows.Next after we closed the driverStmt.

	ctx := context.Background()
	conn, err := (&Driver{}).Open(*integrationServerFlag)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("Failed preparing query: %v", err)
	}

	rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, []driver.NamedValue{})
	if err != nil {
		t.Fatalf("Failed running query: %v", err)
	}
	defer rows.Close()

	stmt.Close() // NOTE: the important bit.

	var result driver.Value
	if err := rows.Next([]driver.Value{result}); err != nil {
		t.Fatalf("unexpected result: %+v, no error was expected", err)
	}
	if err := rows.Next([]driver.Value{result}); err != io.EOF {
		t.Fatalf("unexpected result: %+v, expected io.EOF", err)
	}
}

func TestIntegrationExec(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()

	_, err := db.Query(`SELECT count(*) FROM nation`)
	expected := "Schema must be specified when session schema is not set"
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Expected to fail to execute query with error: %v, got: %v", expected, err)
	}

	result, err := db.Exec("USE tpch.sf100")
	if err != nil {
		t.Fatal("Failed executing query:", err.Error())
	}
	if result == nil {
		t.Fatal("Expected exec result to be not nil")
	}

	a, err := result.RowsAffected()
	if err != nil {
		t.Fatal("Expected RowsAffected not to return any error, got:", err)
	}
	if a != 0 {
		t.Fatal("Expected RowsAffected to be zero, got:", a)
	}
	rows, err := db.Query(`SELECT count(*) FROM nation`)
	if err != nil {
		t.Fatal("Failed executing query:", err.Error())
	}
	if rows == nil || !rows.Next() {
		t.Fatal("Failed fetching results")
	}
}

func TestIntegrationUnsupportedHeader(t *testing.T) {
	dsn := *integrationServerFlag
	dsn += "?catalog=tpch&schema=sf10"
	db := integrationOpen(t, dsn)
	defer db.Close()
	cases := []struct {
		query string
		err   error
	}{
		{
			query: "SET ROLE dummy",
			err:   errors.New(`trino: query failed (200 OK): "USER_ERROR: line 1:1: Role 'dummy' does not exist"`),
		},
		{
			query: "SET PATH dummy",
			err:   errors.New(`trino: query failed (200 OK): "USER_ERROR: SET PATH not supported by client"`),
		},
	}
	for _, c := range cases {
		_, err := db.Query(c.query)
		if err == nil || err.Error() != c.err.Error() {
			t.Fatal("unexpected error:", err)
		}
	}
}

func TestIntegrationQueryContext(t *testing.T) {
	tests := []struct {
		name           string
		timeout        time.Duration
		expectedErrMsg string
	}{
		{
			name:           "Context Cancellation",
			timeout:        0,
			expectedErrMsg: "canceled",
		},
		{
			name:           "Context Deadline Exceeded",
			timeout:        3 * time.Second,
			expectedErrMsg: "context deadline exceeded",
		},
	}

	if err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}}); err != nil {
		t.Fatal(err)
	}

	dsn := *integrationServerFlag + "?catalog=tpch&schema=sf100&source=cancel-test&custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	defer db.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ctx context.Context
			var cancel context.CancelFunc

			if tt.timeout == 0 {
				ctx, cancel = context.WithCancel(context.Background())
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), tt.timeout)
			}
			defer cancel()

			errCh := make(chan error, 1)
			done := make(chan struct{})
			longQuery := "SELECT COUNT(*) FROM lineitem"

			go func() {
				// query will complete in ~7s unless cancelled
				rows, err := db.QueryContext(ctx, longQuery)
				if err != nil {
					errCh <- err
					return
				}
				defer rows.Close()

				rows.Next()
				if err = rows.Err(); err != nil {
					errCh <- err
					return
				}
				close(done)
			}()

			// Poll system.runtime.queries to get the query ID
			var queryID string
			pollCtx, pollCancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer pollCancel()

			for {
				row := db.QueryRowContext(pollCtx, "SELECT query_id FROM system.runtime.queries WHERE state = 'RUNNING' AND source = 'cancel-test' AND query = ?", longQuery)
				err := row.Scan(&queryID)
				if err == nil {
					break
				}
				if err != sql.ErrNoRows {
					t.Fatal("failed to read query ID:", err)
				}
				if err = contextSleep(pollCtx, 100*time.Millisecond); err != nil {
					t.Fatal("query did not start in 1 second")
				}
			}

			if tt.timeout == 0 {
				cancel()
			}

			// Wait for the query to be canceled or completed
			select {
			case <-done:
				t.Fatal("unexpected query succeeded despite cancellation or deadline")
			case err := <-errCh:
				if !strings.Contains(err.Error(), tt.expectedErrMsg) {
					t.Fatalf("expected error containing %q, but got: %v", tt.expectedErrMsg, err)
				}
			}

			// Poll system.runtime.queries to verify the query was canceled
			pollCtx, pollCancel = context.WithTimeout(context.Background(), 2*time.Second)
			defer pollCancel()

			for {
				row := db.QueryRowContext(pollCtx, "SELECT state, error_code FROM system.runtime.queries WHERE query_id = ?", queryID)
				var state string
				var code *string
				err := row.Scan(&state, &code)
				if err != nil {
					t.Fatal("failed to read query state:", err)
				}
				if state == "FAILED" && code != nil && *code == "USER_CANCELED" {
					return
				}
				if err = contextSleep(pollCtx, 100*time.Millisecond); err != nil {
					t.Fatalf("query was not canceled in 2 seconds; state: %s, code: %v, err: %v", state, code, err)
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

func contextSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	}
}

func TestIntegrationDayToHourIntervalMilliPrecision(t *testing.T) {
	db := integrationOpen(t)
	defer db.Close()
	tests := []struct {
		name    string
		arg     time.Duration
		wantErr bool
	}{
		{
			name:    "valid 1234567891s",
			arg:     time.Duration(1234567891) * time.Second,
			wantErr: false,
		},
		{
			name:    "valid 123456789.1s",
			arg:     time.Duration(123456789100) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid 12345678.91s",
			arg:     time.Duration(12345678910) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid 1234567.891s",
			arg:     time.Duration(1234567891) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid -1234567891s",
			arg:     time.Duration(-1234567891) * time.Second,
			wantErr: false,
		},
		{
			name:    "valid -123456789.1s",
			arg:     time.Duration(-123456789100) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid -12345678.91s",
			arg:     time.Duration(-12345678910) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "valid -1234567.891s",
			arg:     time.Duration(-1234567891) * time.Millisecond,
			wantErr: false,
		},
		{
			name:    "invalid 1234567891.2s",
			arg:     time.Duration(1234567891200) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid 123456789.12s",
			arg:     time.Duration(123456789120) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid 12345678.912s",
			arg:     time.Duration(12345678912) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid -1234567891.2s",
			arg:     time.Duration(-1234567891200) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid -123456789.12s",
			arg:     time.Duration(-123456789120) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid -12345678.912s",
			arg:     time.Duration(-12345678912) * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "invalid max seconds (9223372036)",
			arg:     time.Duration(math.MaxInt64) / time.Second * time.Second,
			wantErr: true,
		},
		{
			name:    "invalid min seconds (-9223372036)",
			arg:     time.Duration(math.MinInt64) / time.Second * time.Second,
			wantErr: true,
		},
		{
			name: "valid max seconds (2147483647)",
			arg:  math.MaxInt32 * time.Second,
		},
		{
			name: "valid min seconds (-2147483647)",
			arg:  -math.MaxInt32 * time.Second,
		},
		{
			name: "valid max minutes (153722867)",
			arg:  time.Duration(math.MaxInt64) / time.Minute * time.Minute,
		},
		{
			name: "valid min minutes (-153722867)",
			arg:  time.Duration(math.MinInt64) / time.Minute * time.Minute,
		},
		{
			name: "valid max hours (2562047)",
			arg:  time.Duration(math.MaxInt64) / time.Hour * time.Hour,
		},
		{
			name: "valid min hours (-2562047)",
			arg:  time.Duration(math.MinInt64) / time.Hour * time.Hour,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := db.Exec("SELECT ?", test.arg)
			if (err != nil) != test.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, test.wantErr)
				return
			}
		})
	}
}

func TestIntegrationLargeQuery(t *testing.T) {
	version, err := strconv.Atoi(*trinoImageTagFlag)
	if (err != nil && *trinoImageTagFlag != "latest") || (err == nil && version < 418) {
		t.Skip("Skipping test when not using Trino 418 or later.")
	}
	dsn := *integrationServerFlag
	dsn += "?explicitPrepare=false"
	db := integrationOpen(t, dsn)
	defer db.Close()
	rows, err := db.Query("SELECT ?, '"+strings.Repeat("a", 5000000)+"'", 42)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if rows.Err() != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("not enough rows returned:", count)
	}
}

func TestIntegrationTypeConversionSpoolingProtocolInlineJsonEncoder(t *testing.T) {
	err := RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		t.Fatal(err)
	}
	dsn := *integrationServerFlag
	dsn += "?custom_client=uncompressed"
	db := integrationOpen(t, dsn)
	var (
		goTime            time.Time
		nullTime          NullTime
		goString          string
		nullString        sql.NullString
		nullStringSlice   NullSliceString
		nullStringSlice2  NullSlice2String
		nullStringSlice3  NullSlice3String
		nullInt64Slice    NullSliceInt64
		nullInt64Slice2   NullSlice2Int64
		nullInt64Slice3   NullSlice3Int64
		nullFloat64Slice  NullSliceFloat64
		nullFloat64Slice2 NullSlice2Float64
		nullFloat64Slice3 NullSlice3Float64
		goMap             map[string]interface{}
		nullMap           NullMap
		goRow             []interface{}
	)
	err = db.QueryRow(`
		SELECT
			TIMESTAMP '2017-07-10 01:02:03.004 UTC',
			CAST(NULL AS TIMESTAMP),
			CAST('string' AS VARCHAR),
			CAST(NULL AS VARCHAR),
			ARRAY['A', 'B', NULL],
			ARRAY[ARRAY['A'], NULL],
			ARRAY[ARRAY[ARRAY['A'], NULL], NULL],
			ARRAY[1, 2, NULL],
			ARRAY[ARRAY[1, 1, 1], NULL],
			ARRAY[ARRAY[ARRAY[1, 1, 1], NULL], NULL],
			ARRAY[1.0, 2.0, NULL],
			ARRAY[ARRAY[1.1, 1.1, 1.1], NULL],
			ARRAY[ARRAY[ARRAY[1.1, 1.1, 1.1], NULL], NULL],
			MAP(ARRAY['a', 'b'], ARRAY['c', 'd']),
			CAST(NULL AS MAP(ARRAY(INTEGER), ARRAY(INTEGER))),
			ROW(1, 'a', CAST('2017-07-10 01:02:03.004 UTC' AS TIMESTAMP(6) WITH TIME ZONE), ARRAY['c'])
	`, sql.Named(trinoEncoding, "json")).Scan(
		&goTime,
		&nullTime,
		&goString,
		&nullString,
		&nullStringSlice,
		&nullStringSlice2,
		&nullStringSlice3,
		&nullInt64Slice,
		&nullInt64Slice2,
		&nullInt64Slice3,
		&nullFloat64Slice,
		&nullFloat64Slice2,
		&nullFloat64Slice3,
		&goMap,
		&nullMap,
		&goRow,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationSelectTpchSpoolingSegments(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		encoding string
		expected int
	}{
		// Testing with a LIMIT of 1001 rows.
		// Since we exceed the `protocol.spooling.inlining.max-rows` threshold (1000),
		// this query trigger spooling protocol with spooled segments.
		{
			name:     "Spooled Segment JSON+ZSTD Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 1001",
			encoding: "json+zstd",
			expected: 1001,
		},
		{
			name:     "Spooled Segment JSON Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 1001",
			encoding: "json",
			expected: 1001,
		},
		{
			name:     "Spooled Segment JSON+LZ4 Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 1001",
			encoding: "json+lz4",
			expected: 1001,
		},
		// Testing with a LIMIT of 100 rows.
		// This should remain inline as it is below the `protocol.spooling.inlining.max-rows` (1000) and bellow `protocol.spooling.inlining.max-size` 128kb
		{
			name:     "Inline Segment JSON+ZSTD Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 100",
			encoding: "json+zstd",
			expected: 100,
		},
		{
			name:     "Inline Segment JSON+LZ4 Encoded",
			query:    "SELECT * FROM tpch.sf1.customer LIMIT 100",
			encoding: "json+lz4",
			expected: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := integrationOpen(t)
			defer db.Close()

			rows, err := db.Query(tt.query, sql.Named(trinoEncoding, tt.encoding))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
				var col tpchRow
				err = rows.Scan(
					&col.CustKey,
					&col.Name,
					&col.Address,
					&col.NationKey,
					&col.Phone,
					&col.AcctBal,
					&col.MktSegment,
					&col.Comment,
				)
				if err != nil {
					t.Fatalf("Row scan failed: %v", err)
				}
			}

			if rows.Err() != nil {
				t.Fatalf("Rows iteration error: %v", rows.Err())
			}

			if count != tt.expected {
				t.Fatalf("Expected %d rows, got %d", tt.expected, count)
			}
		})
	}
}
