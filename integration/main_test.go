package integration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	dt "github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"
	"github.com/trinodb/trino-go-client/trino"
)

const (
	// DockerS3Name is the S3 emulator's container name, and the host Trino
	// reaches it by on the shared network.
	DockerS3Name    = "s3"
	bucketName      = "spooling"
	DockerTrinoName = "trino-go-client-tests"
	TrinoNetwork    = "trino-network"
	// DockerDexName is the OAuth2 provider's container name, and its host
	// name on the shared network.
	DockerDexName = "dex"
	// legacyS3Name is the container name older checkouts used for the emulator.
	legacyS3Name = "localstack"

	uncompressedClient = "uncompressed"
	tlsClient          = "integration-tls"
)

var (
	pool           dt.ClosablePool
	trinoContainer dt.ClosableResource
	trinoNetwork   dt.ClosableNetwork
	// secretsDir holds the generated TLS certificate and the password file
	// mounted into the container.
	secretsDir string

	// serverVersion is the numeric Trino version reported by the server under
	// test, whether it runs in the container or behind -trino_server_dsn.
	serverVersion             int
	spoolingProtocolSupported bool

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
	// tlsServer is the DSN of the HTTPS endpoint, using a client that trusts
	// the certificate generated for the container; empty without Docker.
	tlsServer = ""
	// oauth2Addresses maps the host names Trino and Dex know each other by to
	// their published ports; empty unless Dex runs.
	oauth2Addresses map[string]string
)

func TestMain(m *testing.M) {
	flag.Parse()
	if err := trino.RegisterCustomClient(uncompressedClient, &http.Client{Transport: &http.Transport{DisableCompression: true}}); err != nil {
		log.Fatalf("Could not register the %s client: %s", uncompressedClient, err)
	}
	if *trinoImageTagFlag == "" {
		*trinoImageTagFlag = "latest"
	}

	ctx := context.Background()
	if !testing.Short() {
		if *integrationServerFlag == "" {
			startContainers(ctx)
		}
		version, err := detectServerVersion(ctx, *integrationServerFlag)
		if err != nil {
			setupFatal(ctx, "Could not read the Trino version from %s: %s", *integrationServerFlag, err)
		}
		serverVersion = version
		spoolingProtocolSupported = serverVersion >= 466
		log.Printf("Running integration tests against Trino %d", serverVersion)
	}

	code := m.Run()

	releaseDockerResources(ctx)
	os.Exit(code)
}

// startContainers runs Trino, and an S3 emulator when the image supports the
// spooling protocol, and points the integration tests at them.
func startContainers(ctx context.Context) {
	var err error
	pool, err = dt.NewPool(ctx, "", dt.WithMaxWait(1*time.Minute))
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s\nStart Docker, pass -trino_server_dsn to test a running Trino, or pass -short to skip the integration tests", err)
	}
	stopOnSignal(ctx)

	removeExistingContainer(ctx, DockerTrinoName)
	removeExistingContainer(ctx, DockerS3Name)
	removeExistingContainer(ctx, legacyS3Name)
	removeExistingContainer(ctx, DockerDexName)
	trinoNetwork = createNetwork(ctx)

	wd, err := os.Getwd()
	if err != nil {
		setupFatal(ctx, "Failed to get working directory: %s", err)
	}

	imageVersion := imageVersion(ctx)
	if imageVersion >= 466 {
		setupS3Emulator(ctx)
	}

	secretsDir, err = prepareSecrets(wd + "/etc/secrets")
	if err != nil {
		setupFatal(ctx, "Could not prepare the TLS certificates: %s", err)
	}

	mounts := []string{
		secretsDir + ":/etc/trino/secrets",
		wd + "/etc/jvm.config:/etc/trino/jvm.config",
		wd + "/etc/node.properties:/etc/trino/node.properties",
		wd + "/etc/password-authenticator.properties:/etc/trino/password-authenticator.properties",
		wd + "/etc/catalog/memory.properties:/etc/trino/catalog/memory.properties",
		wd + "/etc/catalog/tpch.properties:/etc/trino/catalog/tpch.properties",
	}
	if imageVersion >= 458 {
		mounts = append(mounts,
			wd+"/etc/catalog/hive.properties:/etc/trino/catalog/hive.properties",
			wd+"/etc/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties",
		)
	}
	var dexAddress string
	if imageVersion >= 477 {
		dexAddress = setupDex(ctx, wd+"/etc/dex.yaml")
	}
	switch {
	case imageVersion < 466:
		mounts = append(mounts, wd+"/etc/config-pre-466version.properties:/etc/trino/config.properties")
	case imageVersion < 477:
		mounts = append(mounts, wd+"/etc/config-pre-477version.properties:/etc/trino/config.properties")
	default:
		mounts = append(mounts, wd+"/etc/config.properties:/etc/trino/config.properties")
	}
	if imageVersion >= 466 {
		mounts = append(mounts, wd+"/etc/spooling-manager.properties:/etc/trino/spooling-manager.properties")
	}

	trinoContainer = runContainer(ctx, "trinodb/trino",
		dt.WithName(DockerTrinoName),
		dt.WithTag(*trinoImageTagFlag),
		dt.WithMounts(mounts),
		dt.WithContainerConfig(func(c *container.Config) {
			c.ExposedPorts = network.PortSet{
				network.MustParsePort("8080/tcp"): {},
				network.MustParsePort("8443/tcp"): {},
			}
		}),
		dt.WithHostConfig(func(hc *container.HostConfig) {
			hc.NetworkMode = container.NetworkMode(trinoNetwork.ID())
			hc.Ulimits = []*container.Ulimit{
				{
					Name: "nofile",
					Hard: 4096,
					Soft: 4096,
				},
			}
		}),
	)

	waitForContainerHealth(ctx, trinoContainer, "trino")

	if imageVersion >= 458 {
		if err := grantAdminRoleToTestUser(ctx); err != nil {
			setupFatal(ctx, "Failed to grant the admin role to the test user: %s", err)
		}
	}

	*integrationServerFlag = "http://test@localhost:" + trinoContainer.GetPort("8080/tcp")

	tlsConfig, err := getTLSConfig(secretsDir)
	if err != nil {
		setupFatal(ctx, "Failed to load the TLS config: %s", err)
	}
	if err := trino.RegisterCustomClient(tlsClient, &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}); err != nil {
		setupFatal(ctx, "Could not register the %s client: %s", tlsClient, err)
	}
	tlsServer = "https://admin:admin@localhost:" + trinoContainer.GetPort("8443/tcp") + "?custom_client=" + tlsClient
	if dexAddress != "" {
		oauth2Addresses = map[string]string{
			DockerDexName + ":5556": dexAddress,
			"trino:8443":            "localhost:" + trinoContainer.GetPort("8443/tcp"),
		}
	}
}

// setupDex starts the OAuth2 provider Trino is configured with, which must be
// up before Trino reads its discovery document.
func setupDex(ctx context.Context, configPath string) string {
	dex := runContainer(ctx, "ghcr.io/dexidp/dex",
		dt.WithName(DockerDexName),
		dt.WithTag("v2.45.1"),
		dt.WithMounts([]string{configPath + ":/etc/dex/config.yaml"}),
		dt.WithCmd([]string{"dex", "serve", "/etc/dex/config.yaml"}),
		dt.WithContainerConfig(func(c *container.Config) {
			c.ExposedPorts = network.PortSet{network.MustParsePort("5556/tcp"): {}}
		}),
		dt.WithHostConfig(func(hc *container.HostConfig) {
			hc.NetworkMode = container.NetworkMode(trinoNetwork.ID())
		}),
	)
	address := "localhost:" + dex.GetPort("5556/tcp")
	if err := pool.Retry(ctx, 0, func() error {
		resp, err := http.Get("http://" + address + "/dex/.well-known/openid-configuration")
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("dex discovery returned %s", resp.Status)
		}
		return nil
	}); err != nil {
		setupFatal(ctx, "Timed out waiting for Dex: %s\nContainer logs:\n%s", err, getLogs(ctx, dex))
	}
	return address
}

// imageVersion is the Trino version the -trino_image_tag names, used for the
// decisions that must be made before the container starts; "latest" counts as
// newer than any release.
func imageVersion(ctx context.Context) int {
	if *trinoImageTagFlag == "latest" {
		return math.MaxInt
	}
	version, err := strconv.Atoi(*trinoImageTagFlag)
	if err != nil {
		setupFatal(ctx, "Invalid -trino_image_tag %q: expected \"latest\" or a release number", *trinoImageTagFlag)
	}
	return version
}

var versionPrefix = regexp.MustCompile(`^\d+`)

// detectServerVersion asks the coordinator which Trino release it runs, so
// tests can be gated on the server actually under test rather than on the
// image tag, which says nothing about a server passed with -trino_server_dsn.
func detectServerVersion(ctx context.Context, dsn string) (int, error) {
	dsn, err := addQueryTimeout(dsn)
	if err != nil {
		return 0, err
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var nodeVersion string
	if err := db.QueryRowContext(ctx, "SELECT node_version FROM system.runtime.nodes WHERE coordinator").Scan(&nodeVersion); err != nil {
		return 0, err
	}
	digits := versionPrefix.FindString(nodeVersion)
	if digits == "" {
		return 0, fmt.Errorf("node_version %q does not start with a release number", nodeVersion)
	}
	return strconv.Atoi(digits)
}

// stopOnSignal removes the containers when the run is interrupted, since
// TestMain does not get to run its cleanup then.
func stopOnSignal(ctx context.Context) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signals
		log.Printf("Received %s, stopping", sig)
		releaseDockerResources(ctx)
		os.Exit(130)
	}()
}

// releaseDockerResources removes the containers, the network, and the
// generated secrets, unless -no_cleanup asks to keep them for inspection.
func releaseDockerResources(ctx context.Context) {
	if pool == nil {
		return
	}
	if *noCleanup {
		log.Print("Leaving Docker containers running, as requested by -no_cleanup")
		return
	}
	if err := pool.Close(ctx); err != nil {
		log.Printf("Could not clean up Docker resources: %s", err)
	}
	if secretsDir != "" {
		if err := os.RemoveAll(secretsDir); err != nil {
			log.Printf("Could not remove %s: %s", secretsDir, err)
		}
	}
}

// runContainer starts a container. The pool tracks it and removes it on Close.
func runContainer(ctx context.Context, repository string, opts ...dt.RunOption) dt.ClosableResource {
	resource, err := pool.Run(ctx, repository, opts...)
	if err != nil {
		setupFatal(ctx, "Could not start %s container: %s", repository, err)
	}

	return resource
}

// removeExistingContainer deletes a container left behind by an earlier run. It
// may be unhealthy, or built from a different -trino_image_tag, so every run
// starts from a fresh one.
func removeExistingContainer(ctx context.Context, name string) {
	existing, ok := inspectContainer(ctx, name)
	if !ok {
		return
	}

	log.Printf("Removing container %s left over from a previous run", name)
	if _, err := pool.Client().ContainerRemove(ctx, existing.ID, mobyclient.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	}); err != nil {
		setupFatal(ctx, "Could not remove container %s left over from a previous run: %s", name, err)
	}
}

// setupFatal stops a run that could not build its fixtures, and removes the
// containers it had already started. The next run would remove them anyway, but
// until then a Trino container keeps using CPU. -no_cleanup still keeps them,
// so a broken container can be inspected.
func setupFatal(ctx context.Context, format string, v ...any) {
	log.Printf(format, v...)
	releaseDockerResources(ctx)
	os.Exit(1)
}

// grantAdminRoleToTestUser lets the test user take the hive admin role, which
// the role tests rely on. The CLI reports failures through its exit code only.
func grantAdminRoleToTestUser(ctx context.Context) error {
	grantSQL := "SET ROLE admin IN hive; GRANT admin TO USER test IN hive;"

	execCmd := []string{
		"trino",
		"--user", "admin",
		"--execute", grantSQL,
	}
	result, err := trinoContainer.Exec(ctx, execCmd)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("trino CLI exited with code %d\nstdout: %s\nstderr: %s", result.ExitCode, result.StdOut, result.StdErr)
	}
	return nil
}

// createNetwork builds the network the containers share, first deleting one
// left behind by an earlier run. The pool tracks the network it creates and
// removes it on Close, after the containers: Docker will not remove a network
// while containers are still attached to it.
func createNetwork(ctx context.Context) dt.ClosableNetwork {
	networks, err := pool.Client().NetworkList(ctx, mobyclient.NetworkListOptions{})
	if err != nil {
		setupFatal(ctx, "Could not list Docker networks: %s", err)
	}
	for _, n := range networks.Items {
		if n.Name != TrinoNetwork {
			continue
		}
		log.Printf("Removing network %s left over from a previous run", TrinoNetwork)
		if _, err := pool.Client().NetworkRemove(ctx, n.ID, mobyclient.NetworkRemoveOptions{}); err != nil {
			setupFatal(ctx, "Could not remove network %s left over from a previous run: %s", TrinoNetwork, err)
		}
	}

	created, err := pool.CreateNetwork(ctx, TrinoNetwork, nil)
	if err != nil {
		setupFatal(ctx, "Could not create Docker network: %s", err)
	}

	return created
}

// inspectContainer looks up a container by name or ID, reporting whether it exists.
// inspectContainer looks up a container by name or ID, reporting whether it exists.
func inspectContainer(ctx context.Context, nameOrID string) (container.InspectResponse, bool) {
	resp, err := pool.Client().ContainerInspect(ctx, nameOrID, mobyclient.ContainerInspectOptions{})
	if err != nil {
		return container.InspectResponse{}, false
	}

	return resp.Container, true
}

// setupS3Emulator starts the object store the spooling protocol writes its
// segments to, and creates the bucket. Trino addresses it by container name on
// the shared network; the host port is only used to create the bucket.
//
// Floci is an MIT-licensed AWS emulator: a 77 MB native image that starts in
// well under a second, needs no licence token, and implements what Trino's S3
// filesystem uses, including multipart uploads and the SSE-C headers the
// spooling manager encrypts segments with.
func setupS3Emulator(ctx context.Context) {
	s3Container := runContainer(ctx, "floci/floci",
		dt.WithName(DockerS3Name),
		dt.WithTag("2.1.0"),
		dt.WithEnv([]string{
			"FLOCI_STORAGE_MODE=memory",
			"FLOCI_DEFAULT_REGION=us-east-1",
		}),
		dt.WithContainerConfig(func(c *container.Config) {
			// the image's own check runs every 5 seconds, which is most of
			// the time this container needs to become ready
			c.Healthcheck = &container.HealthConfig{
				Test:     []string{"CMD", "/usr/local/bin/healthcheck.sh"},
				Interval: 250 * time.Millisecond,
				Timeout:  3 * time.Second,
				Retries:  40,
			}
		}),
		dt.WithHostConfig(func(hc *container.HostConfig) {
			hc.NetworkMode = container.NetworkMode(trinoNetwork.ID())
		}),
	)

	s3Endpoint := "http://localhost:" + s3Container.GetPort("4566/tcp")
	log.Println("S3 emulator started at:", s3Endpoint)

	waitForContainerHealth(ctx, s3Container, "s3")

	// A zero timeout makes the pool fall back to the max wait it was built with.
	if err := pool.Retry(ctx, 0, func() error {
		return createS3Bucket(ctx, s3Endpoint, bucketName)
	}); err != nil {
		setupFatal(ctx, "Could not create the %s bucket in the S3 emulator: %s\nContainer logs:\n%s", bucketName, err, getLogs(ctx, s3Container))
	}
}

// createS3Bucket creates the bucket with a bare PUT. The emulator does not
// check request signatures, so this needs no AWS SDK, which keeps the SDK's
// modules out of the requirements every consumer of the driver inherits.
func createS3Bucket(ctx context.Context, endpoint, bucketName string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+"/"+bucketName, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create S3 bucket: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("failed to create S3 bucket: %s: %s", resp.Status, body)
	}

	log.Printf("Bucket %s created successfully!", bucketName)
	return nil
}

func waitForContainerHealth(ctx context.Context, c dt.ClosableResource, containerName string) {
	// A zero timeout makes the pool fall back to the max wait it was built with.
	if err := pool.Retry(ctx, 0, func() error {
		inspect, ok := inspectContainer(ctx, c.ID())
		if !ok {
			setupFatal(ctx, "Failed to inspect container %s", c.ID())
		}
		state := inspect.State
		if state == nil || !state.Running {
			setupFatal(ctx, "Container %s is not running\nContainer logs:\n%s", c.ID(), getLogs(ctx, c))
		}
		log.Printf("Waiting for %s container: %s\n", containerName, state.Status)
		if state.Health == nil || state.Health.Status != container.Healthy {
			return errors.New("Not ready")
		}
		return nil
	}); err != nil {
		setupFatal(ctx, "Timed out waiting for container %s to get ready: %s\nContainer logs:\n%s", containerName, err, getLogs(ctx, c))
	}
}

// prepareSecrets builds the directory mounted at /etc/trino/secrets: the
// password file from the repository, and a certificate generated for this run.
// A temporary directory keeps generated files out of the source tree; it is
// world-readable so the container's user can read it.
func prepareSecrets(sourceDir string) (string, error) {
	dir, err := os.MkdirTemp("", "trino-go-client-secrets-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return "", err
	}
	passwords, err := os.ReadFile(filepath.Join(sourceDir, "password.db"))
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "password.db"), passwords, 0o644); err != nil {
		return "", err
	}
	if err := generateCerts(dir); err != nil {
		return "", err
	}
	return dir, nil
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
		DNSNames:              []string{"localhost", "trino"},
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

func getLogs(ctx context.Context, c dt.ClosableResource) string {
	stdout, stderr, err := c.Logs(ctx)
	if err != nil {
		return fmt.Sprintf("failed to read container logs: %s", err)
	}
	return stdout + stderr
}

// integrationDSN returns the DSN of the integration test server, skipping the
// test in short mode where no server is available.
func integrationDSN(t testing.TB) string {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	return *integrationServerFlag
}

// requireServerVersion skips the test unless the server under test is at
// least the given Trino release.
func requireServerVersion(t testing.TB, minimum int) {
	t.Helper()
	integrationDSN(t)
	if serverVersion < minimum {
		t.Skipf("Skipping test: needs Trino %d or later, the server runs %d", minimum, serverVersion)
	}
}

// integrationOpen opens a connection to the integration test server, or to
// dsn when given, and closes it when the test ends. Queries time out after
// -trino_query_timeout unless the DSN sets its own query_timeout.
func integrationOpen(t testing.TB, dsn ...string) *sql.DB {
	t.Helper()
	target := integrationDSN(t)
	if len(dsn) > 0 {
		target = dsn[0]
	}
	target, err := addQueryTimeout(target)
	require.NoError(t, err)
	db, err := sql.Open("trino", target)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func addQueryTimeout(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid DSN %q: %w", dsn, err)
	}
	query := parsed.Query()
	if query.Get("query_timeout") != "" {
		return dsn, nil
	}
	query.Set("query_timeout", integrationServerQueryTimeout.String())
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func contextSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
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

type queryProtocol struct {
	name string
	args []any
}

// queryProtocols lists the ways a query can be run against the server: the
// direct protocol, and the spooling protocol when the server supports it.
func queryProtocols() []queryProtocol {
	protocols := []queryProtocol{{name: "direct protocol"}}
	if spoolingProtocolSupported {
		protocols = append(protocols, queryProtocol{name: "spooling protocol", args: []any{sql.Named("encoding", "json")}})
	}
	return protocols
}

// findRunningQuery returns the ID of the running query with the given text
// and source, polling until the server reports it. The text distinguishes it
// from the polling queries, which share the connection and its source.
func findRunningQuery(t *testing.T, db *sql.DB, source, query string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var queryID string
		err := db.QueryRowContext(ctx, "SELECT query_id FROM system.runtime.queries WHERE state = 'RUNNING' AND source = ? AND query = ?", source, query).Scan(&queryID)
		if err == nil {
			return queryID
		}
		require.ErrorIs(t, err, sql.ErrNoRows, "failed to read the query ID")
		require.NoError(t, contextSleep(ctx, 100*time.Millisecond), "no running query with source %q appeared in 5 seconds", source)
	}
}

// requireQueryCancelled polls the server until the query has failed with
// USER_CANCELED, which is the only proof that the client's cancel request
// reached the server.
func requireQueryCancelled(t *testing.T, db *sql.DB, queryID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var state string
		var code *string
		err := db.QueryRowContext(ctx, "SELECT state, error_code FROM system.runtime.queries WHERE query_id = ?", queryID).Scan(&state, &code)
		require.NoError(t, err, "failed to read the state of query %s", queryID)
		if state == "FAILED" && code != nil && *code == "USER_CANCELED" {
			return
		}
		err = contextSleep(ctx, 100*time.Millisecond)
		require.NoError(t, err, "query %s was not canceled in 5 seconds; state: %s, code: %v", queryID, state, code)
	}
}
