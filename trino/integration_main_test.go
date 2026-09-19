package trino

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
	"log"
	"math/big"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	dt "github.com/ory/dockertest/v4"
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
	pool                      dt.ClosablePool
	trinoContainer            dt.ClosableResource
	trinoNetwork              dt.ClosableNetwork
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
	tlsServer = ""
)

func TestMain(m *testing.M) {
	flag.Parse()
	DefaultQueryTimeout = *integrationServerQueryTimeout
	DefaultCancelQueryTimeout = *integrationServerQueryTimeout
	if *trinoImageTagFlag == "" {
		*trinoImageTagFlag = "latest"
	}

	if *trinoImageTagFlag == "latest" {
		spoolingProtocolSupported = true
	} else {
		version, err := strconv.Atoi(*trinoImageTagFlag)
		if err != nil {
			log.Fatalf("Invalid trino_image_tag: %s", *trinoImageTagFlag)
		}
		spoolingProtocolSupported = version >= 466
	}

	ctx := context.Background()

	var err error
	if *integrationServerFlag == "" && !testing.Short() {
		pool, err = dt.NewPool(ctx, "", dt.WithMaxWait(1*time.Minute))
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		removeExistingContainer(ctx, DockerTrinoName)
		if spoolingProtocolSupported {
			removeExistingContainer(ctx, DockerLocalStackName)
		}

		trinoNetwork = createNetwork(ctx)

		wd, err := os.Getwd()
		if err != nil {
			setupFatal(ctx, "Failed to get working directory: %s", err)
		}

		if spoolingProtocolSupported {
			if err := setupLocalStack(ctx); err != nil {
				setupFatal(ctx, "Failed to start LocalStack: %s", err)
			}
		}

		err = generateCerts(wd + "/etc/secrets")
		if err != nil {
			setupFatal(ctx, "Could not generate TLS certificates: %s", err)
		}

		mounts := []string{
			wd + "/etc/secrets:/etc/trino/secrets",
			wd + "/etc/jvm.config:/etc/trino/jvm.config",
			wd + "/etc/node.properties:/etc/trino/node.properties",
			wd + "/etc/password-authenticator.properties:/etc/trino/password-authenticator.properties",
			wd + "/etc/catalog/memory.properties:/etc/trino/catalog/memory.properties",
			wd + "/etc/catalog/tpch.properties:/etc/trino/catalog/tpch.properties",
		}
		version, err := strconv.Atoi(*trinoImageTagFlag)
		if (err != nil && *trinoImageTagFlag == "latest") || (err == nil && version >= 458) {
			mounts = append(mounts, wd+"/etc/catalog/hive.properties:/etc/trino/catalog/hive.properties")
		}

		if spoolingProtocolSupported {
			version, err := strconv.Atoi(*trinoImageTagFlag)
			if (err != nil && *trinoImageTagFlag != "latest") || (err == nil && version < 477) {
				mounts = append(mounts, wd+"/etc/config-pre-477version.properties:/etc/trino/config.properties")
			} else {
				mounts = append(mounts, wd+"/etc/config.properties:/etc/trino/config.properties")
			}
			mounts = append(mounts, wd+"/etc/spooling-manager.properties:/etc/trino/spooling-manager.properties")
		} else {
			mounts = append(mounts, wd+"/etc/config-pre-466version.properties:/etc/trino/config.properties")
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

		err = grantAdminRoleToTestUser(ctx)
		if err != nil {
			setupFatal(ctx, "Failed to grant admin role to test user: %s", err)
		}

		*integrationServerFlag = "http://test@localhost:" + trinoContainer.GetPort("8080/tcp")
		tlsServer = "https://admin:admin@localhost:" + trinoContainer.GetPort("8443/tcp")

		http.DefaultTransport.(*http.Transport).TLSClientConfig, err = getTLSConfig(wd + "/etc/secrets")
		if err != nil {
			setupFatal(ctx, "Failed to set the default TLS config: %s", err)
		}
	}

	code := m.Run()

	if pool != nil {
		if *noCleanup {
			log.Print("Leaving Docker containers running, as requested by -no_cleanup")
		} else if err := pool.Close(ctx); err != nil {
			log.Fatalf("Could not clean up Docker resources: %s", err)
		}
	}

	os.Exit(code)
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
		log.Fatalf("Could not remove container %s left over from a previous run: %s", name, err)
	}
}

// setupFatal stops a run that could not build its fixtures, and removes the
// containers it had already started. The next run would remove them anyway, but
// until then they hold their ports - LocalStack binds 4566 and 4571 - and a
// Trino container keeps using CPU. -no_cleanup still keeps them, so a broken
// container can be inspected.
func setupFatal(ctx context.Context, format string, v ...any) {
	log.Printf(format, v...)
	if *noCleanup {
		log.Print("Leaving Docker containers running, as requested by -no_cleanup")
	} else if err := pool.Close(ctx); err != nil {
		log.Printf("Could not clean up Docker resources: %s", err)
	}
	os.Exit(1)
}

func grantAdminRoleToTestUser(ctx context.Context) error {
	grantSQL := "SET ROLE admin IN hive; GRANT admin TO USER test IN hive;"

	execCmd := []string{
		"trino",
		"--user", "admin",
		"--execute", grantSQL,
	}
	_, err := trinoContainer.Exec(ctx, execCmd)
	if err != nil {
		log.Printf("Warning: Failed to execute GRANT: %s", err)
	}

	return err
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
func inspectContainer(ctx context.Context, nameOrID string) (container.InspectResponse, bool) {
	resp, err := pool.Client().ContainerInspect(ctx, nameOrID, mobyclient.ContainerInspectOptions{})
	if err != nil {
		return container.InspectResponse{}, false
	}

	return resp.Container, true
}

func setupLocalStack(ctx context.Context) error {
	localstackContainer := runContainer(ctx, "localstack/localstack",
		dt.WithName(DockerLocalStackName),
		// Pinned: from the 2026.x line on, the image refuses to start without a
		// LOCALSTACK_AUTH_TOKEN. 4.14 is the last release that runs license-free.
		dt.WithTag("4.14"),
		dt.WithEnv([]string{
			"SERVICES=s3",
			"region_name=us-east-1",
			"AWS_ACCESS_KEY_ID=test",
			"AWS_SECRET_ACCESS_KEY=test",
		}),
		dt.WithPortBindings(network.PortMap{
			network.MustParsePort("4566/tcp"): {{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: "4566"}},
			network.MustParsePort("4571/tcp"): {{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: "4571"}},
		}),
		dt.WithHostConfig(func(hc *container.HostConfig) {
			hc.NetworkMode = container.NetworkMode(trinoNetwork.ID())
		}),
	)

	localstackPort := localstackContainer.GetPort("4566/tcp")
	s3Endpoint := "http://localhost:" + localstackPort

	log.Println("LocalStack started at:", s3Endpoint)

	waitForContainerHealth(ctx, localstackContainer, "localstack")

	var err error
	for retry := 0; retry < MAXRetries; retry++ {
		err = createS3Bucket(s3Endpoint, "test", "test", bucketName)
		if err == nil {
			log.Println("S3 bucket created successfully")
			return nil
		}
		log.Printf("Failed to create S3 bucket, retrying... (%d/%d)\n", retry+1, MAXRetries)
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("failed to create S3 bucket after multiple attempts: %w", err)
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
		UsePathStyle: true,
	})

	createBucketInput := &s3.CreateBucketInput{
		Bucket: &bucketName,
	}

	_, err = s3Client.CreateBucket(context.TODO(), createBucketInput)
	if err != nil {
		return fmt.Errorf("failed to create S3 bucket: %w", err)
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

// integrationOpen opens a connection to the integration test server, or to
// dsn when given, and closes it when the test ends.
func integrationOpen(t testing.TB, dsn ...string) *sql.DB {
	t.Helper()
	target := integrationDSN(t)
	if len(dsn) > 0 {
		target = dsn[0]
	}
	db, err := sql.Open("trino", target)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
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

type queryProtocol struct {
	name string
	args []any
}

// queryProtocols lists the ways a query can be run against the server: the
// direct protocol, and the spooling protocol when the server supports it.
func queryProtocols() []queryProtocol {
	protocols := []queryProtocol{{name: "direct protocol"}}
	if spoolingProtocolSupported {
		protocols = append(protocols, queryProtocol{name: "spooling protocol", args: []any{sql.Named(trinoEncoding, "json")}})
	}
	return protocols
}
