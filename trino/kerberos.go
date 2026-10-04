package trino

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

const (
	kerberosCredentialCachePathConfig     = "KerberosCredentialCachePath"
	kerberosServicePrincipalPatternConfig = "KerberosServicePrincipalPattern"
	kerberosUseCanonicalHostnameConfig    = "KerberosUseCanonicalHostname"
	kerberosClientConfig                  = "KerberosClient"

	defaultKerberosServicePrincipalPattern = "${SERVICE}@${HOST}"
	servicePlaceholder                     = "${SERVICE}"
	hostPlaceholder                        = "${HOST}"
	credentialCacheFilePrefix              = "FILE:"
)

func (c *Config) validateKerberos() error {
	if c.KerberosKeytabPath != "" && c.KerberosCredentialCachePath != "" {
		return fmt.Errorf("trino: client configuration error, %s and %s cannot be specified together", kerberosKeytabPathConfig, kerberosCredentialCachePathConfig)
	}
	if c.KerberosClient != nil {
		if !c.KerberosEnabled {
			return fmt.Errorf("trino: client configuration error, %s requires %s", kerberosClientConfig, kerberosEnabledConfig)
		}
		if c.KerberosKeytabPath != "" || c.KerberosCredentialCachePath != "" {
			return fmt.Errorf("trino: client configuration error, %s cannot be specified together with %s or %s", kerberosClientConfig, kerberosKeytabPathConfig, kerberosCredentialCachePathConfig)
		}
	}
	// gokrb5 finds the realm of a service through domain_realm in krb5.conf
	// and cannot take it from the principal.
	if service, _, ok := cutLast(c.KerberosServicePrincipalPattern, "@"); ok && strings.Contains(service, "/") {
		return fmt.Errorf("trino: client configuration error, %s %q names a realm; use the service@host form and map the host to its realm in domain_realm of krb5.conf", kerberosServicePrincipalPatternConfig, c.KerberosServicePrincipalPattern)
	}
	return nil
}

func newKerberosClient(conf *Config) (*client.Client, error) {
	// The caller logged the client in and owns it, like the JAAS Subject the
	// JDBC driver uses with KerberosDelegation.
	if conf.KerberosClient != nil {
		return conf.checkedKerberosClient()
	}
	confKerb, err := config.Load(conf.KerberosConfigPath)
	if err != nil {
		return nil, fmt.Errorf("trino: Error loading krb config: %w", err)
	}

	var kerberosClient *client.Client
	if conf.KerberosKeytabPath != "" {
		kerberosClient, err = newKerberosClientWithKeytab(conf, confKerb)
	} else {
		kerberosClient, err = newKerberosClientFromCredentialCache(conf, confKerb)
	}
	if err != nil {
		return nil, err
	}

	loginErr := kerberosClient.Login()
	if loginErr != nil {
		return nil, fmt.Errorf("trino: Error login to KDC: %v", loginErr)
	}
	return kerberosClient, nil
}

func newKerberosClientWithKeytab(conf *Config, confKerb *config.Config) (*client.Client, error) {
	kt, err := keytab.Load(conf.KerberosKeytabPath)
	if err != nil {
		return nil, fmt.Errorf("trino: Error loading Keytab: %w", err)
	}
	return client.NewWithKeytab(conf.KerberosPrincipal, conf.KerberosRealm, kt, confKerb), nil
}

func newKerberosClientFromCredentialCache(conf *Config, confKerb *config.Config) (*client.Client, error) {
	path, err := conf.kerberosCredentialCachePath()
	if err != nil {
		return nil, err
	}
	ccache, err := loadCredentialCache(path)
	if err != nil {
		return nil, fmt.Errorf("trino: Error loading Kerberos credential cache %s: %w", path, err)
	}
	if err := conf.checkPrincipal("Kerberos credential cache holds a ticket", ccache.GetClientPrincipalName().PrincipalNameString(), ccache.GetClientRealm()); err != nil {
		return nil, err
	}
	kerberosClient, err := client.NewFromCCache(ccache, confKerb)
	if err != nil {
		return nil, fmt.Errorf("trino: Error loading Kerberos credential cache %s: %w", path, err)
	}
	return kerberosClient, nil
}

// kerberosCredentialCachePath falls back to the cache kinit writes to, as
// the JDK and MIT Kerberos look it up: KRB5CCNAME, then /tmp/krb5cc_<uid>.
func (c *Config) kerberosCredentialCachePath() (string, error) {
	if c.KerberosCredentialCachePath != "" {
		return c.KerberosCredentialCachePath, nil
	}
	if name := os.Getenv("KRB5CCNAME"); name != "" {
		return credentialCacheFile(name)
	}
	uid := os.Getuid()
	if uid < 0 {
		return "", fmt.Errorf("trino: client configuration error, Kerberos needs %s or %s", kerberosKeytabPathConfig, kerberosCredentialCachePathConfig)
	}
	return "/tmp/krb5cc_" + strconv.Itoa(uid), nil
}

// credentialCacheFile accepts the FILE type of KRB5CCNAME, the only one
// gokrb5 reads; a single letter before the colon is a Windows drive.
func credentialCacheFile(name string) (string, error) {
	if path, ok := strings.CutPrefix(name, credentialCacheFilePrefix); ok {
		return path, nil
	}
	cacheType, _, hasType := strings.Cut(name, ":")
	if hasType && len(cacheType) > 1 {
		return "", fmt.Errorf("trino: unsupported Kerberos credential cache type %s in KRB5CCNAME, only %s caches can be read; set %s", cacheType, strings.TrimSuffix(credentialCacheFilePrefix, ":"), kerberosCredentialCachePathConfig)
	}
	return name, nil
}

// loadCredentialCache turns the panics gokrb5 raises on a length field
// pointing past the end of the cache file into an error.
func loadCredentialCache(path string) (ccache *credentials.CCache, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("malformed credential cache: %v", r)
		}
	}()
	return credentials.LoadCCache(path)
}

func (c *Config) checkedKerberosClient() (*client.Client, error) {
	creds := c.KerberosClient.Credentials
	if creds == nil {
		return nil, fmt.Errorf("trino: %s has no credentials, log it in before passing it", kerberosClientConfig)
	}
	if err := c.checkPrincipal(kerberosClientConfig+" holds credentials", creds.UserName(), creds.Realm()); err != nil {
		return nil, err
	}
	return c.KerberosClient, nil
}

// checkPrincipal makes sure the driver never authenticates as another user
// than KerberosPrincipal and KerberosRealm name.
func (c *Config) checkPrincipal(source, name, realm string) error {
	if c.KerberosPrincipal != "" && c.KerberosPrincipal != name && c.KerberosPrincipal != name+"@"+realm {
		return fmt.Errorf("trino: %s for %s@%s, not for %s", source, name, realm, c.KerberosPrincipal)
	}
	if c.KerberosRealm != "" && c.KerberosRealm != realm {
		return fmt.Errorf("trino: %s for realm %s, not for %s", source, realm, c.KerberosRealm)
	}
	return nil
}

func (c *Conn) setSPNEGOHeader(req *http.Request) error {
	principal, err := c.kerberosServicePrincipal.forHost(req.Context(), req.URL.Hostname())
	if err != nil {
		return err
	}
	err = spnego.SetSPNEGOHeader(c.kerberosClient, req, principal)
	if err != nil {
		c.markUnusable()
		return fmt.Errorf("error setting client SPNEGO header: %w", err)
	}
	return nil
}

type kerberosServicePrincipal struct {
	pattern     string
	serviceName string
	// canonicalizer is nil when KerberosDisableCanonicalHostname is set
	canonicalizer *hostnameCanonicalizer
}

func newKerberosServicePrincipal(conf *Config) kerberosServicePrincipal {
	principal := kerberosServicePrincipal{
		pattern:     conf.KerberosServicePrincipalPattern,
		serviceName: conf.KerberosRemoteServiceName,
	}
	if principal.pattern == "" {
		principal.pattern = defaultKerberosServicePrincipalPattern
	}
	if principal.serviceName == "" {
		principal.serviceName = defaultKerberosServiceName
	}
	if !conf.KerberosDisableCanonicalHostname {
		principal.canonicalizer = newHostnameCanonicalizer(net.DefaultResolver, os.Hostname)
	}
	return principal
}

// forHost substitutes the pattern like the JDBC driver, which reads the
// result as a GSS-API service@host name. gokrb5 takes a Kerberos principal
// instead, so service@host becomes service/host; a result without @ is
// already a principal and is used as is.
func (p kerberosServicePrincipal) forHost(ctx context.Context, host string) (string, error) {
	if p.canonicalizer != nil {
		canonical, err := p.canonicalizer.canonicalize(ctx, host)
		if err != nil {
			return "", err
		}
		host = canonical
	}
	name := strings.ReplaceAll(p.pattern, hostPlaceholder, strings.ToLower(host))
	name = strings.ReplaceAll(name, servicePlaceholder, p.serviceName)
	service, serviceHost, ok := cutLast(name, "@")
	if !ok {
		return name, nil
	}
	return service + "/" + serviceHost, nil
}

type hostResolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
	LookupAddr(ctx context.Context, addr string) ([]string, error)
}

// hostnameCanonicalizer resolves a host the way the JDBC driver does through
// InetAddress: a forward lookup, which follows CNAME records, then a reverse
// lookup of the first address. Go's resolver does not cache, and the driver
// sets the SPNEGO header on every request, so the result is kept for the
// lifetime of the connection.
type hostnameCanonicalizer struct {
	resolver      hostResolver
	localHostname func() (string, error)

	mu        sync.Mutex
	canonical map[string]string
}

func newHostnameCanonicalizer(resolver hostResolver, localHostname func() (string, error)) *hostnameCanonicalizer {
	return &hostnameCanonicalizer{resolver: resolver, localHostname: localHostname, canonical: map[string]string{}}
}

func (h *hostnameCanonicalizer) canonicalize(ctx context.Context, host string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if canonical, ok := h.canonical[host]; ok {
		return canonical, nil
	}
	canonical, err := h.resolve(ctx, host)
	if err != nil {
		return "", err
	}
	h.canonical[host] = canonical
	return canonical, nil
}

func (h *hostnameCanonicalizer) resolve(ctx context.Context, host string) (string, error) {
	name := host
	if net.ParseIP(host) != nil {
		name = h.reverseLookup(ctx, host)
	}
	// localhost has no useful canonical name, so the JDBC driver uses the
	// one of the machine instead
	lookupHost := host
	if strings.EqualFold(name, "localhost") {
		localHostname, err := h.localHostname()
		if err != nil {
			return "", fmt.Errorf("trino: failed to get the local hostname for the Kerberos service principal: %w", err)
		}
		lookupHost = localHostname
	}
	canonical, err := h.canonicalName(ctx, lookupHost)
	if err != nil {
		return "", err
	}
	if strings.EqualFold(canonical, "localhost") {
		return "", fmt.Errorf("trino: Fully qualified name of localhost should not resolve to 'localhost'. System configuration error? Set %s=false to use the URL host in the Kerberos service principal", kerberosUseCanonicalHostnameConfig)
	}
	return canonical, nil
}

func (h *hostnameCanonicalizer) canonicalName(ctx context.Context, host string) (string, error) {
	address := host
	if net.ParseIP(host) == nil {
		addresses, err := h.resolver.LookupHost(ctx, host)
		if err != nil {
			return "", fmt.Errorf("trino: failed to resolve host %s for the Kerberos service principal: %w", host, err)
		}
		address = addresses[0]
	}
	return h.reverseLookup(ctx, address), nil
}

// reverseLookup falls back to the address itself, as Java's
// getCanonicalHostName does.
func (h *hostnameCanonicalizer) reverseLookup(ctx context.Context, address string) string {
	names, err := h.resolver.LookupAddr(ctx, address)
	if err != nil || len(names) == 0 {
		return address
	}
	return strings.TrimSuffix(names[0], ".")
}

func cutLast(s, sep string) (before, after string, found bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
}
