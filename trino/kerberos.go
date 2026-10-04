package trino

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

const (
	kerberosCredentialCachePathConfig = "KerberosCredentialCachePath"

	credentialCacheFilePrefix = "FILE:"
)

func (c *Config) validateKerberos() error {
	if c.KerberosKeytabPath != "" && c.KerberosCredentialCachePath != "" {
		return fmt.Errorf("trino: client configuration error, %s and %s cannot be specified together", kerberosKeytabPathConfig, kerberosCredentialCachePathConfig)
	}
	return nil
}

func newKerberosClient(conf *Config) (*client.Client, error) {
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
	if err := conf.checkCredentialCachePrincipal(ccache); err != nil {
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

func (c *Config) checkCredentialCachePrincipal(ccache *credentials.CCache) error {
	name := ccache.GetClientPrincipalName().PrincipalNameString()
	realm := ccache.GetClientRealm()
	if c.KerberosPrincipal != "" && c.KerberosPrincipal != name && c.KerberosPrincipal != name+"@"+realm {
		return fmt.Errorf("trino: Kerberos credential cache holds a ticket for %s@%s, not for %s", name, realm, c.KerberosPrincipal)
	}
	if c.KerberosRealm != "" && c.KerberosRealm != realm {
		return fmt.Errorf("trino: Kerberos credential cache holds a ticket for realm %s, not for %s", realm, c.KerberosRealm)
	}
	return nil
}

func (c *Conn) setSPNEGOHeader(req *http.Request) error {
	remoteServiceName := "trino"
	if c.kerberosRemoteServiceName != "" {
		remoteServiceName = c.kerberosRemoteServiceName
	}
	err := spnego.SetSPNEGOHeader(c.kerberosClient, req, remoteServiceName+"/"+req.URL.Hostname())
	if err != nil {
		return fmt.Errorf("error setting client SPNEGO header: %w", err)
	}
	return nil
}
