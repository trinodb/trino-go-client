package trino

import (
	"fmt"
	"net/http"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

func newKerberosClient(conf *Config) (*client.Client, error) {
	kt, err := keytab.Load(conf.KerberosKeytabPath)
	if err != nil {
		return nil, fmt.Errorf("trino: Error loading Keytab: %w", err)
	}
	confKerb, err := config.Load(conf.KerberosConfigPath)
	if err != nil {
		return nil, fmt.Errorf("trino: Error loading krb config: %w", err)
	}

	kerberosClient := client.NewWithKeytab(conf.KerberosPrincipal, conf.KerberosRealm, kt, confKerb)
	loginErr := kerberosClient.Login()
	if loginErr != nil {
		return nil, fmt.Errorf("trino: Error login to KDC: %v", loginErr)
	}
	return kerberosClient, nil
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
