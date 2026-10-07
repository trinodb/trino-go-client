package trino

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ServerInfo is what the coordinator reports about itself at /v1/info.
type ServerInfo struct {
	NodeVersion string
	Environment string
	Coordinator bool
	// Starting is true until the server finished starting up and accepts
	// queries.
	Starting bool
	// Uptime is zero when the server is too old to report it.
	Uptime time.Duration
}

// Ping implements the driver.Pinger interface. It checks that the
// coordinator answers /v1/info and has finished starting, since until then
// it fails every query. Because /v1/info does not require authentication,
// Ping then sends HEAD /v1/statement, like the JDBC driver does with
// validateConnection=true, so rejected credentials fail it too. Servers
// older than Trino 469 do not answer that request, and Ping skips the
// credentials check for them. With external authentication, the request can
// start the login flow when no valid token is cached.
func (c *Conn) Ping(ctx context.Context) error {
	info, err := c.ServerInfo(ctx)
	if err != nil {
		return err
	}
	if info.Starting {
		return errors.New("trino: server is still starting")
	}
	return c.validateCredentials(ctx)
}

// ServerInfo fetches the coordinator's /v1/info. From database/sql, reach it
// through sql.Conn.Raw, where the driver connection is a *Conn.
func (c *Conn) ServerInfo(ctx context.Context) (ServerInfo, error) {
	req, err := c.newRequest(ctx, http.MethodGet, c.baseURL+"/v1/info", nil, nil)
	if err != nil {
		return ServerInfo{}, c.unsentRequestError(err)
	}
	resp, err := c.roundTrip(ctx, req)
	if err != nil {
		return ServerInfo{}, err
	}
	defer resp.Body.Close()

	var wire struct {
		NodeVersion struct {
			Version string `json:"version"`
		} `json:"nodeVersion"`
		Environment string `json:"environment"`
		Coordinator bool   `json:"coordinator"`
		Starting    bool   `json:"starting"`
		Uptime      string `json:"uptime"`
	}
	if err := json.UnmarshalRead(resp.Body, &wire); err != nil {
		return ServerInfo{}, fmt.Errorf("trino: decoding server info: %w", err)
	}
	info := ServerInfo{
		NodeVersion: wire.NodeVersion.Version,
		Environment: wire.Environment,
		Coordinator: wire.Coordinator,
		Starting:    wire.Starting,
	}
	if wire.Uptime != "" {
		if info.Uptime, err = parseAirliftDuration(wire.Uptime); err != nil {
			return ServerInfo{}, fmt.Errorf("trino: decoding server info: %w", err)
		}
	}
	return info, nil
}

// validateCredentials relies on the statement resource requiring an
// authenticated user and answering HEAD without starting a query. Servers
// before Trino 469 answer 405, which JDBC reports as an invalid connection;
// failing Ping for them would make database/sql unusable with those
// servers, so 405 only means the credentials cannot be checked.
func (c *Conn) validateCredentials(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodHead, c.baseURL+"/v1/statement", nil, nil)
	if err != nil {
		return c.unsentRequestError(err)
	}
	resp, err := c.roundTrip(ctx, req)
	var queryFailed *ErrQueryFailed
	if errors.As(err, &queryFailed) && queryFailed.StatusCode == http.StatusMethodNotAllowed {
		return nil
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
