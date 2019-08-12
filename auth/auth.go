package auth

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type perRPCCredentials struct {
	lock        sync.Mutex
	client      *http.Client
	accessToken string
	expires     time.Time
}

func newPerRPCCredentials() *perRPCCredentials {
	return &perRPCCredentials{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		},
	}
}

type token struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

func (a *perRPCCredentials) getToken(ctx context.Context) (string, error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if time.Now().Before(a.expires) {
		return a.accessToken, nil
	}

	payload := url.Values{}
	payload.Add("grant_type", "client_credentials")
	payload.Add("audience", "endpoints.topos.com")
	payload.Add("client_id", os.Getenv("TOPOSCLIENTID"))
	payload.Add("client_secret", os.Getenv("TOPOSCLIENTSECRET"))
	req, err := http.NewRequest("POST", "https://auth.topos.com/oauth/token", strings.NewReader(payload.Encode()))
	if err != nil {
		return "", err
	}

	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	response, err := a.client.Do(req)
	if err != nil {
		return "", err
	}

	t := &token{}
	if err := json.NewDecoder(response.Body).Decode(t); err != nil {
		if err := req.Body.Close(); err != nil {
			return "", err
		}

		return "", err
	}

	a.accessToken = "Bearer " + t.AccessToken
	a.expires = time.Now().Add(time.Duration(t.ExpiresIn-60) * time.Second)
	return a.accessToken, nil
}

func (a *perRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := a.getToken(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"authorization": token,
	}, nil
}

func (a *perRPCCredentials) RequireTransportSecurity() bool {
	return true
}

func DialOptions(secure, insecureSkipVerify bool) []grpc.DialOption {
	if !secure {
		return []grpc.DialOption{
			grpc.WithInsecure(),
		}
	}

	return []grpc.DialOption{
		grpc.WithPerRPCCredentials(newPerRPCCredentials()),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: insecureSkipVerify,
		})),
	}
}
