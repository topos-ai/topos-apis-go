package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	grpccredentials "google.golang.org/grpc/credentials"
)

const (
	toposClientID     string = "tJbqmqfttOHJ0kGfy8Bvf60v8Z4pW7T4"
	toposAudience     string = "https://endpoints.topos.com"
	toposCallbackPort string = "3000"
	toposCallback     string = "http://localhost:" + toposCallbackPort + "/callback"
)

var httpClient *http.Client = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	},
}

type getTokenResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

func getToken(ctx context.Context, payload url.Values) (*getTokenResponse, error) {
	req, err := http.NewRequest("POST", "https://auth.topos.com/oauth/token", strings.NewReader(payload.Encode()))
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	response, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", response.StatusCode)
	}

	t := &getTokenResponse{}
	if err := json.NewDecoder(response.Body).Decode(t); err != nil {
		if err := req.Body.Close(); err != nil {
			return nil, err
		}

		return nil, err
	}

	return t, nil
}

func randomString() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(value), nil
}

func sha256SumString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func getTokenPKCE(ctx context.Context) (*getTokenResponse, error) {
	codeVerifier, err := randomString()
	if err != nil {
		return nil, err
	}

	state, err := randomString()
	if err != nil {
		return nil, err
	}

	authorizePayload := url.Values{}
	authorizePayload.Set("audience", toposAudience)
	authorizePayload.Set("client_id", toposClientID)
	authorizePayload.Set("code_challenge", sha256SumString(codeVerifier))
	authorizePayload.Set("code_challenge_method", "S256")
	authorizePayload.Set("redirect_uri", toposCallback)
	authorizePayload.Set("response_type", "code")
	authorizePayload.Set("state", state)

	authorizePayload.Add("scope", "offline_access")

	if err := exec.Command("open", "https://auth.topos.com/authorize?"+authorizePayload.Encode()).Run(); err != nil {
		return nil, err
	}

	codeCh := make(chan string)

	setCode := &sync.Once{}
	server := &http.Server{
		Addr: fmt.Sprintf("127.0.0.1:%s", toposCallbackPort),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != "/callback" {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}

			if reqState := req.FormValue("state"); reqState != state {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			setCode.Do(func() {
				codeCh <- req.FormValue("code")
				fmt.Fprint(w, "ok")
			})
		}),
	}

	errCh := make(chan error)
	go func() {
		defer close(codeCh)
		defer close(errCh)
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	code := <-codeCh
	if err := server.Close(); err != nil {
		return nil, err
	}

	for err := range errCh {
		if err != http.ErrServerClosed {
			return nil, err
		}
	}

	tokenPayload := url.Values{}
	tokenPayload.Set("client_id", toposClientID)
	tokenPayload.Set("code", code)
	tokenPayload.Set("code_verifier", codeVerifier)
	tokenPayload.Set("grant_type", "authorization_code")
	tokenPayload.Set("redirect_uri", toposCallback)

	return getToken(ctx, tokenPayload)
}

type credentials struct {
	AccessToken        string    `json:"access_token"`
	AccessTokenExpires time.Time `json:"access_token_expires"`
	RefreshToken       string    `json:"refresh_token"`
}

func writeCredentialsFile(credentials *credentials) error {
	toposDir := filepath.Join(os.Getenv("HOME"), ".topos")
	if err := os.MkdirAll(toposDir, 0700); err != nil {
		return err
	}

	path := filepath.Join(toposDir, "credentials.json")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(file).Encode(credentials); err != nil {
		if err := file.Close(); err != nil {
			return err
		}

		return err
	}

	return file.Close()
}

func readCredentialsFile() (*credentials, error) {
	path := filepath.Join(os.Getenv("HOME"), ".topos", "credentials.json")
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	credentials := &credentials{}
	if err := json.NewDecoder(file).Decode(credentials); err != nil {
		if err := file.Close(); err != nil {
			return nil, err
		}

		return nil, err
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	return credentials, nil
}

func Login(ctx context.Context) error {
	response, err := getTokenPKCE(ctx)
	if err != nil {
		return err
	}

	return writeCredentialsFile(&credentials{
		AccessToken:        response.AccessToken,
		AccessTokenExpires: time.Now().Add(time.Duration(response.ExpiresIn-60) * time.Second),
		RefreshToken:       response.RefreshToken,
	})
}

type localCredentials struct {
	lock        sync.Mutex
	credentials *credentials
}

func newLocalCredentials() (*localCredentials, error) {
	credentials, err := readCredentialsFile()
	if err != nil {
		return nil, err
	}

	return &localCredentials{
		credentials: credentials,
	}, nil
}

func (c *localCredentials) accessToken(ctx context.Context) (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if time.Now().Before(c.credentials.AccessTokenExpires) {
		return c.credentials.AccessToken, nil
	}

	payload := url.Values{}
	payload.Add("grant_type", "refresh_token")
	payload.Add("client_id", toposClientID)
	payload.Add("refresh_token", c.credentials.RefreshToken)

	response, err := getToken(ctx, payload)
	if err != nil {
		return "", nil
	}

	credentials := &credentials{
		AccessToken:        response.AccessToken,
		AccessTokenExpires: time.Now().Add(time.Duration(response.ExpiresIn-60) * time.Second),
		RefreshToken:       c.credentials.RefreshToken,
	}

	if err := writeCredentialsFile(credentials); err != nil {
		return "", err
	}

	c.credentials = credentials
	return c.credentials.AccessToken, nil
}

func (c *localCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	accessToken, err := c.accessToken(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"authorization": accessToken,
	}, nil
}

func (*localCredentials) RequireTransportSecurity() bool {
	return true
}

func withAddr(addr string) grpc.DialOption {
	if strings.HasPrefix(addr, "localhost:") || strings.HasPrefix(addr, "127.0.0.1:") {
		return grpc.WithInsecure()
	}

	return grpc.WithTransportCredentials(grpccredentials.NewTLS(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: !strings.Contains(addr, "."),
	}))
}

func Dial(addr string, useLocalCredentials bool) (*grpc.ClientConn, error) {
	if !useLocalCredentials {
		return grpc.Dial(addr, withAddr(addr))
	}

	creds, err := newLocalCredentials()
	if err != nil {
		return nil, err
	}

	return grpc.Dial(addr, withAddr(addr), grpc.WithPerRPCCredentials(creds))
}
