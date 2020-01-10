package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const (
	toposClientID     string = "tJbqmqfttOHJ0kGfy8Bvf60v8Z4pW7T4"
	toposAudience     string = "https://endpoints.topos.com"
	toposCallbackPort string = "8676"
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

func writeString(path, s string) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	if _, err := io.WriteString(file, s); err != nil {
		if err := file.Close(); err != nil {
			return err
		}

		return err
	}

	return file.Close()
}

func readString(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}

	s, err := ioutil.ReadAll(file)
	if err := file.Close(); err != nil {
		return "", err
	}

	return string(s), err
}

func Login(ctx context.Context) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	toposDir := filepath.Join(homeDir, ".topos")
	if err := os.MkdirAll(toposDir, 0700); err != nil {
		return err
	}

	var accessToken string
	refreshTokenPath := filepath.Join(toposDir, "refresh_token")
	if refreshToken, err := readString(refreshTokenPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		response, err := getTokenPKCE(ctx)
		if err != nil {
			return err
		}

		if err := writeString(refreshTokenPath, response.RefreshToken); err != nil {
			return err
		}

		accessToken = response.AccessToken
	} else {
		response, err := getTokenRefresh(ctx, refreshToken)
		if err != nil {
			return err
		}

		accessToken = response.AccessToken
	}

	accessTokenPath := filepath.Join(toposDir, "access_token")
	return writeString(accessTokenPath, accessToken)
}

type localCredentials struct {
	lock            sync.Mutex
	refreshToken    string
	accessTokenPath string
	accessToken     string
	expiry          int64
}

type accessTokenExp struct {
	Exp int64 `json:"exp"`
}

func newLocalCredentials() (*localCredentials, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	toposDir := filepath.Join(homeDir, ".topos")
	refreshTokenPath := filepath.Join(toposDir, "refresh_token")
	refreshToken, err := readString(refreshTokenPath)
	if err != nil {
		return nil, err
	}

	accessTokenPath := filepath.Join(toposDir, "access_token")
	accessToken, err := readString(accessTokenPath)
	if err != nil {
		return nil, err
	}

	accessTokenComponents := strings.SplitN(accessToken, ".", 3)
	if len(accessTokenComponents) != 3 {
		return nil, fmt.Errorf("invalid access_token")
	}

	accessTokenPayloadJSON, err := base64.RawURLEncoding.DecodeString(accessTokenComponents[1])
	if err != nil {
		return nil, err
	}

	exp := accessTokenExp{}
	if err := json.Unmarshal(accessTokenPayloadJSON, &exp); err != nil {
		return nil, err
	}

	return &localCredentials{
		refreshToken:    refreshToken,
		accessTokenPath: accessTokenPath,
		accessToken:     accessToken,
		expiry:          exp.Exp,
	}, nil
}

func getTokenRefresh(ctx context.Context, refreshToken string) (*getTokenResponse, error) {
	payload := url.Values{}
	payload.Add("grant_type", "refresh_token")
	payload.Add("client_id", toposClientID)
	payload.Add("refresh_token", refreshToken)

	return getToken(ctx, payload)
}

func (c *localCredentials) authorization(ctx context.Context) (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	now := time.Now().Unix()
	if now < c.expiry {
		return c.accessToken, nil
	}

	response, err := getTokenRefresh(ctx, c.refreshToken)
	if err != nil {
		return "", err
	}

	if err := writeString(c.accessTokenPath, response.AccessToken); err != nil {
		return "", err
	}

	c.accessToken = response.AccessToken
	c.expiry = now + response.ExpiresIn
	return c.accessToken, nil
}

func (c *localCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	accessToken, err := c.authorization(ctx)
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

type remoteCredentials struct{}

func authorizationMetadataFromIncomingContext(ctx context.Context) []string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}

	return md.Get("authorization")
}

func (remoteCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	requestMetadata := map[string]string{}
	if authorizationMetadata := authorizationMetadataFromIncomingContext(ctx); len(authorizationMetadata) > 0 {
		requestMetadata["authorization"] = authorizationMetadata[0]
	}

	return requestMetadata, nil
}

func (remoteCredentials) RequireTransportSecurity() bool {
	return true
}

func withAddr(addr string) grpc.DialOption {
	if strings.HasPrefix(addr, "localhost:") || strings.HasPrefix(addr, "127.0.0.1:") {
		return grpc.WithInsecure()
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: !strings.Contains(addr, "."),
	}))
}

func Dial(addr string, useLocalCredentials bool) (*grpc.ClientConn, error) {
	if !useLocalCredentials {
		return grpc.Dial(addr, withAddr(addr), grpc.WithPerRPCCredentials(remoteCredentials{}))
	}

	creds, err := newLocalCredentials()
	if err != nil {
		return nil, err
	}

	return grpc.Dial(addr, withAddr(addr), grpc.WithPerRPCCredentials(creds))
}
