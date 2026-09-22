package route

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReCasaOS/CasaOS-Common/external"
	"github.com/ReCasaOS/CasaOS-Common/utils/jwt"
	"github.com/ReCasaOS/CasaOS-MessageBus/codegen"
	"github.com/ReCasaOS/CasaOS-MessageBus/config"
	"github.com/ReCasaOS/CasaOS-MessageBus/repository"
	"github.com/ReCasaOS/CasaOS-MessageBus/service"
	"github.com/labstack/echo/v4"
	echo_middleware "github.com/labstack/echo/v4/middleware"
	"gotest.tools/assert"
)

// TestAPIRouterAuth drives the whole router (JWT middleware, request validator,
// handlers) to check who gets past authentication.
func TestAPIRouterAuth(t *testing.T) {
	runtimePath := t.TempDir()
	assert.NilError(t, os.WriteFile(filepath.Join(runtimePath, external.InternalSecretFilename), []byte("s3cret\n"), 0o600))
	defer func(old string) { config.CommonInfo.RuntimePath = old }(config.CommonInfo.RuntimePath)
	config.CommonInfo.RuntimePath = runtimePath

	// user-service stand-in: external.GetPublicKey reads its address from the
	// runtime path and fetches the JWKS from it.
	privateKey, publicKey, err := jwt.GenerateKeyPair()
	assert.NilError(t, err)
	jwks, err := jwt.GenerateJwksJSON(publicKey)
	assert.NilError(t, err)
	userService := httptest.NewServer(jwt.JWKSHandler(jwks))
	defer userService.Close()
	assert.NilError(t, os.WriteFile(filepath.Join(runtimePath, external.UserServiceAddressFilename), []byte(userService.URL), 0o600))
	token, err := jwt.GetAccessToken("admin", privateKey, 1)
	assert.NilError(t, err)

	repository, err := repository.NewDatabaseRepositoryInMemory()
	assert.NilError(t, err)
	defer repository.Close()

	// The access logger takes its output from the default config when the
	// router is built.
	var accessLog bytes.Buffer
	defer func(old io.Writer) { echo_middleware.DefaultLoggerConfig.Output = old }(echo_middleware.DefaultLoggerConfig.Output)
	echo_middleware.DefaultLoggerConfig.Output = &accessLog

	services := service.NewServices(&repository)
	swagger, err := codegen.GetSwagger()
	assert.NilError(t, err)
	router, err := NewAPIRouter(swagger, &services)
	assert.NilError(t, err)

	const (
		eventTypes = "/v2/message_bus/event_type"
		body       = `[{"sourceID":"foo","name":"bar","propertyTypeList":[]}]`
		lan        = "192.168.1.20:40000"
		loopback   = "127.0.0.1:40000"
		polling    = "?EIO=4&transport=polling&sid=nope"
	)
	upgrade := map[string]string{echo.HeaderUpgrade: "websocket", echo.HeaderConnection: "Upgrade"}
	withToken := func(target, token string) string {
		if strings.Contains(target, "?") {
			return target + "&token=" + token
		}
		return target + "?token=" + token
	}

	type testCase struct {
		name, method, target, remoteAddr, authorization string
		header                                          map[string]string
		unixSocket                                      bool
		expected                                        int
	}

	// The subscription routes. 400 comes from the handler (no such type) or
	// from engine.io (no transport, unknown sid): past the JWT middleware.
	var subscriptions []testCase
	for _, s := range []struct {
		name, method, target string
		header               map[string]string
	}{
		{"event websocket", http.MethodGet, "/v2/message_bus/event/nobody", upgrade},
		{"action websocket", http.MethodGet, "/v2/message_bus/action/nobody?names=x", upgrade},
		{"socket.io websocket", http.MethodGet, "/v2/message_bus/socket.io", upgrade},
		{"socket.io/ websocket", http.MethodGet, "/v2/message_bus/socket.io/", upgrade},
		{"socket.io polling GET", http.MethodGet, "/v2/message_bus/socket.io" + polling, nil},
		{"socket.io/ polling GET", http.MethodGet, "/v2/message_bus/socket.io/" + polling, nil},
		{"socket.io polling POST", http.MethodPost, "/v2/message_bus/socket.io" + polling, nil},
		{"socket.io/ polling POST", http.MethodPost, "/v2/message_bus/socket.io/" + polling, nil},
	} {
		subscriptions = append(subscriptions,
			testCase{name: s.name + " without a credential needs a JWT", method: s.method, target: s.target, remoteAddr: lan, header: s.header, expected: http.StatusUnauthorized},
			testCase{name: s.name + " with ?token passes", method: s.method, target: withToken(s.target, token), remoteAddr: lan, header: s.header, expected: http.StatusBadRequest},
			testCase{name: s.name + " with a tampered ?token needs a valid one", method: s.method, target: withToken(s.target, token+"x"), remoteAddr: lan, header: s.header, expected: http.StatusUnauthorized},
			testCase{name: s.name + " with a user JWT header passes", method: s.method, target: s.target, remoteAddr: lan, authorization: token, header: s.header, expected: http.StatusBadRequest},
			testCase{name: s.name + " with the secret from loopback passes", method: s.method, target: s.target, remoteAddr: loopback, authorization: "Internal s3cret", header: s.header, expected: http.StatusBadRequest},
			testCase{name: s.name + " with the secret from the LAN needs a JWT", method: s.method, target: s.target, remoteAddr: lan, authorization: "Internal s3cret", header: s.header, expected: http.StatusUnauthorized},
		)
	}

	tests := append(subscriptions, []testCase{
		{name: "loopback without the secret needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: "127.0.0.1:40000", expected: http.StatusUnauthorized},
		{name: "loopback with a wrong secret needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: "127.0.0.1:40000", authorization: "Internal nope", expected: http.StatusUnauthorized},
		{name: "loopback with the secret passes", method: http.MethodPost, target: eventTypes, remoteAddr: "127.0.0.1:40000", authorization: "Internal s3cret", expected: http.StatusOK},
		{name: "IPv6 loopback with the secret passes", method: http.MethodGet, target: eventTypes, remoteAddr: "[::1]:40000", authorization: "Internal s3cret", expected: http.StatusOK},
		{name: "LAN with the secret needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: lan, authorization: "Internal s3cret", expected: http.StatusUnauthorized},
		{name: "LAN forwarded as loopback needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: "127.0.0.1:40000", header: map[string]string{echo.HeaderXForwardedFor: "127.0.0.1, 192.168.1.20"}, expected: http.StatusUnauthorized},
		{name: "Host unix over TCP needs a JWT", method: http.MethodGet, target: "http://unix" + eventTypes, remoteAddr: lan, expected: http.StatusUnauthorized},
		{name: "the unix socket passes", method: http.MethodGet, target: "http://unix" + eventTypes, remoteAddr: "@", unixSocket: true, expected: http.StatusOK},
		{name: "LAN with a user JWT passes", method: http.MethodGet, target: eventTypes, remoteAddr: lan, authorization: token, expected: http.StatusOK},
		{name: "LAN with a tampered JWT needs a valid one", method: http.MethodGet, target: eventTypes, remoteAddr: lan, authorization: token + "x", expected: http.StatusUnauthorized},
		// ?token is read on the subscription routes only.
		{name: "?token on event_type needs a JWT", method: http.MethodGet, target: withToken(eventTypes, token), remoteAddr: lan, expected: http.StatusUnauthorized},
		{name: "?token on an event type registration needs a JWT", method: http.MethodPost, target: withToken(eventTypes, token), remoteAddr: lan, expected: http.StatusUnauthorized},
		{name: "?token on an event publish needs a JWT", method: http.MethodPost, target: withToken("/v2/message_bus/event/nobody/x", token), remoteAddr: lan, expected: http.StatusUnauthorized},
		{name: "?token on ysk needs a JWT", method: http.MethodGet, target: withToken("/v2/message_bus/ysk", token), remoteAddr: lan, header: upgrade, expected: http.StatusUnauthorized},
		{name: "?token with POST on the event subscribe path needs a JWT", method: http.MethodPost, target: withToken("/v2/message_bus/event/nobody", token), remoteAddr: lan, expected: http.StatusUnauthorized},
		// A websocket upgrade opens nothing by itself.
		{name: "websocket upgrade on event_type needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: lan, header: upgrade, expected: http.StatusUnauthorized},
		{name: "websocket upgrade on action_type needs a JWT", method: http.MethodGet, target: "/v2/message_bus/action_type", remoteAddr: lan, header: upgrade, expected: http.StatusUnauthorized},
	}...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reqBody io.Reader
			if tt.method == http.MethodPost {
				reqBody = strings.NewReader(body)
			}
			req := httptest.NewRequest(tt.method, tt.target, reqBody)
			req.RemoteAddr = tt.remoteAddr
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			if tt.authorization != "" {
				req.Header.Set(echo.HeaderAuthorization, tt.authorization)
			}
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			if tt.unixSocket {
				req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, &net.UnixAddr{Name: external.MessageBusSocketPath(runtimePath), Net: "unix"}))
			}

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.Equal(t, rec.Code, tt.expected, rec.Body.String())
		})
	}

	// The access log keeps the path of the ?token requests, not their query.
	assert.Assert(t, strings.Contains(accessLog.String(), `"path":"/v2/message_bus/event/nobody"`), accessLog.String())
	assert.Assert(t, !strings.Contains(accessLog.String(), token), "the access log holds a token")
}

func TestSkipAccessLog(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		path     string
		realIP   string
		host     string
		expected bool
	}{
		{"utilization publish from loopback IPv4", http.MethodPost, "/v2/message_bus/event/casaos/casaos:system:utilization", "127.0.0.1", "127.0.0.1:43251", true},
		{"utilization publish from loopback IPv6", http.MethodPost, "/v2/message_bus/event/casaos/casaos:system:utilization", "::1", "[::1]:43251", true},
		{"publish over the unix socket", http.MethodPost, "/v2/message_bus/event/app-management/app:install:progress", "", "unix", true},
		{"publish from the LAN stays logged", http.MethodPost, "/v2/message_bus/event/casaos/casaos:system:utilization", "192.168.1.20", "nas.local", false},
		{"websocket subscribe stays logged", http.MethodGet, "/v2/message_bus/event/casaos", "127.0.0.1", "127.0.0.1:1", false},
		{"event type registration stays logged", http.MethodPost, "/v2/message_bus/event_type", "127.0.0.1", "127.0.0.1:1", false},
		{"action publish stays logged", http.MethodPost, "/v2/message_bus/action/casaos/x", "127.0.0.1", "127.0.0.1:1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := skipAccessLog(tt.method, tt.path, tt.realIP, tt.host); actual != tt.expected {
				t.Errorf("skipAccessLog(%q, %q, %q, %q) = %v, want %v", tt.method, tt.path, tt.realIP, tt.host, actual, tt.expected)
			}
		})
	}
}
