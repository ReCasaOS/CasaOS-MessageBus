package route

import (
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
	"github.com/ReCasaOS/CasaOS-MessageBus/codegen"
	"github.com/ReCasaOS/CasaOS-MessageBus/config"
	"github.com/ReCasaOS/CasaOS-MessageBus/repository"
	"github.com/ReCasaOS/CasaOS-MessageBus/service"
	"github.com/labstack/echo/v4"
	"gotest.tools/assert"
)

// TestAPIRouterAuth drives the whole router (JWT middleware, request validator,
// handlers) to check who gets in without a user token.
func TestAPIRouterAuth(t *testing.T) {
	runtimePath := t.TempDir()
	assert.NilError(t, os.WriteFile(filepath.Join(runtimePath, external.InternalSecretFilename), []byte("s3cret\n"), 0o600))
	defer func(old string) { config.CommonInfo.RuntimePath = old }(config.CommonInfo.RuntimePath)
	config.CommonInfo.RuntimePath = runtimePath

	repository, err := repository.NewDatabaseRepositoryInMemory()
	assert.NilError(t, err)
	defer repository.Close()

	services := service.NewServices(&repository)
	swagger, err := codegen.GetSwagger()
	assert.NilError(t, err)
	router, err := NewAPIRouter(swagger, &services)
	assert.NilError(t, err)

	const (
		eventTypes = "/v2/message_bus/event_type"
		body       = `[{"sourceID":"foo","name":"bar","propertyTypeList":[]}]`
		lan        = "192.168.1.20:40000"
	)

	tests := []struct {
		name, method, target, remoteAddr, authorization string
		header                                          map[string]string
		unixSocket                                      bool
		expected                                        int
	}{
		{name: "loopback without the secret needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: "127.0.0.1:40000", expected: http.StatusUnauthorized},
		{name: "loopback with a wrong secret needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: "127.0.0.1:40000", authorization: "Internal nope", expected: http.StatusUnauthorized},
		{name: "loopback with the secret passes", method: http.MethodPost, target: eventTypes, remoteAddr: "127.0.0.1:40000", authorization: "Internal s3cret", expected: http.StatusOK},
		{name: "IPv6 loopback with the secret passes", method: http.MethodGet, target: eventTypes, remoteAddr: "[::1]:40000", authorization: "Internal s3cret", expected: http.StatusOK},
		{name: "LAN with the secret needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: lan, authorization: "Internal s3cret", expected: http.StatusUnauthorized},
		{name: "LAN forwarded as loopback needs a JWT", method: http.MethodGet, target: eventTypes, remoteAddr: "127.0.0.1:40000", header: map[string]string{echo.HeaderXForwardedFor: "127.0.0.1, 192.168.1.20"}, expected: http.StatusUnauthorized},
		{name: "Host unix over TCP needs a JWT", method: http.MethodGet, target: "http://unix" + eventTypes, remoteAddr: lan, expected: http.StatusUnauthorized},
		{name: "the unix socket passes", method: http.MethodGet, target: "http://unix" + eventTypes, remoteAddr: "@", unixSocket: true, expected: http.StatusOK},
		// no event type for this source: 400 from the handler, past the JWT middleware
		{name: "websocket upgrade GET passes", method: http.MethodGet, target: "/v2/message_bus/event/nobody", remoteAddr: lan, header: map[string]string{echo.HeaderUpgrade: "websocket", echo.HeaderConnection: "Upgrade"}, expected: http.StatusBadRequest},
	}

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
				req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, &net.UnixAddr{Name: "/tmp/message-bus.sock", Net: "unix"}))
			}

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.Equal(t, rec.Code, tt.expected, rec.Body.String())
		})
	}
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
