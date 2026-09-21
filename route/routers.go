package route

import (
	"crypto/ecdsa"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/ReCasaOS/CasaOS-Common/external"
	"github.com/ReCasaOS/CasaOS-Common/utils/jwt"
	"github.com/ReCasaOS/CasaOS-MessageBus/codegen"
	"github.com/ReCasaOS/CasaOS-MessageBus/config"
	"github.com/ReCasaOS/CasaOS-MessageBus/service"
	"github.com/deepmap/oapi-codegen/pkg/middleware"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	echojwt "github.com/labstack/echo-jwt/v4"
	"github.com/labstack/echo/v4"
	echo_middleware "github.com/labstack/echo/v4/middleware"
)

// eventPublishPrefix is the route casaos POSTs casaos:system:utilization to
// every 5s (route/periodical.go). Those loopback publishes are dropped from
// the access log (issue #2211); everything else stays logged.
const eventPublishPrefix = "/v2/message_bus/event/"

// fromHost reports whether the request came from this machine: the unix
// socket (app-management) or a loopback TCP peer (casaos, local-storage).
func fromHost(realIP, host string) bool {
	return host == "unix" || realIP == "::1" || realIP == "127.0.0.1"
}

// skipAccessLog reports whether the access-log line is dropped for a request.
// GET is the websocket subscribe and stays logged.
func skipAccessLog(method, path, realIP, host string) bool {
	return method == http.MethodPost && strings.HasPrefix(path, eventPublishPrefix) && fromHost(realIP, host)
}

// fromUnixSocket reports whether the request came in on the unix socket
// listener (main.go), which only root can connect to; app-management publishes
// its events there. The Host header ("unix" from that client) proves nothing:
// any TCP client can send it, through the gateway too.
func fromUnixSocket(r *http.Request) bool {
	addr, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	return ok && addr.Network() == "unix"
}

// websocketSubscribeRoutes are the GET routes (as registered by codegen) a
// client subscribes on with a WebSocket upgrade: subscribeEventWS,
// subscribeActionWS, subscribeSIO and subscribeSIO2. Socket.IO polling is not
// among them: the dashboard opens the websocket transport directly.
var websocketSubscribeRoutes = map[string]bool{
	"/v2/message_bus/event/:source_id":  true,
	"/v2/message_bus/action/:source_id": true,
	"/v2/message_bus/socket.io":         true,
	"/v2/message_bus/socket.io/":        true,
}

// skipJWT reports whether a request needs no user token: it comes from one of
// this box's services (over the unix socket, or from loopback with the internal
// secret the gateway writes each boot), or it is a WebSocket subscribe.
func skipJWT(c echo.Context) bool {
	r := c.Request()
	if fromUnixSocket(r) || external.IsInternalRequest(c.RealIP(), r.Header.Get(echo.HeaderAuthorization), config.CommonInfo.RuntimePath) {
		return true
	}

	// Browsers cannot set headers on a WebSocket and the dashboard subscribes
	// without a token, so the event stream stays open to anyone who reaches it.
	// c.Path() is the route the router matched (it runs before middleware), so
	// the Upgrade header opens those routes only, not every GET.
	return r.Method == echo.GET && strings.EqualFold(r.Header.Get(echo.HeaderUpgrade), "websocket") && websocketSubscribeRoutes[c.Path()]
}

func NewAPIRouter(swagger *openapi3.T, services *service.Services) (http.Handler, error) {
	apiRoute := NewAPIRoute(services)

	e := echo.New()

	e.Use((echo_middleware.CORSWithConfig(echo_middleware.CORSConfig{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{echo.POST, echo.GET, echo.OPTIONS, echo.PUT, echo.DELETE},
		AllowHeaders:     []string{echo.HeaderAuthorization, echo.HeaderContentLength, echo.HeaderXCSRFToken, echo.HeaderContentType, echo.HeaderAccessControlAllowOrigin, echo.HeaderAccessControlAllowHeaders, echo.HeaderAccessControlAllowMethods, echo.HeaderConnection, echo.HeaderOrigin, echo.HeaderXRequestedWith},
		ExposeHeaders:    []string{echo.HeaderContentLength, echo.HeaderAccessControlAllowOrigin, echo.HeaderAccessControlAllowHeaders},
		MaxAge:           172800,
		AllowCredentials: true,
	})))

	e.Use(echo_middleware.Gzip())
	e.Use(echo_middleware.Recover())
	e.Use(echo_middleware.LoggerWithConfig(echo_middleware.LoggerConfig{
		Skipper: func(c echo.Context) bool {
			r := c.Request()
			return skipAccessLog(r.Method, r.URL.Path, c.RealIP(), r.Host)
		},
	}))

	e.Use(echojwt.WithConfig(echojwt.Config{
		Skipper: skipJWT,
		ParseTokenFunc: func(c echo.Context, token string) (interface{}, error) {
			valid, claims, err := jwt.Validate(token, func() (*ecdsa.PublicKey, error) { return external.GetPublicKey(config.CommonInfo.RuntimePath) })
			if err != nil || !valid {
				return nil, echo.ErrUnauthorized
			}

			c.Request().Header.Set("user_id", strconv.Itoa(claims.ID))

			return claims, nil
		},
		TokenLookupFuncs: []echo_middleware.ValuesExtractor{
			func(c echo.Context) ([]string, error) {
				return []string{c.Request().Header.Get(echo.HeaderAuthorization)}, nil
			},
		},
	}))

	e.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{Options: openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}}))

	apiPath, err := getAPIPath(getSwaggerURL(swagger))
	if err != nil {
		return nil, err
	}

	codegen.RegisterHandlersWithBaseURL(e, apiRoute, apiPath)

	return e, nil
}

func NewDocRouter(swagger *openapi3.T, docHTML string, docYAML string) (http.Handler, error) {
	apiPath, err := getAPIPath(getSwaggerURL(swagger))
	if err != nil {
		return nil, err
	}

	docPath := "/doc" + apiPath

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == docPath {
			if _, err := w.Write([]byte(docHTML)); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}

		if r.URL.Path == docPath+"/openapi.yaml" {
			if _, err := w.Write([]byte(docYAML)); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}
	}), nil
}

func getSwaggerURL(swagger *openapi3.T) string {
	return swagger.Servers[0].URL
}

func getAPIPath(swaggerURL string) (string, error) {
	u, err := url.Parse(swaggerURL)
	if err != nil {
		return "", err
	}

	return strings.TrimRight(u.Path, "/"), nil
}
