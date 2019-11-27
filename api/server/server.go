package server

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/libopenstorage/openstorage/pkg/auth"
)

// Route is a specification and  handler for a REST endpoint.
type Route struct {
	verb string
	path string
	fn   func(http.ResponseWriter, *http.Request)
}

// AnonRegex defines a regex replace for a string
type AnonRegex struct {
	anonymizeRegx *regexp.Regexp
	replaceString string
}

// anonIDRegxes[] used to hide ID content when logging.
var anonIDRegxes = []AnonRegex{
	{
		// Anonymize the token=... and replace with token=***..*
		// JWT Regex below pulled from - https://www.regextester.com/105777
		anonymizeRegx: regexp.MustCompile(`token=[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.?[A-Za-z0-9-_.+/=]*`),
		replaceString: `token=********`,
	},
}

func (r *Route) GetVerb() string {
	return r.verb
}

func (r *Route) GetPath() string {
	return r.path
}

func (r *Route) GetFn() func(http.ResponseWriter, *http.Request) {
	return r.fn
}

// StartGraphAPI starts a REST server to receive GraphDriver commands from
// the Linux container engine.
func StartGraphAPI(name string, restBase string) error {
	graphPlugin := newGraphPlugin(name)
	if _, _, err := startServer(name, restBase, 0, graphPlugin); err != nil {
		return err
	}

	return nil
}

// StartVolumeMgmtAPI starts a REST server to receive volume management API commands
func StartVolumeMgmtAPI(
	name, sdkUds string,
	mgmtBase string,
	mgmtPort uint16,
	auth bool,
	authenticators map[string]auth.Authenticator,
) (*http.Server, *http.Server, error) {
	var (
		unixServer, portServer *http.Server
		err                    error
	)
	volMgmtApi := newVolumeAPI(name, sdkUds)

	if auth {
		unixServer, portServer, err = startServerWithAuth(
			name,
			mgmtBase,
			mgmtPort,
			volMgmtApi,
			authenticators,
		)
	} else {
		unixServer, portServer, err = startServer(
			name,
			mgmtBase,
			mgmtPort,
			volMgmtApi,
		)

	}
	return unixServer, portServer, err
}

// StartVolumePluginAPI starts a REST server to receive volume API commands
// from the linux container  engine
func StartVolumePluginAPI(
	name, sdkUds string,
	pluginBase string,
	pluginPort uint16,
) error {
	volPluginApi := newVolumePlugin(name, sdkUds)
	if _, _, err := startServer(
		name,
		pluginBase,
		pluginPort,
		volPluginApi,
	); err != nil {
		return err
	}
	return nil
}

// StartClusterAPI starts a REST server to receive driver configuration commands
// from the CLI/UX to control the OSD cluster.
func StartClusterAPI(clusterApiBase string, clusterPort uint16, authenticators map[string]auth.Authenticator) error {
	clusterApi := newClusterAPI()

	// start server as before
	if _, _, err := startServerWithAuth("osd", clusterApiBase, clusterPort, clusterApi, authenticators); err != nil {
		return err
	}

	return nil
}

func GetClusterAPIRoutes() []*Route {
	clusterApi := newClusterAPI()
	return clusterApi.Routes()
}

func SetClusterAPIRoutesWithAuth(router *mux.Router, authenticators map[string]auth.Authenticator) (*mux.Router, error) {
	clusterApi := newClusterAPI()
	return clusterApi.SetupRoutesWithAuth(router, authenticators)
}

func GetCredentialsRoutes() []*Route {
	return (&volAPI{}).credsRoutes()
}

func SetCredentialsRoutesWithAuth(router *mux.Router, authenticators map[string]auth.Authenticator) (*mux.Router, error) {
	volApi := &volAPI{}
	credRoutes := volApi.credsRoutes()
	securityMiddleware := newSecurityMiddleware(authenticators)

	for _, route := range credRoutes {
		router.Methods(route.GetVerb()).Path(route.GetPath()).HandlerFunc(securityMiddleware(route.fn))
	}

	return router, nil
}

func startServerWithAuth(
	name, sockBase string,
	port uint16,
	rs restServer,
	authenticators map[string]auth.Authenticator,
) (*http.Server, *http.Server, error) {
	var err error
	router := mux.NewRouter()
	router.NotFoundHandler = http.HandlerFunc(notFound)
	router, err = rs.SetupRoutesWithAuth(router, authenticators)
	if err != nil {
		return nil, nil, err
	}
	return startServerCommon(name, sockBase, port, rs, router)
}

func startServer(name string, sockBase string, port uint16, rs restServer) (*http.Server, *http.Server, error) {
	router := mux.NewRouter()
	router.NotFoundHandler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Write([]byte("Hello2"))
	})
	for _, v := range rs.Routes() {
		router.Methods(v.verb).Path(v.path).HandlerFunc(v.fn)
	}
	return startServerCommon(name, sockBase, port, rs, router)
}

func startServerCommon(name string, sockBase string, port uint16, rs restServer, router *mux.Router) (*http.Server, *http.Server, error) {
	var (
		listener net.Listener
		err      error
	)
	socket := path.Join(sockBase, name+".sock")
	os.Remove(socket)
	os.MkdirAll(path.Dir(socket), 0755)

	logrus.Printf("Starting REST service on socket : %+v", socket)
	listener, err = net.Listen("unix", socket)
	if err != nil {
		logrus.Warnln("Cannot listen on UNIX socket: ", err)
		return nil, nil, err
	}
	unixServer := &http.Server{Handler: router}
	go func() {
		if err := unixServer.Serve(listener); err != nil {
			logrus.Errorf("error starting unix server %v", err)
		}
	}()

	if port != 0 {
		logrus.Printf("Starting REST service on port : %v", port)
		portServer := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: router}
		go portServer.ListenAndServe()
		return unixServer, portServer, nil
	}
	return unixServer, nil, nil
}

type restServer interface {
	Routes() []*Route
	SetupRoutesWithAuth(router *mux.Router, authenticators map[string]auth.Authenticator) (*mux.Router, error)
	String() string
	logRequest(request string, id string) *logrus.Entry
	sendError(request string, id string, w http.ResponseWriter, msg string, code int)
}

type restBase struct {
	restServer
	version string
	name    string
}

func regxAnonString(inputStr string, anonRegxes []AnonRegex) string {
	anonString := inputStr
	if len(anonString) > 0 {
		for _, anonRegx := range anonRegxes {
			anonString = anonRegx.anonymizeRegx.ReplaceAllString(
				anonString,
				anonRegx.replaceString)
		}
	}
	return anonString
}

func (rest *restBase) logRequest(request string, id string) *logrus.Entry {
	return logrus.WithFields(map[string]interface{}{
		"Driver":  rest.name,
		"Request": request,
		"ID":      regxAnonString(id, anonIDRegxes),
	})
}
func (rest *restBase) sendError(request string, id string, w http.ResponseWriter, msg string, code int) {
	rest.logRequest(request, id).Warnln(code, " ", msg)
	http.Error(w, msg, code)
}

func notFound(w http.ResponseWriter, r *http.Request) {
	logrus.Warnf("Not found: %+v ", r.URL)
	http.NotFound(w, r)
}
