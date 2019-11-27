/*
Package sdk is the gRPC implementation of the SDK gRPC server
Copyright 2018 Portworx

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package sdk

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubernetes-csi/csi-test/utils"
	"github.com/libopenstorage/openstorage/alerts"
	mockalerts "github.com/libopenstorage/openstorage/alerts/mock"
	"github.com/libopenstorage/openstorage/api"
	mockapi "github.com/libopenstorage/openstorage/api/mock"
	clustermanager "github.com/libopenstorage/openstorage/cluster/manager"
	mockcluster "github.com/libopenstorage/openstorage/cluster/mock"
	"github.com/libopenstorage/openstorage/config"
	"github.com/libopenstorage/openstorage/pkg/grpcserver"
	policy "github.com/libopenstorage/openstorage/pkg/storagepolicy"
	"github.com/libopenstorage/openstorage/volume"
	volumedrivers "github.com/libopenstorage/openstorage/volume/drivers"
	mockdriver "github.com/libopenstorage/openstorage/volume/drivers/mock"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/mem"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	mockDriverName = "mock"
	testUds        = "/tmp/sdk-test.sock"
)

// testServer is a simple struct used abstract
// the creation and setup of the gRPC CSI service
type testServer struct {
	conn   *grpc.ClientConn
	server *Server
	m      *mockdriver.MockVolumeDriver
	c      *mockcluster.MockCluster
	a      *mockalerts.MockFilterDeleter
	s      *mockapi.MockOpenStoragePoolServer
	mc     *gomock.Controller
	gw     *httptest.Server
	port   string
	gwport string
}

func init() {
	logrus.SetLevel(logrus.PanicLevel)
}

func setupMockDriver(tester *testServer, t *testing.T) {
	volumedrivers.Add(mockDriverName, func(map[string]string) (volume.VolumeDriver, error) {
		return tester.m, nil
	})

	var err error

	// Register mock driver
	err = volumedrivers.Register(mockDriverName, nil)
	assert.Nil(t, err)
}

func newTestServer(t *testing.T) *testServer {
	tester := &testServer{}
	tester.setPorts()

	// Add driver to registry
	tester.mc = gomock.NewController(&utils.SafeGoroutineTester{})
	tester.m = mockdriver.NewMockVolumeDriver(tester.mc)
	tester.c = mockcluster.NewMockCluster(tester.mc)
	tester.a = mockalerts.NewMockFilterDeleter(tester.mc)
	tester.s = mockapi.NewMockOpenStoragePoolServer(tester.mc)

	setupMockDriver(tester, t)

	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, kvdb.LogFatalErrorCB)
	assert.NoError(t, err)
	kvdb.SetInstance(kv)
	// Init storage policy manager
	_, err = policy.Init()
	sp, err := policy.Inst()
	assert.NotNil(t, sp)

	// Setup simple driver
	os.Remove(testUds)
	tester.server, err = New(&ServerConfig{
		DriverName:          mockDriverName,
		Net:                 "tcp",
		Address:             ":" + tester.port,
		RestPort:            tester.gwport,
		Socket:              testUds,
		Cluster:             tester.c,
		StoragePolicy:       sp,
		AlertsFilterDeleter: tester.a,
		StoragePoolServer:   tester.s,
		AccessOutput:        ioutil.Discard,
		AuditOutput:         ioutil.Discard,
		Security: &SecurityConfig{
			Tls: &TLSConfig{
				CertFile: "test_certs/server-cert.pem",
				KeyFile:  "test_certs/server-key.pem",
			},
		},
	})
	assert.Nil(t, err)
	err = tester.server.Start()
	assert.Nil(t, err)

	grpccreds, err := credentials.NewClientTLSFromFile("test_certs/server-cert.pem", "")
	assert.Nil(t, err)

	// Setup a connection to the driver
	tester.conn, err = grpcserver.Connect("localhost:"+tester.port, []grpc.DialOption{grpc.WithTransportCredentials(grpccreds)})
	assert.Nil(t, err)

	// Setup REST gateway
	mux, err := tester.server.restGateway.restServerSetupHandlers()
	assert.NoError(t, err)
	assert.NotNil(t, mux)
	tester.gw = httptest.NewServer(mux)

	return tester
}

func (s *testServer) setPorts() {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	port := r.Intn(20000) + 10000

	s.port = fmt.Sprintf("%d", port)
	s.gwport = fmt.Sprintf("%d", port+1)
}

func (s *testServer) MockDriver() *mockdriver.MockVolumeDriver {
	return s.m
}

func (s *testServer) MockCluster() *mockcluster.MockCluster {
	return s.c
}

func (s *testServer) MockFilterDeleter() *mockalerts.MockFilterDeleter {
	return s.a
}

func (s *testServer) Stop() {
	// Remove from registry
	volumedrivers.Remove("mock")

	// Shutdown servers
	s.conn.Close()
	s.server.Stop()
	s.gw.Close()

	// Check mocks
	s.mc.Finish()
}

func (s *testServer) Conn() *grpc.ClientConn {
	return s.conn
}

func (s *testServer) Server() grpcserver.Server {
	return s.server.netServer
}

func (s *testServer) UdsServer() grpcserver.Server {
	return s.server.udsServer
}

func (s *testServer) GatewayURL() string {
	return s.gw.URL
}

func TestSdkGateway(t *testing.T) {
	s := newTestServer(t)
	defer s.Stop()

	// Check we can get the swagger.json file
	res, err := http.Get(s.GatewayURL() + "/swagger.json")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)

	// Check we get the swagger-ui
	res, err = http.Get(s.GatewayURL() + "/swagger-ui")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)

	// Check unhandled address
	res, err = http.Get(s.GatewayURL() + "/this-should-not-work")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, res.StatusCode)

	// Check the gateway works
	// First setup the mock
	id := "id"
	name := "name"
	cluster := api.Cluster{
		Id:     name,
		NodeId: "somenodeid",
		Status: api.Status_STATUS_NOT_IN_QUORUM,
	}
	s.MockCluster().EXPECT().Enumerate().Return(cluster, nil).Times(1)
	s.MockCluster().EXPECT().Uuid().Return(id).Times(1)

	// Then send the request
	res, err = http.Get(s.GatewayURL() + "/v1/clusters/inspectcurrent")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)

	// Setup mock for CORS request
	s.MockCluster().EXPECT().Enumerate().Return(cluster, nil).Times(1)
	s.MockCluster().EXPECT().Uuid().Return(id).Times(1)

	// Try cross-origin reqeuest, should get allowed
	reqOrigin := "openstorage.io"
	req, err := http.NewRequest("GET", s.GatewayURL()+"/v1/clusters/inspectcurrent", nil)
	assert.NoError(t, err)
	req.Header.Add("origin", reqOrigin)

	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))

}

func TestSdkWithNoVolumeDriverThenAddOne(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "fake_test", []string{}, nil, kvdb.LogFatalErrorCB)
	if err != nil {
		logrus.Panicf("Failed to initialize KVDB")
	}
	if err := kvdb.SetInstance(kv); err != nil {
		logrus.Panicf("Failed to set KVDB instance")
	}
	clustermanager.Init(config.ClusterConfig{
		ClusterId: "fakecluster",
		NodeId:    "fakeNode",
	})
	cm, err := clustermanager.Inst()
	startChan := make(chan bool)
	go func() {
		cm.Start(false, "9002", "")
		close(startChan)
	}()
	defer cm.Shutdown()
	<-startChan
	if err := volumedrivers.Register("fake", map[string]string{}); err != nil {
		t.Fatalf("Unable to start volume driver fake: %v", err)
	}

	// Setup SDK Server with no volume driver
	alert, err := alerts.NewFilterDeleter(kv)
	assert.NoError(t, err)

	sp, err := policy.Inst()
	os.Remove(testUds)
	tester := &testServer{}
	tester.setPorts()
	tester.mc = gomock.NewController(&utils.SafeGoroutineTester{})
	tester.s = mockapi.NewMockOpenStoragePoolServer(tester.mc)

	server, err := New(&ServerConfig{
		Net:                 "tcp",
		Address:             ":" + tester.port,
		RestPort:            tester.gwport,
		Socket:              testUds,
		Cluster:             cm,
		StoragePolicy:       sp,
		StoragePoolServer:   tester.s,
		AlertsFilterDeleter: alert,
		AccessOutput:        ioutil.Discard,
		AuditOutput:         ioutil.Discard,
		Security: &SecurityConfig{
			Tls: &TLSConfig{
				CertFile: "test_certs/server-cert.pem",
				KeyFile:  "test_certs/server-key.pem",
			},
		},
	})
	assert.Nil(t, err)
	err = server.Start()
	assert.Nil(t, err)
	defer func() {
		server.Stop()
	}()

	grpccreds, err := credentials.NewClientTLSFromFile("test_certs/server-cert.pem", "")
	assert.Nil(t, err)

	// Setup a connection to the driver
	conn, err := grpc.Dial("localhost:"+tester.port, grpc.WithTransportCredentials(grpccreds))

	// Setup API names that depend on the volume driver
	// To get the names, look at api.pb.go and search for grpc.Invoke or c.cc.Invoke
	apis := []string{
		"/openstorage.api.OpenStorageVolume/Create",
		"/openstorage.api.OpenStorageVolume/Clone",
		"/openstorage.api.OpenStorageVolume/Delete",
		"/openstorage.api.OpenStorageVolume/Inspect",
		"/openstorage.api.OpenStorageVolume/Stats",
		"/openstorage.api.OpenStorageVolume/Enumerate",
		"/openstorage.api.OpenStorageVolume/EnumerateWithFilters",
		"/openstorage.api.OpenStorageVolume/SnapshotCreate",
		"/openstorage.api.OpenStorageVolume/SnapshotRestore",
		"/openstorage.api.OpenStorageVolume/SnapshotEnumerate",
		"/openstorage.api.OpenStorageVolume/SnapshotEnumerateWithFilters",
		"/openstorage.api.OpenStorageVolume/SnapshotScheduleUpdate",
		"/openstorage.api.OpenStorageMountAttach/Attach",
		"/openstorage.api.OpenStorageMountAttach/Detach",
		"/openstorage.api.OpenStorageMountAttach/Mount",
		"/openstorage.api.OpenStorageMountAttach/Unmount",
		"/openstorage.api.OpenStorageCloudBackup/Create",
		"/openstorage.api.OpenStorageCloudBackup/Restore",
		"/openstorage.api.OpenStorageCloudBackup/Delete",
		"/openstorage.api.OpenStorageCloudBackup/DeleteAll",
		"/openstorage.api.OpenStorageCloudBackup/EnumerateWithFilters",
		"/openstorage.api.OpenStorageCloudBackup/Status",
		"/openstorage.api.OpenStorageCloudBackup/Catalog",
		"/openstorage.api.OpenStorageCloudBackup/History",
		"/openstorage.api.OpenStorageCloudBackup/StateChange",
		"/openstorage.api.OpenStorageCloudBackup/SchedCreate",
		"/openstorage.api.OpenStorageCloudBackup/SchedDelete",
		"/openstorage.api.OpenStorageCloudBackup/SchedEnumerate",
		"/openstorage.api.OpenStorageCredentials/Create",
		"/openstorage.api.OpenStorageCredentials/Enumerate",
		"/openstorage.api.OpenStorageCredentials/Inspect",
		"/openstorage.api.OpenStorageCredentials/Delete",
		"/openstorage.api.OpenStorageCredentials/Validate",
	}

	// The main purpose of this test is to make sure that the server
	// does not panic using a nil point to a driver
	for _, api := range apis {
		err = conn.Invoke(context.Background(), api, nil, nil)
		assert.Error(t, err)
		serverError, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, serverError.Code(), codes.Unavailable)
		assert.Contains(t, serverError.Message(), "Resource")
	}

	// Check the driver is not loaded
	identities := api.NewOpenStorageIdentityClient(conn)
	id, err := identities.Version(context.Background(), &api.SdkIdentityVersionRequest{})
	assert.NoError(t, err)
	assert.Contains(t, id.GetVersion().GetDriver(), "no driver")

	// Now add the volume driver
	d, err := volumedrivers.Get("fake")
	assert.NoError(t, err)
	driverMap := map[string]volume.VolumeDriver{"fake": d, DefaultDriverName: d}
	server.UseVolumeDrivers(driverMap)

	// Identify that the driver is now running
	id, err = identities.Version(context.Background(), &api.SdkIdentityVersionRequest{})
	assert.NoError(t, err)
	assert.Equal(t, "fake", id.GetVersion().GetDriver())

	// This part of the test we cannot simply send nils for request and response
	// because real data is being passed. Therefore, a single call will satisfy that
	// the driver is working now.
	volumes := api.NewOpenStorageVolumeClient(conn)
	r, err := volumes.Create(context.Background(), &api.SdkVolumeCreateRequest{
		Name: "myvol",
		Spec: &api.VolumeSpec{
			Size:    uint64(12345),
			HaLevel: 1,
		},
	})
	assert.NoError(t, err)
	assert.True(t, len(r.GetVolumeId()) != 0)
}
