/*
CSI Interface for OSD
Copyright 2017 Portworx

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
package csi

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-csi/csi-test/utils"
	mockapi "github.com/libopenstorage/openstorage/api/mock"
	"github.com/libopenstorage/openstorage/api/server/sdk"
	"github.com/libopenstorage/openstorage/cluster"
	clustermanager "github.com/libopenstorage/openstorage/cluster/manager"
	mockcluster "github.com/libopenstorage/openstorage/cluster/mock"
	"github.com/libopenstorage/openstorage/config"
	"github.com/libopenstorage/openstorage/pkg/auth"
	"github.com/libopenstorage/openstorage/pkg/grpcserver"
	"github.com/libopenstorage/openstorage/pkg/options"
	"github.com/libopenstorage/openstorage/pkg/role"
	"github.com/libopenstorage/openstorage/pkg/storagepolicy"
	"github.com/libopenstorage/openstorage/volume"
	volumedrivers "github.com/libopenstorage/openstorage/volume/drivers"
	mockdriver "github.com/libopenstorage/openstorage/volume/drivers/mock"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/mem"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	mockDriverName   = "mock"
	testSharedSecret = "mysecret"
	fakeWithSched    = "fake-sched"
)

var (
	cm              cluster.Cluster
	systemUserToken string
)

func init() {
	setupFakeDriver()
}

// testServer is a simple struct used abstract
// the creation and setup of the gRPC CSI service
type testServer struct {
	conn   *grpc.ClientConn
	server grpcserver.Server
	m      *mockdriver.MockVolumeDriver
	c      *mockcluster.MockCluster
	s      *mockapi.MockOpenStoragePoolServer
	mc     *gomock.Controller
	sdk    *sdk.Server
	port   string
	gwport string
	uds    string
}

func setupFakeDriver() {
	kv, err := kvdb.New(mem.Name, "fake_test", []string{}, nil, kvdb.LogFatalErrorCB)
	if err != nil {
		logrus.Panicf("Failed to initialize KVDB")
	}
	if err := kvdb.SetInstance(kv); err != nil {
		logrus.Panicf("Failed to set KVDB instance")
	}
	// Need to setup a fake cluster. No need to start it.
	clustermanager.Init(config.ClusterConfig{
		ClusterId: "fakecluster",
		NodeId:    "fakeNode",
	})
	cm, err = clustermanager.Inst()
	if err != nil {
		logrus.Panicf("Unable to initialize cluster manager: %v", err)
	}

	// Requires a non-nil cluster
	if err := volumedrivers.Register("fake", map[string]string{}); err != nil {
		logrus.Panicf("Unable to start volume driver fake: %v", err)
	}
}

func createToken(t *testing.T, name, role string) string {
	claims := &auth.Claims{
		Issuer: "openstorage.io",
		Name:   name,
		Email:  name + "@openstorage.io",
		Roles:  []string{role},
	}
	signature := &auth.Signature{
		Key:  []byte(testSharedSecret),
		Type: jwt.SigningMethodHS256,
	}
	options := &auth.Options{
		Expiration: time.Now().Add(1 * time.Hour).Unix(),
	}
	token, err := auth.Token(claims, signature, options)
	assert.NoError(t, err)
	return token
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
	return newTestServerWithConfig(t, &OsdCsiServerConfig{
		DriverName: mockDriverName,
	})
}

func newTestServerWithConfig(t *testing.T, config *OsdCsiServerConfig) *testServer {
	tester := &testServer{}
	tester.setPorts()

	// Add driver to registry
	tester.mc = gomock.NewController(&utils.SafeGoroutineTester{})
	tester.m = mockdriver.NewMockVolumeDriver(tester.mc)
	tester.c = mockcluster.NewMockCluster(tester.mc)
	tester.s = mockapi.NewMockOpenStoragePoolServer(tester.mc)

	if config.Cluster == nil {
		config.Cluster = tester.c
	}

	setupMockDriver(tester, t)

	// Initialise storage policy manager
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, kvdb.LogFatalErrorCB)
	assert.NoError(t, err)
	rm, err := role.NewSdkRoleManager(kv)
	assert.NoError(t, err)

	// Setup storage policy
	kv, err = kvdb.New(mem.Name, "test", []string{}, nil, kvdb.LogFatalErrorCB)
	assert.NoError(t, err)
	kvdb.SetInstance(kv)
	stp, err := storagepolicy.Init()
	if err != nil {
		stp, _ = storagepolicy.Inst()
	}
	assert.NotNil(t, stp)

	selfsignedJwt, err := auth.NewJwtAuth(&auth.JwtAuthConfig{
		SharedSecret:  []byte(testSharedSecret),
		UsernameClaim: auth.UsernameClaimTypeName,
	})

	// setup sdk server
	tester.sdk, err = sdk.New(&sdk.ServerConfig{
		DriverName:        "fake",
		Net:               "tcp",
		Address:           ":" + tester.port,
		RestPort:          tester.gwport,
		Cluster:           tester.c,
		Socket:            tester.uds,
		StoragePolicy:     stp,
		StoragePoolServer: tester.s,
		AccessOutput:      ioutil.Discard,
		AuditOutput:       ioutil.Discard,
		Security: &sdk.SecurityConfig{
			Role: rm,
			Authenticators: map[string]auth.Authenticator{
				"openstorage.io": selfsignedJwt,
			},
		},
	})
	assert.Nil(t, err)
	err = tester.sdk.Start()
	assert.Nil(t, err)
	tester.sdk.UseVolumeDrivers(map[string]volume.VolumeDriver{
		"mock":    tester.m,
		"default": tester.m,
	})

	// Setup CSI simple driver
	config.Net = "tcp"
	config.Address = "127.0.0.1:0"
	config.SdkUds = tester.uds
	tester.server, err = NewOsdCsiServer(config)
	assert.Nil(t, err)
	err = tester.server.Start()
	assert.Nil(t, err)

	// Setup a connection to the driver
	tester.conn, err = grpc.Dial(tester.server.Address(), grpc.WithInsecure())
	assert.Nil(t, err)

	systemUserToken = createToken(t, "user1", "system.user")
	// Setup fake-sched driver for REST UTs
	// Point it to the fake driver head
	/*fakeDriver, err := volumedrivers.Get(fake.Name)
	assert.NoError(t, err)
	volumedrivers.Add(fakeWithSched,
		func(params map[string]string) (volume.VolumeDriver, error) {
			return fakeDriver, nil
		},
	)
	volumedrivers.Register(fakeWithSched, nil)
	*/
	return tester
}

func (s *testServer) setPorts() {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	port := r.Intn(20000) + 10000

	s.port = fmt.Sprintf("%d", port)
	s.gwport = fmt.Sprintf("%d", port+1)
	s.uds = fmt.Sprintf("/tmp/osd-csi-ut-%d.sock", port)
}

func (s *testServer) MockDriver() *mockdriver.MockVolumeDriver {
	return s.m
}

func (s *testServer) MockCluster() *mockcluster.MockCluster {
	return s.c
}

func (s *testServer) Stop() {
	// Remove from registry
	volumedrivers.Remove("mock")

	// Shutdown servers
	s.conn.Close()
	s.server.Stop()
	s.sdk.Stop()

	// Check mocks
	s.mc.Finish()
}

func (s *testServer) Conn() *grpc.ClientConn {
	return s.conn
}

func (s *testServer) Server() grpcserver.Server {
	return s.server
}

func TestCSIServerStart(t *testing.T) {
	s := newTestServer(t)
	assert.True(t, s.Server().IsRunning())
	defer s.Stop()

	// Check if we can still talk to the server
	// after starting multiple times.
	err := s.Server().Start()
	assert.True(t, s.Server().IsRunning())
	assert.NotNil(t, err)
	err = s.Server().Start()
	assert.True(t, s.Server().IsRunning())
	assert.NotNil(t, err)
	err = s.Server().Start()
	assert.True(t, s.Server().IsRunning())
	assert.NotNil(t, err)

	// Make a call
	s.MockDriver().EXPECT().Name().Return("mock").Times(2)
	c := csi.NewIdentityClient(s.Conn())
	r, err := c.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	assert.Nil(t, err)

	// Verify
	name := r.GetName()
	version := r.GetVendorVersion()
	assert.Equal(t, name, "mock.openstorage.org")
	assert.Equal(t, version, csiDriverVersion)
}

func TestCSIServerStop(t *testing.T) {
	s := newTestServer(t)
	assert.True(t, s.Server().IsRunning())
	s.Stop()
	assert.False(t, s.Server().IsRunning())
}

func TestNewCSIServerBadParameters(t *testing.T) {
	tester := &testServer{}
	tester.setPorts()
	setupMockDriver(tester, t)
	s, err := NewOsdCsiServer(nil)
	assert.Nil(t, s)
	assert.NotNil(t, err)

	s, err = NewOsdCsiServer(&OsdCsiServerConfig{})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must be provided")

	s, err = NewOsdCsiServer(&OsdCsiServerConfig{
		Net:    "test",
		SdkUds: tester.uds,
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must be provided")

	s, err = NewOsdCsiServer(&OsdCsiServerConfig{
		Net:     "test",
		Address: "blah",
		SdkUds:  tester.uds,
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must be provided")

	s, err = NewOsdCsiServer(&OsdCsiServerConfig{
		Net:        "test",
		Address:    "blah",
		DriverName: "name",
		SdkUds:     tester.uds,
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Unable to get driver")

	s, err = NewOsdCsiServer(&OsdCsiServerConfig{
		Net:        "test",
		Address:    "blah",
		DriverName: "name",
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "SdkUds must be provided")

	// Add driver to registry
	mc := gomock.NewController(t)
	defer mc.Finish()
	m := mockdriver.NewMockVolumeDriver(mc)
	volumedrivers.Add("mock", func(map[string]string) (volume.VolumeDriver, error) {
		return m, nil
	})
	defer volumedrivers.Remove("mock")
	s, err = NewOsdCsiServer(&OsdCsiServerConfig{
		Net:        "test",
		Address:    "blah",
		DriverName: "mock",
		SdkUds:     tester.uds,
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Unable to setup server")
	os.Remove(tester.uds)
}

func TestAddEncryptionInfoToLabels(t *testing.T) {
	s := OsdCsiServer{}

	secrets := map[string]string{
		options.OptionsSecret:        "secret",
		options.OptionsSecretContext: "context",
		options.OptionsSecretKey:     "key",
	}
	labels := map[string]string{
		"test": "val",
	}
	labels = s.addEncryptionInfoToLabels(labels, secrets)

	assert.Equal(t, labels[options.OptionsSecret], "secret")
	assert.Equal(t, labels[options.OptionsSecretContext], "context")
	assert.Equal(t, labels[options.OptionsSecretKey], "key")
}
