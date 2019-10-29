package server

import (
	"context"
	"github.com/libopenstorage/openstorage/pkg/auth"
	"github.com/libopenstorage/openstorage/pkg/auth/systemtoken"
	"net/http"
	"reflect"
	"testing"
)

type mockAuthenticator struct {
	claims *auth.Claims
}

func (a *mockAuthenticator) AuthenticateToken(context context.Context, token string) (*auth.Claims, error) {
	return a.claims, nil
}

func (a *mockAuthenticator) Username(claims *auth.Claims) string {
	return a.claims.Name
}

type noTokenGenerator struct{}

func (n *noTokenGenerator) GetToken(opts *auth.Options) (string, error) {
	return "", nil
}

// Issuer returns the token issuer for this generator necessary
// for registering the authenticator in the SDK.
func (n *noTokenGenerator) Issuer() string {
	return ""
}

// GetAuthenticator returns an authenticator for this issuer used by the SDK
func (n *noTokenGenerator) GetAuthenticator() (auth.Authenticator, error) {
	return nil, nil
}

func TestNewSecurityMiddleware(t *testing.T) {
	table := []struct {
		description    string
		isAuthEnabled  bool
		authenticators map[string]auth.Authenticator
	}{
		{
			description:   "auth enabled authenticator not null",
			isAuthEnabled: true,
			authenticators: map[string]auth.Authenticator{
				"no-authenticators": &mockAuthenticator{},
			},
		}, {
			description:    "auth enabled authenticator is null",
			isAuthEnabled:  true,
			authenticators: nil,
		},
		{
			description:   "auth not enabled authenticator is not null",
			isAuthEnabled: false,
			authenticators: map[string]auth.Authenticator{
				"no-authenticators": &mockAuthenticator{},
			},
		}, {
			description:    "auth is not enabled authenticator is null",
			authenticators: nil,
			isAuthEnabled:  false,
		},
	}

	var handlerFunc http.HandlerFunc

	handlerFunc = func(w http.ResponseWriter, r *http.Request) {}

	for _, testCase := range table {
		t.Log(testCase.description)

		var (
			stm auth.TokenGenerator
			err error
		)

		if testCase.isAuthEnabled {
			stm, err = systemtoken.NewManager(&systemtoken.Config{
				ClusterId:    "cluster-id",
				NodeId:       "node-id",
				SharedSecret: "shared-secret",
			})

			if err != nil {
				t.Errorf("Failed to create system token manager: %v\n", err)
				continue
			}
		} else {
			stm = &noTokenGenerator{}
		}

		auth.InitSystemTokenManager(stm)

		decorator := newSecurityMiddleware(testCase.authenticators)

		if testCase.isAuthEnabled && testCase.authenticators != nil {
			if reflect.ValueOf(decorator(handlerFunc)) == reflect.ValueOf(handlerFunc) {
				t.Errorf("func must be decorated")
			}
		} else {
			if reflect.ValueOf(decorator(handlerFunc)) != reflect.ValueOf(handlerFunc) {
				t.Errorf("func must not be decorated")
			}
		}
	}
}
