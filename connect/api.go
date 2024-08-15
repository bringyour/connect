package connect

import (
	"context"
	"encoding/json"
	// "encoding/base64"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
	"errors"
	"strings"

	// "github.com/golang/glog"
)


const defaultHttpTimeout = 60 * time.Second
const defaultHttpConnectTimeout = 5 * time.Second
const defaultHttpTlsTimeout = 5 * time.Second


func defaultClient() *http.Client {
	// see https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := &net.Dialer{
    	Timeout: defaultHttpConnectTimeout,
  	}
	transport := &http.Transport{
	  	DialContext: dialer.DialContext,
	  	TLSHandshakeTimeout: defaultHttpTlsTimeout,
	}
	return &http.Client{
		Transport: transport,
		Timeout: defaultHttpTimeout,
	}
}


type apiCallback[R any] interface {
	Result(result R, err error)
}


// for internal use
type simpleApiCallback[R any] struct {
	callback func(result R, err error)
}

func NewApiCallback[R any](callback func(result R, err error)) apiCallback[R] {
	return &simpleApiCallback[R]{
		callback: callback,
	}
}

func NewNoopApiCallback[R any]() apiCallback[R] {
	return &simpleApiCallback[R]{
		callback: func(result R, err error){},
	}
}

func (self *simpleApiCallback[R]) Result(result R, err error) {
	self.callback(result, err)
}


type ApiCallbackResult[R any] struct {
	Result R
	Error error
}


func NewBlockingApiCallback[R any]() (apiCallback[R], chan ApiCallbackResult[R]) {
	c := make(chan ApiCallbackResult[R])
	apiCallback := NewApiCallback[R](func(result R, err error) {
		c <- ApiCallbackResult[R]{
			Result: result,
			Error: err,
		}
	})
	return apiCallback, c
}


type BringYourApi struct {
	ctx context.Context
	cancel context.CancelFunc

	apiUrl string

	byJwt string
}

// TODO manage extenders

func NewBringYourApi(apiUrl string) *BringYourApi {
	return NewBringYourApiWithContext(context.Background(), apiUrl)
}

func NewBringYourApiWithContext(ctx context.Context, apiUrl string) *BringYourApi {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &BringYourApi{
		ctx: cancelCtx,
		cancel: cancel,
		apiUrl: apiUrl,
	}
}

// this gets attached to api calls that need it
func (self *BringYourApi) SetByJwt(byJwt string) {
	self.byJwt = byJwt
}


type AuthLoginCallback apiCallback[*AuthLoginResult]

// `model.AuthLoginArgs`
type AuthLoginArgs struct {
	UserAuth string `json:"user_auth,omitempty"`
	AuthJwtType string `json:"auth_jwt_type,omitempty"`
	AuthJwt string `json:"auth_jwt,omitempty"`
}

// `model.AuthLoginResult`
type AuthLoginResult struct {
	UserName string `json:"user_name,omitempty"`
	UserAuth string `json:"user_auth,omitempty"`
	AuthAllowed []string `json:"auth_allowed,omitempty"`
	Error *AuthLoginResultError `json:"error,omitempty"`
	Network *AuthLoginResultNetwork `json:"network,omitempty"`
}

// `model.AuthLoginResultError`
type AuthLoginResultError struct {
	SuggestedUserAuth string `json:"suggested_user_auth,omitempty"`
	Message string `json:"message"`
}

// `model.AuthLoginResultNetwork`
type AuthLoginResultNetwork struct {
	ByJwt string `json:"by_jwt"`
}

func (self *BringYourApi) AuthLogin(authLogin *AuthLoginArgs, callback AuthLoginCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/auth/login", self.apiUrl),
		authLogin,
		self.byJwt,
		&AuthLoginResult{},
		callback,
	)
}


type AuthLoginWithPasswordCallback apiCallback[*AuthLoginWithPasswordResult]

type AuthLoginWithPasswordArgs struct {
	UserAuth string `json:"user_auth"`
	Password string `json:"password"`
}

type AuthLoginWithPasswordResult struct {
	VerificationRequired *AuthLoginWithPasswordResultVerification `json:"verification_required,omitempty"`
	Network *AuthLoginWithPasswordResultNetwork `json:"network,omitempty"`
	Error *AuthLoginWithPasswordResultError `json:"error,omitempty"`
}

type AuthLoginWithPasswordResultVerification struct {
	UserAuth string `json:"user_auth"`
}

type AuthLoginWithPasswordResultNetwork struct {
	ByJwt string `json:"by_jwt,omitempty"`
	NetworkName string `json:"name,omitempty"`
}

type AuthLoginWithPasswordResultError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) AuthLoginWithPassword(authLoginWithPassword *AuthLoginWithPasswordArgs, callback AuthLoginWithPasswordCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/auth/login-with-password", self.apiUrl),
		authLoginWithPassword,
		self.byJwt,
		&AuthLoginWithPasswordResult{},
		callback,
	)
}


type AuthVerifyCallback apiCallback[*AuthVerifyResult]

type AuthVerifyArgs struct {
	UserAuth string `json:"user_auth"`
	VerifyCode string `json:"verify_code"`
}

type AuthVerifyResult struct {
	Network *AuthVerifyResultNetwork `json:"network,omitempty"`
	Error *AuthVerifyResultError `json:"error,omitempty"`
}

type AuthVerifyResultNetwork struct {
	ByJwt string `json:"by_jwt"`
}

type AuthVerifyResultError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) AuthVerify(authVerify *AuthVerifyArgs, callback AuthVerifyCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/auth/verify", self.apiUrl),
		authVerify,
		self.byJwt,
		&AuthVerifyResult{},
		callback,
	)
}


type AuthPasswordResetCallback apiCallback[*AuthPasswordResetResult]

type AuthPasswordResetArgs struct {
    UserAuth string `json:"user_auth"`
}

type AuthPasswordResetResult struct {
    UserAuth string `json:"user_auth"`
}

func (self *BringYourApi) AuthPasswordReset(authPasswordReset *AuthPasswordResetArgs, callback AuthPasswordResetCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/auth/password-reset", self.apiUrl),
		authPasswordReset,
		self.byJwt,
		&AuthPasswordResetResult{},
		callback,
	)
}


type AuthVerifySendCallback apiCallback[*AuthVerifySendResult]

type AuthVerifySendArgs struct {
    UserAuth string `json:"user_auth"`
}

type AuthVerifySendResult struct {
    UserAuth string `json:"user_auth"`
}

func (self *BringYourApi) AuthVerifySend(authVerifySend *AuthVerifySendArgs, callback AuthVerifySendCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/auth/verify-send", self.apiUrl),
		authVerifySend,
		self.byJwt,
		&AuthVerifySendResult{},
		callback,
	)
}



type AuthNetworkClientCallback apiCallback[*AuthNetworkClientResult]

type AuthNetworkClientArgs struct {
	// FIXME how to bring this back as optional with gomobile. Use a new type *OptionalId?
	// if omitted, a new client_id is created
	// ClientId string `json:"client_id",omitempty`
	Description string `json:"description"`
	DeviceSpec string `json:"device_spec"`
}

type AuthNetworkClientResult struct {
	ByClientJwt string `json:"by_client_jwt,omitempty"`
	Error *AuthNetworkClientError `json:"error,omitempty"`
}

type AuthNetworkClientError struct {
	// can be a hard limit or a rate limit
	ClientLimitExceeded bool `json:"client_limit_exceeded"` 
	Message string `json:"message"`
}

func (self *BringYourApi) AuthNetworkClient(authNetworkClient *AuthNetworkClientArgs, callback AuthNetworkClientCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/network/auth-client", self.apiUrl),
		authNetworkClient,
		self.byJwt,
		&AuthNetworkClientResult{},
		callback,
	)
}

func (self *BringYourApi) AuthNetworkClientSync(authNetworkClient *AuthNetworkClientArgs) (*AuthNetworkClientResult, error) {
	return post(
		self.ctx,
		fmt.Sprintf("%s/network/auth-client", self.apiUrl),
		authNetworkClient,
		self.byJwt,
		&AuthNetworkClientResult{},
		NewNoopApiCallback[*AuthNetworkClientResult](),
	)
}


type RemoveNetworkClientCallback apiCallback[*RemoveNetworkClientResult]

type RemoveNetworkClientArgs struct {
	ClientId Id `json:"client_id"`
}

type RemoveNetworkClientResult struct {
	Error *RemoveNetworkClientError `json:"error"`
}

type RemoveNetworkClientError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) RemoveNetworkClient(removeNetworkClient *RemoveNetworkClientArgs, callback RemoveNetworkClientCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/network/remove-client", self.apiUrl),
		removeNetworkClient,
		self.byJwt,
		&RemoveNetworkClientResult{},
		callback,
	)
}

func (self *BringYourApi) RemoveNetworkClientSync(removeNetworkClient *RemoveNetworkClientArgs) (*RemoveNetworkClientResult, error) {
	return post(
		self.ctx,
		fmt.Sprintf("%s/network/remove-client", self.apiUrl),
		removeNetworkClient,
		self.byJwt,
		&RemoveNetworkClientResult{},
		NewNoopApiCallback[*RemoveNetworkClientResult](),
	)
}


type ProviderSpec struct {
    LocationId *Id `json:"location_id,omitempty"`
    LocationGroupId *Id `json:"location_group_id,omitempty"`
    ClientId *Id `json:"client_id,omitempty"`
}

type FindProviders2Callback apiCallback[*FindProviders2Result]

type FindProviders2Args struct {
	Specs []*ProviderSpec `json:"specs"`
	Count int `json:"count"`
	ExcludeClientIds []Id `json:"exclude_client_ids"`
	Exclude [][]Id `json:"exclude,omitempty"`
}

type FindProviders2Result struct {
	Providers []*FindProvidersProvider `json:"providers"`
}

type FindProvidersProvider struct {
	ClientId Id `json:"client_id"`
	EstimatedBytesPerSecond ByteCount `json:"estimated_bytes_per_second"`
	IntermediaryIds []Id `json:"intermediary_ids,omitempty"`
}

func (self *BringYourApi) FindProviders2(findProviders2 *FindProviders2Args, callback FindProviders2Callback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/network/find-providers2", self.apiUrl),
		findProviders2,
		self.byJwt,
		&FindProviders2Result{},
		callback,
	)
}

func (self *BringYourApi) FindProviders2Sync(findProviders2 *FindProviders2Args) (*FindProviders2Result, error) {
	return post(
		self.ctx,
		fmt.Sprintf("%s/network/find-providers2", self.apiUrl),
		findProviders2,
		self.byJwt,
		&FindProviders2Result{},
		NewNoopApiCallback[*FindProviders2Result](),
	)
}



type ConnectControlCallback apiCallback[*ConnectControlResult]

type ConnectControlArgs struct {
	Pack string `json:"pack"`
}

type ConnectControlResult struct {
	Pack string `json:"pack"`
}

type ConnectControlError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) ConnectControl(connectControl *ConnectControlArgs, callback ConnectControlCallback) {
	go post(
		self.ctx,
		fmt.Sprintf("%s/connect/control", self.apiUrl),
		connectControl,
		self.byJwt,
		&ConnectControlResult{},
		callback,
	)
}


func post[R any](ctx context.Context, url string, args any, byJwt string, result R, callback apiCallback[R]) (R, error) {
	var requestBodyBytes []byte
	if args == nil {
		requestBodyBytes = make([]byte, 0)
	} else {
		var err error
		requestBodyBytes, err = json.Marshal(args)
		if err != nil {
			var empty R
			callback.Result(empty, err)
			return empty, err
		}
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(requestBodyBytes))
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	req.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		req.Header.Add("Authorization", auth)
	}

	client := defaultClient()
	r, err := client.Do(req)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}
	defer r.Body.Close()

	responseBodyBytes, err := io.ReadAll(r.Body)

	if http.StatusOK != r.StatusCode {
		// the response body is the error message
		errorMessage := strings.TrimSpace(string(responseBodyBytes))
		err = errors.New(errorMessage)
		callback.Result(result, err)
		return result, err
	}

	if err != nil {
		callback.Result(result, err)
		return result, err
	}

	err = json.Unmarshal(responseBodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}


func get[R any](ctx context.Context, url string, byJwt string, result R, callback apiCallback[R]) (R, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	req.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		req.Header.Add("Authorization", auth)
	}

	client := defaultClient()
	r, err := client.Do(req)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	responseBodyBytes, err := io.ReadAll(r.Body)
	r.Body.Close()

	err = json.Unmarshal(responseBodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}


// TODO post with extender

