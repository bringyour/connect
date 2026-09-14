package connect

import (
	"context"
	// "encoding/json"
	// "encoding/base64"
	// "bytes"
	"fmt"
	// "io"
	// "net"
	// "net/http"
	// "time"
	// "errors"
	// "strings"
	// "github.com/urnetwork/glog/v2026"
	"sync"
)

// FIXME rename to Api
type BringYourApi struct {
	ctx    context.Context
	cancel context.CancelFunc

	clientStrategy *ClientStrategy

	apiUrl string

	mutex sync.Mutex
	byJwt string
}

// func NewBringYourApi(clientStrategy *ClientStrategy, apiUrl string) *BringYourApi {
// 	return NewBringYourApiWithContext(context.Background(), clientStrategy, apiUrl)
// }

func NewBringYourApi(ctx context.Context, clientStrategy *ClientStrategy, apiUrl string) *BringYourApi {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &BringYourApi{
		ctx:            cancelCtx,
		cancel:         cancel,
		clientStrategy: clientStrategy,
		apiUrl:         apiUrl,
	}
}

// this gets attached to api calls that need it
func (self *BringYourApi) SetByJwt(byJwt string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.byJwt = byJwt
}

func (self *BringYourApi) ByJwt() string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.byJwt
}

func (self *BringYourApi) Close() {
	self.cancel()
}

type AuthLoginCallback ApiCallback[*AuthLoginResult]

// `model.AuthLoginArgs`
type AuthLoginArgs struct {
	UserAuth    string `json:"user_auth,omitempty"`
	AuthJwtType string `json:"auth_jwt_type,omitempty"`
	AuthJwt     string `json:"auth_jwt,omitempty"`
}

// `model.AuthLoginResult`
type AuthLoginResult struct {
	UserName    string                  `json:"user_name,omitempty"`
	UserAuth    string                  `json:"user_auth,omitempty"`
	AuthAllowed []string                `json:"auth_allowed,omitempty"`
	Error       *AuthLoginResultError   `json:"error,omitempty"`
	Network     *AuthLoginResultNetwork `json:"network,omitempty"`
}

// `model.AuthLoginResultError`
type AuthLoginResultError struct {
	SuggestedUserAuth string `json:"suggested_user_auth,omitempty"`
	Message           string `json:"message"`
}

// `model.AuthLoginResultNetwork`
type AuthLoginResultNetwork struct {
	ByJwt string `json:"by_jwt"`
}

func (self *BringYourApi) AuthLogin(authLogin *AuthLoginArgs, callback AuthLoginCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/auth/login", self.apiUrl),
			authLogin,
			self.ByJwt(),
			&AuthLoginResult{},
			callback,
		)
	})
}

type AuthLoginWithPasswordCallback ApiCallback[*AuthLoginWithPasswordResult]

type AuthLoginWithPasswordArgs struct {
	UserAuth string `json:"user_auth"`
	Password string `json:"password"`
}

type AuthLoginWithPasswordResult struct {
	VerificationRequired *AuthLoginWithPasswordResultVerification `json:"verification_required,omitempty"`
	Network              *AuthLoginWithPasswordResultNetwork      `json:"network,omitempty"`
	Error                *AuthLoginWithPasswordResultError        `json:"error,omitempty"`
}

type AuthLoginWithPasswordResultVerification struct {
	UserAuth string `json:"user_auth"`
}

type AuthLoginWithPasswordResultNetwork struct {
	ByJwt       string `json:"by_jwt,omitempty"`
	NetworkName string `json:"name,omitempty"`
}

type AuthLoginWithPasswordResultError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) AuthLoginWithPassword(authLoginWithPassword *AuthLoginWithPasswordArgs, callback AuthLoginWithPasswordCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/auth/login-with-password", self.apiUrl),
			authLoginWithPassword,
			self.ByJwt(),
			&AuthLoginWithPasswordResult{},
			callback,
		)
	})
}

type AuthVerifyCallback ApiCallback[*AuthVerifyResult]

type AuthVerifyArgs struct {
	UserAuth   string `json:"user_auth"`
	VerifyCode string `json:"verify_code"`
}

type AuthVerifyResult struct {
	Network *AuthVerifyResultNetwork `json:"network,omitempty"`
	Error   *AuthVerifyResultError   `json:"error,omitempty"`
}

type AuthVerifyResultNetwork struct {
	ByJwt string `json:"by_jwt"`
}

type AuthVerifyResultError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) AuthVerify(authVerify *AuthVerifyArgs, callback AuthVerifyCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/auth/verify", self.apiUrl),
			authVerify,
			self.ByJwt(),
			&AuthVerifyResult{},
			callback,
		)
	})
}

type AuthPasswordResetCallback ApiCallback[*AuthPasswordResetResult]

type AuthPasswordResetArgs struct {
	UserAuth string `json:"user_auth"`
}

type AuthPasswordResetResult struct {
	UserAuth string `json:"user_auth"`
}

func (self *BringYourApi) AuthPasswordReset(authPasswordReset *AuthPasswordResetArgs, callback AuthPasswordResetCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/auth/password-reset", self.apiUrl),
			authPasswordReset,
			self.ByJwt(),
			&AuthPasswordResetResult{},
			callback,
		)
	})
}

type AuthVerifySendCallback ApiCallback[*AuthVerifySendResult]

type AuthVerifySendArgs struct {
	UserAuth string `json:"user_auth"`
}

type AuthVerifySendResult struct {
	UserAuth string `json:"user_auth"`
}

func (self *BringYourApi) AuthVerifySend(authVerifySend *AuthVerifySendArgs, callback AuthVerifySendCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/auth/verify-send", self.apiUrl),
			authVerifySend,
			self.ByJwt(),
			&AuthVerifySendResult{},
			callback,
		)
	})
}

type AuthNetworkClientCallback ApiCallback[*AuthNetworkClientResult]

type AuthNetworkClientArgs struct {
	ClientId       *Id    `json:"client_id,omitempty"`
	SourceClientId *Id    `json:"source_client_id,omitempty"`
	Description    string `json:"description"`
	DeviceSpec     string `json:"device_spec"`
}

type AuthNetworkClientResult struct {
	ByClientJwt string                  `json:"by_client_jwt,omitempty"`
	Error       *AuthNetworkClientError `json:"error,omitempty"`
}

type AuthNetworkClientError struct {
	// can be a hard limit or a rate limit
	ClientLimitExceeded bool `json:"client_limit_exceeded"`
	// the network is at its PLAN's limit for concurrent connected clients, rather
	// than a hard cap. The client should prompt the user to upgrade (or, for an
	// agent, pay inline over x402 -- the same request answers 402 with payment
	// terms). Distinct from ClientLimitExceeded, which no upgrade can lift.
	UpgradeRequired bool   `json:"upgrade_required,omitempty"`
	Message         string `json:"message"`
}

func (self *BringYourApi) AuthNetworkClient(authNetworkClient *AuthNetworkClientArgs, callback AuthNetworkClientCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/network/auth-client", self.apiUrl),
			authNetworkClient,
			self.ByJwt(),
			&AuthNetworkClientResult{},
			callback,
		)
	})
}

func (self *BringYourApi) AuthNetworkClientSync(authNetworkClient *AuthNetworkClientArgs) (*AuthNetworkClientResult, error) {
	return self.AuthNetworkClientSyncWithCtx(self.ctx, authNetworkClient)
}

// AuthNetworkClientSyncWithCtx is the caller-bounded form used by multi-client
// maintenance. The ordinary API context owns the generator lifetime; this
// narrower context prevents one auth request from parking destination
// enumeration indefinitely.
func (self *BringYourApi) AuthNetworkClientSyncWithCtx(ctx context.Context, authNetworkClient *AuthNetworkClientArgs) (*AuthNetworkClientResult, error) {
	return HttpPostWithStrategy(
		ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/auth-client", self.apiUrl),
		authNetworkClient,
		self.ByJwt(),
		&AuthNetworkClientResult{},
		NewNoopApiCallback[*AuthNetworkClientResult](),
	)
}

type RemoveNetworkClientCallback ApiCallback[*RemoveNetworkClientResult]

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
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/network/remove-client", self.apiUrl),
			removeNetworkClient,
			self.ByJwt(),
			&RemoveNetworkClientResult{},
			callback,
		)
	})
}

func (self *BringYourApi) RemoveNetworkClientSync(removeNetworkClient *RemoveNetworkClientArgs) (*RemoveNetworkClientResult, error) {
	return HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/remove-client", self.apiUrl),
		removeNetworkClient,
		self.ByJwt(),
		&RemoveNetworkClientResult{},
		NewNoopApiCallback[*RemoveNetworkClientResult](),
	)
}

type ProviderSpec struct {
	LocationId      *Id  `json:"location_id,omitempty"`
	LocationGroupId *Id  `json:"location_group_id,omitempty"`
	ClientId        *Id  `json:"client_id,omitempty"`
	BestAvailable   bool `json:"best_available,omitempty"`
}

// LocationCoordinates is a wgs84 point. Zero is a valid coordinate — absence
// is expressed by a nil *LocationCoordinates, never by (0, 0).
type LocationCoordinates struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// ProviderLocation is the location of a provider client as reported by
// find-providers2. Immutable after construction: it is shared by value-copied
// `DestinationStats` and monitor `ProviderEvent`s without locking.
type ProviderLocation struct {
	Country           string               `json:"country,omitempty"`
	CountryCode       string               `json:"country_code,omitempty"`
	Region            string               `json:"region,omitempty"`
	City              string               `json:"city,omitempty"`
	CountryLocationId *Id                  `json:"country_location_id,omitempty"`
	RegionLocationId  *Id                  `json:"region_location_id,omitempty"`
	CityLocationId    *Id                  `json:"city_location_id,omitempty"`
	RegionCoordinates *LocationCoordinates `json:"region_coordinates,omitempty"`
	CityCoordinates   *LocationCoordinates `json:"city_coordinates,omitempty"`
}

type FindProviders2Callback ApiCallback[*FindProviders2Result]

type FindProviders2Args struct {
	Specs               []*ProviderSpec `json:"specs"`
	Count               int             `json:"count"`
	ExcludeClientIds    []Id            `json:"exclude_client_ids"`
	ExcludeDestinations [][]Id          `json:"exclude_destinations,omitempty"`
	RankMode            string          `json:"rank_mode"`
	ForceMinimum        bool            `json:"force_minimum,omitempty"`
	// IpFamily filters providers by proven address family. Empty means
	// v4-capable, which is every provider an older server knows, so an older
	// client keeps today's behavior. See ip_family.go.
	IpFamily IpFamilyFilter `json:"ip_family,omitempty"`
}

type FindProviders2Result struct {
	Providers []*FindProvidersProvider `json:"providers"`
}

type FindProvidersProvider struct {
	ClientId                   Id        `json:"client_id"`
	EstimatedBytesPerSecond    ByteCount `json:"estimated_bytes_per_second"`
	HasEstimatedBytesPerSecond bool      `json:"has_estimated_bytes_per_second"`
	Tier                       int       `json:"tier"`
	IntermediaryIds            []Id      `json:"intermediary_ids,omitempty"`
	// NetworkOnly is true when this provider is available through the caller's
	// own network relationship rather than as a public exit.
	NetworkOnly bool `json:"network_only,omitempty"`
	// ReputationFailedNames comes from low-rate external probes. Values are
	// opaque domain/vendor labels; the tunnel does not infer them from TLS.
	ReputationFailedNames string `json:"reputation_failed_names,omitempty"`
	// Location is the provider's location. nil when the server does not know
	// it (or an older server).
	Location *ProviderLocation `json:"location,omitempty"`
	// IpFamily is the provider's proven address-family category. Empty from
	// an older server, which the client treats as v4-only (legacy).
	IpFamily IpFamily `json:"ip_family,omitempty"`
}

func (self *BringYourApi) FindProviders2(findProviders2 *FindProviders2Args, callback FindProviders2Callback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/network/find-providers2", self.apiUrl),
			findProviders2,
			self.ByJwt(),
			&FindProviders2Result{},
			callback,
		)
	})
}

func (self *BringYourApi) FindProviders2Sync(findProviders2 *FindProviders2Args) (*FindProviders2Result, error) {
	return self.FindProviders2SyncWithCtx(self.ctx, findProviders2)
}

// FindProviders2SyncWithCtx is the caller-bounded form used by multi-client
// maintenance, so a discovery request cannot own the only enumerator forever.
func (self *BringYourApi) FindProviders2SyncWithCtx(ctx context.Context, findProviders2 *FindProviders2Args) (*FindProviders2Result, error) {
	return HttpPostWithStrategy(
		ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/find-providers2", self.apiUrl),
		findProviders2,
		self.ByJwt(),
		&FindProviders2Result{},
		NewNoopApiCallback[*FindProviders2Result](),
	)
}

type ConnectControlCallback ApiCallback[*ConnectControlResult]

type ConnectControlArgs struct {
	Pack string `json:"pack"`
}

type ConnectControlResult struct {
	Pack  string               `json:"pack"`
	Error *ConnectControlError `json:"error"`
}

type ConnectControlError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) ConnectControl(connectControl *ConnectControlArgs, callback ConnectControlCallback) {
	self.ConnectControlWithCtx(self.ctx, connectControl, callback)
}

// ConnectControlWithCtx sends control with a caller-chosen context, so control
// messages can outlive the api context (e.g. closing pending contracts after
// the client context is closed). Each request is bounded by the client
// strategy's `RequestTimeout` regardless of the context passed.
func (self *BringYourApi) ConnectControlWithCtx(ctx context.Context, connectControl *ConnectControlArgs, callback ConnectControlCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/connect/control", self.apiUrl),
			connectControl,
			self.ByJwt(),
			&ConnectControlResult{},
			callback,
		)
	})
}

type GetClientKeyCallback ApiCallback[*GetClientKeyResult]

type GetClientKeyArgs struct {
	ClientId Id `json:"client_id"`
}

type GetClientKeyResult struct {
	PublicKey []byte `json:"public_key"`
}

// GetClientKey fetches a peer client's long-lived public identity key
// from the unauthenticated `/key/<client_id>` API. Used as the
// out-of-band cross-check against the
// `Contract.destination_client_public_key` value the platform
// attaches to contracts — if the two disagree, the platform may be
// substituting keys to mount a MITM attack.
//
// The route is unauthenticated by design; no `byJwt` is sent.
func (self *BringYourApi) GetClientKey(args *GetClientKeyArgs, callback GetClientKeyCallback) {
	go HandleError(func() {
		HttpGetWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/key/%s", self.apiUrl, args.ClientId),
			"",
			&GetClientKeyResult{},
			callback,
		)
	})
}

func (self *BringYourApi) GetClientKeySync(args *GetClientKeyArgs) (*GetClientKeyResult, error) {
	return HttpGetWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/key/%s", self.apiUrl, args.ClientId),
		"",
		&GetClientKeyResult{},
		NewNoopApiCallback[*GetClientKeyResult](),
	)
}

type AuthCodeLoginCallback ApiCallback[*AuthCodeLoginResult]

type AuthCodeLoginArgs struct {
	AuthCode string `json:"auth_code"`
}

type AuthCodeLoginResult struct {
	ByJwt string              `json:"by_jwt,omitempty"`
	Error *AuthCodeLoginError `json:"error,omitempty"`
}

type AuthCodeLoginError struct {
	Message string `json:"message"`
}

func (self *BringYourApi) AuthCodeLogin(authCodeLogin *AuthCodeLoginArgs, callback AuthCodeLoginCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/auth/code-login", self.apiUrl),
			authCodeLogin,
			self.ByJwt(),
			&AuthCodeLoginResult{},
			callback,
		)
	})
}

func (self *BringYourApi) AuthCodeLoginSync(authCodeLogin *AuthCodeLoginArgs) (*AuthCodeLoginResult, error) {
	return HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/auth/code-login", self.apiUrl),
		authCodeLogin,
		self.ByJwt(),
		&AuthCodeLoginResult{},
		NewNoopApiCallback[*AuthCodeLoginResult](),
	)
}
