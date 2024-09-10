package connect

import (
	"context"
	"net/url"

	// "encoding/json"
	// "encoding/base64"
	// "bytes"
	"fmt"

	"github.com/pion/webrtc/v3"
	// "io"
	// "net"
	// "net/http"
	// "time"
	// "errors"
	// "strings"
	// "github.com/golang/glog"
)

type BringYourApi struct {
	ctx    context.Context
	cancel context.CancelFunc

	clientStrategy *ClientStrategy

	apiUrl string

	byJwt string
}

func NewBringYourApi(clientStrategy *ClientStrategy, apiUrl string) *BringYourApi {
	return NewBringYourApiWithContext(context.Background(), clientStrategy, apiUrl)
}

func NewBringYourApiWithContext(ctx context.Context, clientStrategy *ClientStrategy, apiUrl string) *BringYourApi {
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
	self.byJwt = byJwt
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
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/auth/login", self.apiUrl),
		authLogin,
		self.byJwt,
		&AuthLoginResult{},
		callback,
	)
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
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/auth/login-with-password", self.apiUrl),
		authLoginWithPassword,
		self.byJwt,
		&AuthLoginWithPasswordResult{},
		callback,
	)
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
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/auth/verify", self.apiUrl),
		authVerify,
		self.byJwt,
		&AuthVerifyResult{},
		callback,
	)
}

type AuthPasswordResetCallback ApiCallback[*AuthPasswordResetResult]

type AuthPasswordResetArgs struct {
	UserAuth string `json:"user_auth"`
}

type AuthPasswordResetResult struct {
	UserAuth string `json:"user_auth"`
}

func (self *BringYourApi) AuthPasswordReset(authPasswordReset *AuthPasswordResetArgs, callback AuthPasswordResetCallback) {
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/auth/password-reset", self.apiUrl),
		authPasswordReset,
		self.byJwt,
		&AuthPasswordResetResult{},
		callback,
	)
}

type AuthVerifySendCallback ApiCallback[*AuthVerifySendResult]

type AuthVerifySendArgs struct {
	UserAuth string `json:"user_auth"`
}

type AuthVerifySendResult struct {
	UserAuth string `json:"user_auth"`
}

func (self *BringYourApi) AuthVerifySend(authVerifySend *AuthVerifySendArgs, callback AuthVerifySendCallback) {
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/auth/verify-send", self.apiUrl),
		authVerifySend,
		self.byJwt,
		&AuthVerifySendResult{},
		callback,
	)
}

type AuthNetworkClientCallback ApiCallback[*AuthNetworkClientResult]

type AuthNetworkClientArgs struct {
	// FIXME how to bring this back as optional with gomobile. Use a new type *OptionalId?
	// if omitted, a new client_id is created
	// ClientId string `json:"client_id",omitempty`
	Description string `json:"description"`
	DeviceSpec  string `json:"device_spec"`
}

type AuthNetworkClientResult struct {
	ByClientJwt string                  `json:"by_client_jwt,omitempty"`
	Error       *AuthNetworkClientError `json:"error,omitempty"`
}

type AuthNetworkClientError struct {
	// can be a hard limit or a rate limit
	ClientLimitExceeded bool   `json:"client_limit_exceeded"`
	Message             string `json:"message"`
}

func (self *BringYourApi) AuthNetworkClient(authNetworkClient *AuthNetworkClientArgs, callback AuthNetworkClientCallback) {
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/auth-client", self.apiUrl),
		authNetworkClient,
		self.byJwt,
		&AuthNetworkClientResult{},
		callback,
	)
}

func (self *BringYourApi) AuthNetworkClientSync(authNetworkClient *AuthNetworkClientArgs) (*AuthNetworkClientResult, error) {
	return HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/auth-client", self.apiUrl),
		authNetworkClient,
		self.byJwt,
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
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/remove-client", self.apiUrl),
		removeNetworkClient,
		self.byJwt,
		&RemoveNetworkClientResult{},
		callback,
	)
}

func (self *BringYourApi) RemoveNetworkClientSync(removeNetworkClient *RemoveNetworkClientArgs) (*RemoveNetworkClientResult, error) {
	return HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/remove-client", self.apiUrl),
		removeNetworkClient,
		self.byJwt,
		&RemoveNetworkClientResult{},
		NewNoopApiCallback[*RemoveNetworkClientResult](),
	)
}

type ProviderSpec struct {
	LocationId      *Id `json:"location_id,omitempty"`
	LocationGroupId *Id `json:"location_group_id,omitempty"`
	ClientId        *Id `json:"client_id,omitempty"`
}

type FindProviders2Callback ApiCallback[*FindProviders2Result]

type FindProviders2Args struct {
	Specs               []*ProviderSpec `json:"specs"`
	Count               int             `json:"count"`
	ExcludeClientIds    []Id            `json:"exclude_client_ids"`
	ExcludeDestinations [][]Id          `json:"exclude_destinations,omitempty"`
}

type FindProviders2Result struct {
	Providers []*FindProvidersProvider `json:"providers"`
}

type FindProvidersProvider struct {
	ClientId                Id        `json:"client_id"`
	EstimatedBytesPerSecond ByteCount `json:"estimated_bytes_per_second"`
	IntermediaryIds         []Id      `json:"intermediary_ids,omitempty"`
}

func (self *BringYourApi) FindProviders2(findProviders2 *FindProviders2Args, callback FindProviders2Callback) {
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/find-providers2", self.apiUrl),
		findProviders2,
		self.byJwt,
		&FindProviders2Result{},
		callback,
	)
}

func (self *BringYourApi) FindProviders2Sync(findProviders2 *FindProviders2Args) (*FindProviders2Result, error) {
	return HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/network/find-providers2", self.apiUrl),
		findProviders2,
		self.byJwt,
		&FindProviders2Result{},
		NewNoopApiCallback[*FindProviders2Result](),
	)
}

type ConnectControlCallback ApiCallback[*ConnectControlResult]

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
	go HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/connect/control", self.apiUrl),
		connectControl,
		self.byJwt,
		&ConnectControlResult{},
		callback,
	)
}

func (self *BringYourApi) PutPeerToPeerOfferSDPSync(ctx context.Context, handshakeID string, sdp webrtc.SessionDescription) (err error) {
	var res any
	_, err = HttpPutWithStrategy(
		ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/peer-to-peer/handshake/%s/offer/sdp", self.apiUrl, handshakeID),
		sdp,
		self.byJwt,
		res,
		NewNoopApiCallback[any](),
	)
	return err
}

func (self *BringYourApi) PollPeerToPeerOfferSDPSync(ctx context.Context, handshakeID string) (sdp webrtc.SessionDescription, err error) {
	for {
		_, err = HttpPollWithStrategy(
			ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/peer-to-peer/handshake/%s/offer/sdp", self.apiUrl, handshakeID),
			self.byJwt,
			&sdp,
			NewNoopApiCallback[*webrtc.SessionDescription](),
		)

		// 204 signals that the server is still waiting for the state
		if err == ErrPollTimeout {
			continue
		}

		return sdp, err

	}

}

func (self *BringYourApi) PutPeerToPeerAnswerSDPSync(ctx context.Context, handshakeID string, sdp webrtc.SessionDescription) (err error) {
	var res any
	_, err = HttpPutWithStrategy(
		ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/peer-to-peer/handshake/%s/answer/sdp", self.apiUrl, handshakeID),
		sdp,
		self.byJwt,
		res,
		NewNoopApiCallback[any](),
	)
	return err
}

func (self *BringYourApi) PollPeerToPeerAnswerSDPSync(ctx context.Context, handshakeID string) (sdp webrtc.SessionDescription, err error) {
	for {
		_, err = HttpPollWithStrategy(
			ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/peer-to-peer/handshake/%s/answer/sdp", self.apiUrl, handshakeID),
			self.byJwt,
			&sdp,
			NewNoopApiCallback[*webrtc.SessionDescription](),
		)

		// 204 signals that the server is still waiting for the state
		if err == ErrPollTimeout {
			continue
		}

		return sdp, err

	}

}

func (self *BringYourApi) PostPeerToPeerOfferPeerCandidateSync(ctx context.Context, handshakeID string, peerCandidate webrtc.ICECandidate) (err error) {
	var res any
	_, err = HttpPostWithStrategy(
		ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/peer-to-peer/handshake/%s/offer/peer_candidates", self.apiUrl, handshakeID),
		peerCandidate,
		self.byJwt,
		res,
		NewNoopApiCallback[any](),
	)
	return err

}

func (self *BringYourApi) PollPeerToPeerOfferCandidatesSync(ctx context.Context, handshakeID string, from int) (candidates []webrtc.ICECandidate, err error) {

	u, err := url.Parse(self.apiUrl)

	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	u = u.JoinPath("peer-to-peer", "handshake", handshakeID, "offer", "peer_candidates")
	q := u.Query()
	q.Set("from", fmt.Sprintf("%d", from))
	u.RawQuery = q.Encode()

	for {
		_, err = HttpPollWithStrategy(
			ctx,
			self.clientStrategy,
			u.String(),
			self.byJwt,
			&candidates,
			NewNoopApiCallback[*[]webrtc.ICECandidate](),
		)

		if err == ErrPollTimeout {
			// 204 signals that the server is still waiting for the state
			continue
		}

		return candidates, err

	}

}

func (self *BringYourApi) PostPeerToPeerAnswerPeerCandidateSync(ctx context.Context, handshakeID string, peerCandidate webrtc.ICECandidate) (err error) {
	var res any
	_, err = HttpPostWithStrategy(
		ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/peer-to-peer/handshake/%s/answer/peer_candidates", self.apiUrl, handshakeID),
		peerCandidate,
		self.byJwt,
		res,
		NewNoopApiCallback[any](),
	)
	return err

}

func (self *BringYourApi) PollPeerToPeerAnswerCandidatesSync(ctx context.Context, handshakeID string, from int) (candidates []webrtc.ICECandidate, err error) {

	u, err := url.Parse(self.apiUrl)

	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	u = u.JoinPath("peer-to-peer", "handshake", handshakeID, "answer", "peer_candidates")
	q := u.Query()
	q.Set("from", fmt.Sprintf("%d", from))
	u.RawQuery = q.Encode()

	for {
		_, err = HttpPollWithStrategy(
			ctx,
			self.clientStrategy,
			u.String(),
			self.byJwt,
			&candidates,
			NewNoopApiCallback[*[]webrtc.ICECandidate](),
		)

		if err == ErrPollTimeout {
			// 204 signals that the server is still waiting for the state
			continue
		}

		return candidates, err

	}

}
