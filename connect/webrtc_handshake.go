package connect

import (
	"context"
	"fmt"

	"github.com/pion/webrtc/v3"
)

func NewWebRTCOfferHandshake(api *BringYourApi, handshakeID string) *WebRTCOfferHandshake {
	return &WebRTCOfferHandshake{
		api:         api,
		handshakeID: handshakeID,
	}
}

type WebRTCOfferHandshake struct {
	api                *BringYourApi
	handshakeID        string
	candidatesReceived int
}

func (o *WebRTCOfferHandshake) OfferSDP(ctx context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	err := o.api.PutPeerToPeerOfferSDPSync(
		ctx,
		o.handshakeID,
		offer,
	)

	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("failed to send offer SDP: %w", err)
	}

	answer, err := o.api.PollPeerToPeerAnswerSDPSync(
		ctx,
		o.handshakeID,
	)

	if err != nil {
		return webrtc.SessionDescription{}, fmt.Errorf("failed to get answer SDP: %w", err)
	}

	return answer, nil

}

func (o *WebRTCOfferHandshake) GetAnswerPeerCandidates(ctx context.Context) ([]webrtc.ICECandidate, error) {
	candidates, err := o.api.PollPeerToPeerAnswerCandidatesSync(
		ctx,
		o.handshakeID,
		o.candidatesReceived,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get answer candidates: %w", err)
	}

	o.candidatesReceived += len(candidates)

	return candidates, nil
}

func (o *WebRTCOfferHandshake) AddOfferPeerCandidate(ctx context.Context, candidate webrtc.ICECandidate) error {
	return o.api.PostPeerToPeerOfferPeerCandidateSync(ctx, o.handshakeID, candidate)
}

func NewWebRTCAnswerHandshake(api *BringYourApi, handshakeID string) *WebRTCAnswerHandshake {
	return &WebRTCAnswerHandshake{
		api:         api,
		handshakeID: handshakeID,
	}
}

type WebRTCAnswerHandshake struct {
	api                *BringYourApi
	handshakeID        string
	candidatesReceived int
}

func (a *WebRTCAnswerHandshake) AnswerSDP(ctx context.Context, answer func(ctx context.Context, offer webrtc.SessionDescription) (webrtc.SessionDescription, error)) error {
	offer, err := a.api.PollPeerToPeerOfferSDPSync(
		ctx,
		a.handshakeID,
	)

	if err != nil {
		return fmt.Errorf("failed to get offer SDP: %w", err)
	}

	answerSDP, err := answer(ctx, offer)
	if err != nil {
		return fmt.Errorf("failed to answer SDP: %w", err)
	}

	err = a.api.PutPeerToPeerAnswerSDPSync(
		ctx,
		a.handshakeID,
		answerSDP,
	)

	if err != nil {
		return fmt.Errorf("failed to send answer SDP: %w", err)
	}

	return nil

}

func (a *WebRTCAnswerHandshake) GetOfferPeerCandidates(ctx context.Context) ([]webrtc.ICECandidate, error) {
	candidates, err := a.api.PollPeerToPeerOfferCandidatesSync(
		ctx,
		a.handshakeID,
		a.candidatesReceived,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get offer candidates: %w", err)
	}

	a.candidatesReceived += len(candidates)

	return candidates, nil
}

func (a *WebRTCAnswerHandshake) AddAnswerPeerCandidate(ctx context.Context, candidate webrtc.ICECandidate) error {
	return a.api.PostPeerToPeerAnswerPeerCandidateSync(ctx, a.handshakeID, candidate)
}
