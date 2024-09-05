package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type Api struct {
	server   *http.Server
	dname    string
	endpoint string
}

type ApiOptions struct {
	ApiUrl     string
	WgEndpoint string
	Dname      string
}

func (o *ApiOptions) AreValid() error {
	if o.ApiUrl == "" {
		return fmt.Errorf("API URL is required")
	}
	if o.WgEndpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if o.Dname == "" {
		return fmt.Errorf("device name is required")
	}
	return nil
}

func startApi(o ApiOptions) (*Api, error) {
	if err := o.AreValid(); err != nil {
		return nil, fmt.Errorf("invalid API options: %w", err)
	}

	api := Api{
		dname:    o.Dname,
		endpoint: o.WgEndpoint,
	}

	router := gin.Default()
	// endpoint for adding peer and returning config
	router.POST("/peer/add/*pubkey", func(c *gin.Context) { api.addPeer(c) })
	// endpoint for removing peer
	router.DELETE("/peer/remove/*pubkey", func(c *gin.Context) { api.removePeer(c) })
	// endpoint for getting existing peer config
	router.GET("/peer/config/*pubkey", func(c *gin.Context) { api.getPeerConfig(c) })

	// wrap Gin router in an HTTP server
	api.server = &http.Server{
		Addr:    o.ApiUrl,
		Handler: router,
	}
	l.Verbosef("Started API server at %s for device %q", o.ApiUrl, o.Dname)

	// start server in goroutine and wait for interrupt signal
	go func() {
		if err := api.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			l.Errorf("Error listen: %s", err)
			return
		}
	}()

	return &api, nil
}

func (a *Api) stopApi() {
	if a.server == nil {
		return
	}
	// try to shutdown the server gracefully (wait max 5 secs to finish pending requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.server.Shutdown(ctx); err != nil {
		l.Errorf("Server forced to shutdown: %v", err)
		return
	}
	l.Verbosef("API Server stopped")
	a.server = nil
}

func (a *Api) getPeerConfig(context *gin.Context) {
	publicKey := parsePublicKey(context)

	config, err := tc.GetPeerConfig(a.dname, publicKey, a.endpoint)
	if err != nil {
		context.String(http.StatusNotFound, fmt.Sprintf("%d Not Found - %v", http.StatusNotFound, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

func (a *Api) addPeer(context *gin.Context) {
	publicKey := parsePublicKey(context)

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		context.String(http.StatusBadRequest, fmt.Sprintf("%d Bad Request - Invalid public key %q", http.StatusBadRequest, publicKey))
		return
	}

	err = tc.AddPeerToDevice(a.dname, _pk)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - %v", http.StatusInternalServerError, err))
		return
	}

	config, err := tc.GetPeerConfig(a.dname, publicKey, a.endpoint)
	if err != nil {
		context.String(http.StatusNotFound, fmt.Sprintf("%d Not Found - %v", http.StatusNotFound, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

func (a *Api) removePeer(context *gin.Context) {
	publicKey := parsePublicKey(context)

	_pk, err := wgtypes.ParseKey(publicKey)
	if err != nil {
		context.String(http.StatusBadRequest, fmt.Sprintf("%d Bad Request - Invalid public key %q", http.StatusBadRequest, publicKey))
		return
	}

	err = tc.RemovePeerFromDevice(a.dname, _pk)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - %v", http.StatusInternalServerError, err))
		return
	}

	context.String(http.StatusOK, fmt.Sprintf("%d OK - Peer %q removed successfully", http.StatusOK, publicKey))
}

func parsePublicKey(context *gin.Context) string {
	publicKey := context.Param("pubkey")

	// remove leading slash since we are using wildcards (should always be trimmed)
	publicKey = strings.TrimPrefix(publicKey, "/")

	return publicKey
}
