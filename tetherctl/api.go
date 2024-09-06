package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"bringyour.com/connect/tether"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type Api struct {
	server *http.Server
	dname  string
}

type ApiOptions struct {
	ApiUrl string
	Dname  string
}

func (o *ApiOptions) AreValid() error {
	if o.ApiUrl == "" {
		return fmt.Errorf("API URL is required")
	}
	if o.Dname == "" {
		return fmt.Errorf("device name is required")
	}
	return nil
}

func startApi(o ApiOptions, errorCallback func(err error)) (*Api, error) {
	if err := o.AreValid(); err != nil {
		return nil, fmt.Errorf("invalid API options: %w", err)
	}

	api := Api{
		dname: o.Dname,
	}

	router := gin.Default()
	// endpoint for adding peer and returning config
	router.POST("/peer/add/:endpoint_type/*pubkey", func(c *gin.Context) { api.addPeer(c) })
	// endpoint for removing peer
	router.DELETE("/peer/remove/*pubkey", func(c *gin.Context) { api.removePeer(c) })
	// endpoint for getting existing peer config
	router.GET("/peer/config/:endpoint_type/*pubkey", func(c *gin.Context) { api.getPeerConfig(c) })

	// wrap Gin router in an HTTP server
	api.server = &http.Server{
		Addr:    o.ApiUrl,
		Handler: router,
	}
	l.Verbosef("Started API server at %s for device %q", o.ApiUrl, o.Dname)

	// start server in goroutine
	go func() {
		if err := api.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			l.Errorf("API error while listening: %s", err)
			errorCallback(err)
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
	endpointType := context.Param("endpoint_type")
	publicKey := parsePublicKey(context)

	config, err := tc.GetPeerConfig(a.dname, publicKey, endpointType)
	if err != nil {
		context.String(http.StatusNotFound, fmt.Sprintf("%d Not Found - %v", http.StatusNotFound, err))
		return
	}

	// return the config as plaintext in the response body
	context.String(http.StatusOK, config)
}

func (a *Api) addPeer(context *gin.Context) {
	endpointType := context.Param("endpoint_type")
	publicKey := parsePublicKey(context)

	if err := tether.EndpointType(endpointType).IsValid(); err != nil {
		context.String(http.StatusBadRequest, fmt.Sprintf("%d Bad Request - %v", http.StatusBadRequest, err))
		return
	}

	if _, err := wgtypes.ParseKey(publicKey); err != nil {
		context.String(http.StatusBadRequest, fmt.Sprintf("%d Bad Request - Invalid public key %q", http.StatusBadRequest, publicKey))
		return
	}

	config, err := tc.AddPeerToDeviceAndGetConfig(a.dname, publicKey, endpointType)
	if err != nil {
		context.String(http.StatusInternalServerError, fmt.Sprintf("%d Internal Server Error - %v", http.StatusInternalServerError, err))
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
