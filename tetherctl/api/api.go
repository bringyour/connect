package api

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
	tc     *tether.Client
}

type ApiOptions struct {
	ApiUrl       string
	Dname        string
	TetherClient *tether.Client
}

func (o *ApiOptions) AreValid() error {
	if o.ApiUrl == "" {
		return fmt.Errorf("API URL is required")
	}
	if o.Dname == "" {
		return fmt.Errorf("device name is required")
	}
	if o.TetherClient == nil {
		return fmt.Errorf("tether client is required")
	}
	return nil
}

func StartApi(o ApiOptions, errorCallback func(err error)) (*Api, error) {
	if err := o.AreValid(); err != nil {
		return nil, fmt.Errorf("invalid API options: %w", err)
	}

	api := Api{
		dname: o.Dname,
		tc:    o.TetherClient,
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

	// start server in goroutine
	go func() {
		if err := api.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errorCallback(err)
			return
		}
	}()

	return &api, nil
}

func (a *Api) StopApi() error {
	if a.server == nil {
		return nil
	}
	// try to shutdown the server gracefully (wait max 5 secs to finish pending requests)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("server forced to shutdown: %w", err)
	}
	a.server = nil
	return nil
}

func (a *Api) getPeerConfig(context *gin.Context) {
	endpointType := context.Param("endpoint_type")
	publicKey := parsePublicKey(context)

	config, err := a.tc.GetPeerConfig(a.dname, publicKey, endpointType)
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

	config, err := a.tc.AddPeerToDeviceAndGetConfig(a.dname, publicKey, endpointType)
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

	err = a.tc.RemovePeerFromDevice(a.dname, _pk)
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
