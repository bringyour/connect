package tether

import (
	"fmt"
	"strings"
)

type EndpointType string

// NOTE: all endpoint types are normalized to have no whitespaces and be lowercase
const (
	EndpointIPv4       EndpointType = "ipv4"
	EndpointIPv6       EndpointType = "ipv6"
	EndpointDomainName EndpointType = "domain"
	EndpointAny        EndpointType = "any" // only for getting an endpoint (gets any available endpoint)
)

// IsValid checks if the endpoint type is valid.
//
// Returns an error if the endpoint type is invalid which can be checked using errors.Is(err, ErrInvalidEndpointType).
func (et EndpointType) IsValid() error {
	// normalize endpoint type to lowercase without any whitespace
	n := func(e EndpointType) string {
		return strings.ReplaceAll(strings.ToLower(string(e)), " ", "")
	}
	switch n(et) {
	case n(EndpointIPv4), n(EndpointIPv6), n(EndpointDomainName), n(EndpointAny):
		return nil
	}
	return fmt.Errorf("%w: %q", ErrInvalidEndpointType, string(et))
}

// AddEndpoint adds an endpoint of the requested type. If the endpoint type already exists, it is overwritten.
//
// Returns an error if the endpoint type is invalid which can be checked using errors.Is(err, ErrInvalidEndpointType).
// Additionally, and endpoint of type EndpointAny cannot be added resulting in an error which can be checked using errors.Is(err, ErrEndpointAddAny).
func (c *Client) AddEndpoint(endpointType EndpointType, endpoint string) error {
	if err := endpointType.IsValid(); err != nil {
		return err
	}
	if endpointType == EndpointAny {
		return ErrEndpointAddAny
	}
	c.endpoints[endpointType] = endpoint
	return nil
}

// RemoveEndpoint removes an endpoint of the requested type. If the endpoint is not found, nothing happens.
//
// Returns an error if the endpoint type is invalid which can be checked using errors.Is(err, ErrInvalidEndpointType).
func (c *Client) RemoveEndpoint(endpointType EndpointType) error {
	if err := endpointType.IsValid(); err != nil {
		return err
	}
	delete(c.endpoints, endpointType)
	return nil
}

// ResetEndpoints removes all endpoints from the client.
func (c *Client) ResetEndpoints() {
	c.endpoints = make(map[EndpointType]string)
}

// GetEndpoint returns the endpoint of the requested type.
//
// An error is returned if the endpoint of the requested type is not found  or if the type is invalid which can be checked using errors.Is(err, ErrEndpointNotFound | ErrInvalidEndpointType), respectively.
func (c *Client) GetEndpoint(endpointType EndpointType) (string, error) {
	if err := endpointType.IsValid(); err != nil {
		return "", err
	}
	if endpointType == EndpointAny {
		for _, endpoint := range c.endpoints {
			return endpoint, nil // get first endpoint
		}
	} else {
		endpoint, ok := c.endpoints[endpointType]
		if ok {
			return endpoint, nil
		}
	}
	return "", fmt.Errorf("%s %w", string(endpointType), ErrEndpointNotFound)
}
