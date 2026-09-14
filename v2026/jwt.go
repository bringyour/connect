package connect

import (
	gojwt "github.com/golang-jwt/jwt/v5"
)

type ByJwt struct {
	UserId      Id
	NetworkName string
	NetworkId   Id
	ClientId    Id
}

func ParseByJwtUnverified(byJwtStr string) (*ByJwt, error) {
	parser := gojwt.NewParser()
	token, _, err := parser.ParseUnverified(byJwtStr, gojwt.MapClaims{})
	if err != nil {
		return nil, err
	}

	claims := token.Claims.(gojwt.MapClaims)

	byJwt := &ByJwt{}

	if userIdStr, ok := claims["user_id"]; ok {
		if userId, err := ParseId(userIdStr.(string)); err == nil {
			byJwt.UserId = userId
		}
	}
	if networkName, ok := claims["network_name"]; ok {
		byJwt.NetworkName = networkName.(string)
	}
	if networkIdStr, ok := claims["network_name"]; ok {
		if networkId, err := ParseId(networkIdStr.(string)); err == nil {
			byJwt.NetworkId = networkId
		}
	}
	if clientIdStr, ok := claims["client_id"]; ok {
		if clientId, err := ParseId(clientIdStr.(string)); err == nil {
			byJwt.ClientId = clientId
		}
	}

	return byJwt, nil
}
