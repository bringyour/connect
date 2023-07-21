package connect


// connections to platform use the extender architecture
// wraps the by tls connection in the extender tls


// extenders are identified and credited with the platform by ip address
// they forward to a special port, 8443, that whitelists their ip without rate limiting
// when an extender gets an http message from a client, it always connects tcp to connect.bringyour.com:8443
// appends the proxy protocol headers, and then forwards the bytes from the client
// https://docs.nginx.com/nginx/admin-guide/load-balancer/using-proxy-protocol/
// rate limit using $proxy_protocol_addr https://www.nginx.com/blog/rate-limiting-nginx/
// add the source ip as the X-Extender header


// websocket transport to by platform

// webrtc transport peer to peer

// websocket transport takes jwt and connect url
// note there can be multiple websocket transports active with different urls (extenders)



// no need for long polling


// primary domain always comes before the extender in terms of priority and weight
// the extender is used when the primary domain is blocked
// IMPORTANT!!  usage of the extender must always verify the server cert is from the primary domain to verify it was forwarded to the real platform



// options menu in login screen:
// set platform urls


