package connect


// FIXME settings

// this should return an active, tested connection
// TODO pass in an API context
func NewP2pConnActive(ctx context.Context, peerId Id, streamId Id) (net.Conn, error) {
	// FIXME
	return nil, fmt.Errorf("Not implemented.")
}


// this should return an active, tested connection
// TODO pass in an API context
func NewP2pConnPassive(ctx context.Context, peerId Id, streamId Id) (net.Conn, error) {
	// FIXME
	return nil, fmt.Errorf("Not implemented.")
}

