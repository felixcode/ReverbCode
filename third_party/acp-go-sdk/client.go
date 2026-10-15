package acp

import (
	"io"
	"log/slog"
)

// ClientSideConnection provides the client's view of the connection and implements Agent calls.
type ClientSideConnection struct {
	conn   *Connection
	client Client
}

// NewClientSideConnection creates a new client-side connection bound to the
// provided Client implementation.
func NewClientSideConnection(client Client, peerInput io.Writer, peerOutput io.Reader) *ClientSideConnection {
	return NewClientSideConnectionWithOptions(client, peerInput, peerOutput, ConnectionOptions{})
}

// NewClientSideConnectionWithOptions retains normal protocol handling with an
// explicit bounded notification queue. It never blocks responses behind a handler.
func NewClientSideConnectionWithOptions(client Client, peerInput io.Writer, peerOutput io.Reader, options ConnectionOptions) *ClientSideConnection {

	csc := &ClientSideConnection{}
	csc.client = client
	csc.conn = NewConnectionWithOptions(csc.handleWithExtensions, peerInput, peerOutput, options)
	return csc
}

// Done exposes a channel that closes when the peer disconnects.
func (c *ClientSideConnection) Done() <-chan struct{} { return c.conn.Done() }

// SetLogger directs connection diagnostics to the provided logger.
func (c *ClientSideConnection) SetLogger(l *slog.Logger) { c.conn.SetLogger(l) }
