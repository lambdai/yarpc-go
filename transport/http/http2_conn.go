// Copyright (c) 2026 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package http

import (
	"time"

	"go.uber.org/atomic"
	"golang.org/x/net/http2"
)

// http2ConnState describes the lifecycle state of a pooled http2Conn, as
// tracked by http2Pool. It is distinct from http2.ClientConnState, which
// reflects the live protocol-level state of the underlying connection.
type http2ConnState int32

const (
	// http2ConnActive connections are eligible to be picked for new
	// requests.
	http2ConnActive http2ConnState = iota
	// http2ConnDraining connections are being scaled down: they are no
	// longer picked for new requests, and are closed once idle.
	http2ConnDraining
)

// http2Conn wraps a single *http2.ClientConn pooled by an http2Pool. All
// concurrency/utilization accounting is read live from the underlying
// http2.ClientConn (via State()/CanTakeNewRequest()), which already tracks
// active streams against the peer's negotiated MaxConcurrentStreams.
type http2Conn struct {
	cc        *http2.ClientConn
	state     atomic.Int32
	createdAt time.Time
}

func newHTTP2Conn(cc *http2.ClientConn) *http2Conn {
	return &http2Conn{
		cc:        cc,
		createdAt: time.Now(),
	}
}

// usable reports whether this connection may be picked for a new request.
func (c *http2Conn) usable() bool {
	return http2ConnState(c.state.Load()) == http2ConnActive && c.cc.CanTakeNewRequest()
}

// streamsActive returns the number of streams currently in flight on this
// connection, read live from the underlying HTTP/2 connection state.
func (c *http2Conn) streamsActive() int {
	return c.cc.State().StreamsActive
}

// maxConcurrentStreams returns the peer-advertised concurrency limit for
// this connection. Zero means no SETTINGS frame has been received yet.
func (c *http2Conn) maxConcurrentStreams() uint32 {
	return c.cc.State().MaxConcurrentStreams
}

// markDraining marks the connection so it is no longer picked for new
// requests. It does not close the connection or wait for existing streams
// to finish; the pool's monitor loop is responsible for closing it once
// idle.
func (c *http2Conn) markDraining() {
	c.state.Store(int32(http2ConnDraining))
}

func (c *http2Conn) draining() bool {
	return http2ConnState(c.state.Load()) == http2ConnDraining
}

func (c *http2Conn) close() error {
	return c.cc.Close()
}
