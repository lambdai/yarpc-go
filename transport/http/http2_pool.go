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
	"context"
	"errors"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/net/http2"
)

var errNoConnsAvailable = errors.New("http2 pool: no connections available")

// http2PoolConfig controls how an http2Pool scales the number of HTTP/2
// connections it maintains to a single peer.
type http2PoolConfig struct {
	minConns               int
	maxConns               int
	scaleUpThreshold       float64
	scaleDownGap           float64
	idleTimeout            time.Duration
	scalingMonitorInterval time.Duration
}

func defaultHTTP2PoolConfig() http2PoolConfig {
	return http2PoolConfig{
		minConns:               defaultHTTP2PoolMinConns,
		maxConns:               defaultHTTP2PoolMaxConns,
		scaleUpThreshold:       defaultHTTP2PoolScaleUpThreshold,
		scaleDownGap:           defaultHTTP2PoolScaleDownGap,
		idleTimeout:            defaultHTTP2PoolConnIdleTimeout,
		scalingMonitorInterval: defaultHTTP2PoolScalingMonitorInterval,
	}
}

// http2Pool maintains a set of HTTP/2 connections to a single peer address,
// scaling the number of connections up when existing ones approach the
// peer's advertised stream concurrency limit, and back down when load
// drops. It bypasses http2.Transport's own connection pooling: connections
// are dialed directly and wrapped with http2.Transport.NewClientConn, and
// this pool alone decides which connection serves each request.
type http2Pool struct {
	addr        string
	h2Transport *http2.Transport
	cfg         http2PoolConfig

	// connsPtr is an immutable slice, replaced via copy-on-write so reads
	// on the request hot path (pickConn) never block on a lock.
	connsPtr atomic.Pointer[[]*http2Conn]

	scalingUp atomic.Bool
	stop      chan struct{}
}

func newHTTP2Pool(addr string, h2Transport *http2.Transport, cfg http2PoolConfig) *http2Pool {
	p := &http2Pool{
		addr:        addr,
		h2Transport: h2Transport,
		cfg:         cfg,
		stop:        make(chan struct{}),
	}
	p.connsPtr.Store(&[]*http2Conn{})
	go p.monitorLoop()
	return p
}

// dial opens a new HTTP/2 connection to the pool's peer, reusing the
// transport's configured dial hook (installed by buildH2Transport for
// cleartext h2c dialing).
func (p *http2Pool) dial(ctx context.Context) (*http2Conn, error) {
	conn, err := p.h2Transport.DialTLSContext(ctx, "tcp", p.addr, nil)
	if err != nil {
		return nil, err
	}
	cc, err := p.h2Transport.NewClientConn(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return newHTTP2Conn(cc), nil
}

// pickConn returns the least-loaded usable connection, growing the pool
// (synchronously) if none is currently usable, and kicking off an
// asynchronous scale-up if the chosen connection is already busy.
func (p *http2Pool) pickConn(ctx context.Context) (*http2Conn, error) {
	conns := *p.connsPtr.Load()

	var best *http2Conn
	for _, c := range conns {
		if !c.usable() {
			continue
		}
		if best == nil || c.streamsActive() < best.streamsActive() {
			best = c
		}
	}

	if best == nil {
		// Cold start, or every connection is draining/saturated/dead:
		// dial synchronously so the caller has something to use now.
		return p.growPool(ctx)
	}

	p.maybeScaleUp(best)
	return best, nil
}

// growPool dials one more connection and adds it to the pool, unless the
// pool is already at its configured maximum, in which case it falls back
// to the least-bad existing connection (mirroring the way a single shared
// http2.Transport would queue an excess request rather than fail it).
func (p *http2Pool) growPool(ctx context.Context) (*http2Conn, error) {
	conns := *p.connsPtr.Load()
	if len(conns) >= p.cfg.maxConns {
		return leastLoaded(conns)
	}

	c, err := p.dial(ctx)
	if err != nil {
		if fallback, ferr := leastLoaded(conns); ferr == nil {
			return fallback, nil
		}
		return nil, err
	}
	p.addConn(c)
	return c, nil
}

// leastLoaded returns the conn with the fewest active streams, regardless
// of usable(), as a last-resort fallback when nothing better is available.
func leastLoaded(conns []*http2Conn) (*http2Conn, error) {
	var best *http2Conn
	for _, c := range conns {
		if best == nil || c.streamsActive() < best.streamsActive() {
			best = c
		}
	}
	if best == nil {
		return nil, errNoConnsAvailable
	}
	return best, nil
}

// maybeScaleUp dials an additional connection, asynchronously and at most
// once concurrently, when least is already busy enough that new requests
// risk queuing behind it.
func (p *http2Pool) maybeScaleUp(least *http2Conn) {
	max := least.maxConcurrentStreams()
	if max == 0 {
		// No SETTINGS observed yet; nothing to compare against.
		return
	}
	if float64(least.streamsActive()) < float64(max)*p.cfg.scaleUpThreshold {
		return
	}
	if len(*p.connsPtr.Load()) >= p.cfg.maxConns {
		return
	}
	if !p.scalingUp.CompareAndSwap(false, true) {
		return // a scale-up is already in flight
	}

	go func() {
		defer p.scalingUp.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), defaultDialerTimeout)
		defer cancel()
		if c, err := p.dial(ctx); err == nil {
			p.addConn(c)
		}
	}()
}

// monitorLoop periodically scales the pool down when load no longer
// justifies the current connection count, and reaps drained/idle
// connections.
func (p *http2Pool) monitorLoop() {
	ticker := time.NewTicker(p.cfg.scalingMonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.maybeScaleDown()
			p.cleanupConns()
		case <-p.stop:
			return
		}
	}
}

// maybeScaleDown marks the most-loaded connection as draining if the
// remaining connections can absorb the current total load, with a
// scaleDownGap safety margin, and the pool is above its configured
// minimum.
func (p *http2Pool) maybeScaleDown() {
	conns := activeConns(*p.connsPtr.Load())
	if len(conns) <= p.cfg.minConns {
		return
	}

	var totalActive, totalCapacity int
	var mostLoaded *http2Conn
	for _, c := range conns {
		totalActive += c.streamsActive()
		totalCapacity += int(c.maxConcurrentStreams())
		if mostLoaded == nil || c.streamsActive() > mostLoaded.streamsActive() {
			mostLoaded = c
		}
	}
	if mostLoaded == nil || totalCapacity == 0 {
		return
	}

	remainingCapacity := totalCapacity - int(mostLoaded.maxConcurrentStreams())
	if remainingCapacity <= 0 {
		return
	}
	if float64(totalActive) < float64(remainingCapacity)*(1-p.cfg.scaleDownGap) {
		mostLoaded.markDraining()
	}
}

// cleanupConns closes and removes draining connections that have gone
// idle, and marks connections that have been idle past idleTimeout as
// draining so a future tick can close them.
func (p *http2Pool) cleanupConns() {
	conns := *p.connsPtr.Load()
	for _, c := range conns {
		if c.draining() && c.streamsActive() == 0 {
			c.close()
			p.removeConn(c)
			continue
		}
		if state := c.cc.State(); !state.LastIdle.IsZero() && time.Since(state.LastIdle) > p.cfg.idleTimeout {
			c.markDraining()
		}
	}
}

func activeConns(conns []*http2Conn) []*http2Conn {
	out := make([]*http2Conn, 0, len(conns))
	for _, c := range conns {
		if !c.draining() {
			out = append(out, c)
		}
	}
	return out
}

// addConn appends c to the pool via copy-on-write.
func (p *http2Pool) addConn(c *http2Conn) {
	for {
		old := p.connsPtr.Load()
		next := make([]*http2Conn, 0, len(*old)+1)
		next = append(next, *old...)
		next = append(next, c)
		if p.connsPtr.CompareAndSwap(old, &next) {
			return
		}
	}
}

// removeConn removes c from the pool via copy-on-write. It is a no-op if c
// is not present (e.g. concurrently removed already).
func (p *http2Pool) removeConn(c *http2Conn) {
	for {
		old := p.connsPtr.Load()
		idx := -1
		for i, cur := range *old {
			if cur == c {
				idx = i
				break
			}
		}
		if idx == -1 {
			return
		}
		next := make([]*http2Conn, 0, len(*old)-1)
		next = append(next, (*old)[:idx]...)
		next = append(next, (*old)[idx+1:]...)
		if p.connsPtr.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Close stops the pool's monitor loop and closes every connection it
// holds.
func (p *http2Pool) Close() {
	close(p.stop)
	for _, c := range *p.connsPtr.Load() {
		c.close()
	}
}
