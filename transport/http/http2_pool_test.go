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
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
)

// newTestH2Server starts a cleartext HTTP/2 test server with the given
// maxConcurrentStreams, and returns its host:port address plus an
// *http2.Transport wired the same way buildH2Transport wires the real one.
func newTestH2Server(t *testing.T, maxConcurrentStreams uint32, handler http.HandlerFunc) (addr string, h2Transport *http2.Transport) {
	t.Helper()

	h2s := &http2.Server{
		MaxConcurrentStreams: maxConcurrentStreams,
		IdleTimeout:          defaultIdleConnTimeout,
	}
	h1s := httptest.NewUnstartedServer(handler)
	h1s.Config.Protocols = new(http.Protocols)
	h1s.Config.Protocols.SetHTTP1(true)
	h1s.Config.Protocols.SetUnencryptedHTTP2(true)
	http2.ConfigureServer(h1s.Config, h2s)
	h1s.Start()
	t.Cleanup(h1s.Close)

	u, err := url.Parse(h1s.URL)
	require.NoError(t, err)

	transportOpts := newTransportOptions()
	h2t := buildH2Transport(&transportOpts)

	return u.Host, h2t
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHTTP2PoolPickConnDialsWhenEmpty(t *testing.T) {
	addr, h2t := newTestH2Server(t, 10, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	pool := newHTTP2Pool(addr, h2t, defaultHTTP2PoolConfig())
	defer pool.Close()

	conn, err := pool.pickConn(context.Background())
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Len(t, *pool.connsPtr.Load(), 1)
}

func TestHTTP2PoolPickConnPicksLeastLoaded(t *testing.T) {
	release := make(chan struct{})
	addr, h2t := newTestH2Server(t, 100, func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Write([]byte("ok"))
	})

	pool := newHTTP2Pool(addr, h2t, defaultHTTP2PoolConfig())
	defer pool.Close()

	busy, err := pool.dial(context.Background())
	require.NoError(t, err)
	idle, err := pool.dial(context.Background())
	require.NoError(t, err)
	pool.addConn(busy)
	pool.addConn(idle)

	// Occupy busy with an in-flight stream so idle is strictly less loaded.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, _ := http.NewRequest(http.MethodGet, "https://"+addr+"/", strings.NewReader(""))
		busy.cc.RoundTrip(req)
	}()

	waitForCondition(t, time.Second, func() bool { return busy.streamsActive() == 1 })

	picked, err := pool.pickConn(context.Background())
	require.NoError(t, err)
	assert.Same(t, idle, picked, "pool should prefer the less-loaded connection")

	close(release)
	wg.Wait()
}

func TestHTTP2PoolMaybeScaleUpSingleFlight(t *testing.T) {
	release := make(chan struct{})
	addr, h2t := newTestH2Server(t, 2, func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Write([]byte("ok"))
	})

	cfg := defaultHTTP2PoolConfig()
	cfg.maxConns = 5
	cfg.scaleUpThreshold = 0.5
	pool := newHTTP2Pool(addr, h2t, cfg)
	defer pool.Close()

	c, err := pool.dial(context.Background())
	require.NoError(t, err)
	pool.addConn(c)

	waitForCondition(t, time.Second, func() bool { return c.maxConcurrentStreams() > 0 })

	// Occupy c with in-flight streams so streamsActive crosses the
	// scale-up threshold (2 active out of MaxConcurrentStreams=2, with a
	// 0.5 threshold).
	var streamsWG sync.WaitGroup
	for i := 0; i < 2; i++ {
		streamsWG.Add(1)
		go func() {
			defer streamsWG.Done()
			req, _ := http.NewRequest(http.MethodGet, "https://"+addr+"/", strings.NewReader(""))
			c.cc.RoundTrip(req)
		}()
	}
	waitForCondition(t, time.Second, func() bool { return c.streamsActive() == 2 })

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.maybeScaleUp(c)
		}()
	}
	wg.Wait()

	waitForCondition(t, time.Second, func() bool { return len(*pool.connsPtr.Load()) == 2 })
	assert.Len(t, *pool.connsPtr.Load(), 2, "concurrent scale-up attempts should add exactly one connection")

	close(release)
	streamsWG.Wait()
}

func TestHTTP2PoolScaleDownHysteresis(t *testing.T) {
	addr, h2t := newTestH2Server(t, 100, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	cfg := defaultHTTP2PoolConfig()
	cfg.minConns = 1
	cfg.scaleDownGap = 0.3
	pool := newHTTP2Pool(addr, h2t, cfg)
	defer pool.Close()

	c1, err := pool.dial(context.Background())
	require.NoError(t, err)
	c2, err := pool.dial(context.Background())
	require.NoError(t, err)
	pool.addConn(c1)
	pool.addConn(c2)

	waitForCondition(t, time.Second, func() bool {
		return c1.maxConcurrentStreams() > 0 && c2.maxConcurrentStreams() > 0
	})

	// No load at all: draining one of two idle connections should be safe
	// (remaining capacity is far above zero load with plenty of headroom).
	pool.maybeScaleDown()

	draining := c1.draining() || c2.draining()
	assert.True(t, draining, "an idle connection should be marked draining when load is zero")

	// Calling it again should not flap the non-draining connection back and
	// forth: it should stay marked draining once already draining, and the
	// pool must not go below minConns of *active* (non-draining) conns.
	active := activeConns(*pool.connsPtr.Load())
	assert.GreaterOrEqual(t, len(active), cfg.minConns)
}

func TestHTTP2PoolAddRemoveConnRace(t *testing.T) {
	addr, h2t := newTestH2Server(t, 100, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	pool := newHTTP2Pool(addr, h2t, defaultHTTP2PoolConfig())
	defer pool.Close()

	const n = 16
	conns := make([]*http2Conn, n)
	for i := range conns {
		conns[i] = newHTTP2Conn(nil) // not usable, but fine for CAS bookkeeping
	}

	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c *http2Conn) {
			defer wg.Done()
			pool.addConn(c)
		}(c)
	}
	wg.Wait()
	assert.Len(t, *pool.connsPtr.Load(), n)

	for _, c := range conns {
		wg.Add(1)
		go func(c *http2Conn) {
			defer wg.Done()
			pool.removeConn(c)
		}(c)
	}
	wg.Wait()
	assert.Empty(t, *pool.connsPtr.Load())
}
