package libp2pquic

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	p2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

type sessionTicketCache struct {
	tls.ClientSessionCache
	tickets atomic.Int32
}

func (c *sessionTicketCache) Put(key string, state *tls.ClientSessionState) {
	if state != nil {
		c.tickets.Add(1)
	}
	c.ClientSessionCache.Put(key, state)
}

func TestHandshakeKeepsSessionTicketsDisabled(t *testing.T) {
	for _, tc := range connTestCases {
		t.Run(tc.Name, func(t *testing.T) {
			serverID, serverKey := createPeer(t)
			clientID, clientKey := createPeer(t)
			serverTransport, err := NewTransport(serverKey, newConnManager(t, tc.Options...), nil, nil, nil)
			require.NoError(t, err)
			defer serverTransport.(io.Closer).Close()
			ln := runServer(t, serverTransport, "/ip4/127.0.0.1/udp/0/quic-v1")
			defer ln.Close()

			identity, err := p2ptls.NewIdentity(clientKey)
			require.NoError(t, err)
			cache := &sessionTicketCache{ClientSessionCache: tls.NewLRUClientSessionCache(1)}
			for i := range 2 {
				t.Run(fmt.Sprintf("connection_%d", i+1), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					tlsConf, verifiedKey := identity.ConfigForPeer(serverID)
					// 客户端主动支持 ticket，以便检测服务端是否意外启用了会话恢复。
					tlsConf.SessionTicketsDisabled = false
					tlsConf.ClientSessionCache = cache
					client, err := quic.DialAddr(ctx, ln.Addr().String(), tlsConf, &quic.Config{})
					require.NoError(t, err)
					defer client.CloseWithError(0, "")
					server, err := ln.Accept()
					require.NoError(t, err)
					defer server.Close()

					require.Equal(t, clientID, server.RemotePeer())
					require.True(t, server.RemotePublicKey().Equals(clientKey.GetPublic()))
					require.False(t, client.ConnectionState().TLS.DidResume)
					require.False(t, server.(*conn).quicConn.ConnectionState().TLS.DidResume)
					select {
					case key := <-verifiedKey:
						require.True(t, key.Equals(serverKey.GetPublic()))
					default:
						t.Fatal("本次连接未执行服务端证书身份校验")
					}

					// 完成握手后的数据传输，再检查客户端是否收到了 session ticket。
					stream, err := server.OpenStream(ctx)
					require.NoError(t, err)
					_, err = stream.Write([]byte("authenticated"))
					require.NoError(t, err)
					require.NoError(t, stream.CloseWrite())
					incoming, err := client.AcceptStream(ctx)
					require.NoError(t, err)
					require.NoError(t, incoming.SetReadDeadline(time.Now().Add(5*time.Second)))
					data, err := io.ReadAll(incoming)
					require.NoError(t, err)
					require.Equal(t, "authenticated", string(data))
					require.NoError(t, client.CloseWithError(0, ""))
					require.Zero(t, cache.tickets.Load(), "服务端不应签发 session ticket")
				})
			}
		})
	}
}
