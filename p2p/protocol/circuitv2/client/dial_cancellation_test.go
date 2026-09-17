package client_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	pb "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/util"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func newCancellationTestHost(t *testing.T, opts ...libp2p.Option) host.Host {
	t.Helper()
	opts = append([]libp2p.Option{libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")}, opts...)
	h, err := libp2p.New(opts...)
	require.NoError(t, err)
	t.Cleanup(func() { h.Close() })
	return h
}

func TestHopCancellationReleasesTargetAndPreservesRelay(t *testing.T) {
	relay := newCancellationTestHost(t)
	next := newCancellationTestHost(t)
	h := newCancellationTestHost(t)
	target := newCancellationTestHost(t)
	started, stopped := make(chan struct{}), make(chan struct{})
	relay.SetStreamHandler(proto.ProtoIDv2Hop, func(s network.Stream) {
		defer s.Reset()
		rd := util.NewDelimitedReader(s, 4096)
		var msg pb.HopMessage
		if err := rd.ReadMsg(&msg); err != nil {
			rd.Close()
			return
		}
		rd.Close()
		close(started)
		io.Copy(io.Discard, s)
		close(stopped)
	})
	nextStarted := make(chan struct{})
	next.SetStreamHandler(proto.ProtoIDv2Hop, func(s network.Stream) {
		defer s.Close()
		rd := util.NewDelimitedReader(s, 4096)
		defer rd.Close()
		var msg pb.HopMessage
		if err := rd.ReadMsg(&msg); err != nil {
			return
		}
		close(nextStarted)
		util.NewDelimitedWriter(s).WriteMsg(&pb.HopMessage{Type: pb.HopMessage_STATUS.Enum(), Status: pb.Status_NO_RESERVATION.Enum()})
	})
	// 两次拨号都在 HOP 阶段结束，不会进入连接升级。
	transport, err := client.New(h, nil)
	require.NoError(t, err)
	defer transport.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	addr := relay.Addrs()[0].Encapsulate(ma.StringCast("/p2p/" + relay.ID().String() + "/p2p-circuit"))
	go func() { _, err := transport.Dial(ctx, addr, target.ID()); done <- err }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("未进入 HOP")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("取消后 HOP 仍然阻塞")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("远端 HOP 流未关闭")
	}
	require.Len(t, h.Network().ConnsToPeer(relay.ID()), 1)
	nextCtx, stopNext := context.WithTimeout(context.Background(), time.Second)
	defer stopNext()
	nextAddr := next.Addrs()[0].Encapsulate(ma.StringCast("/p2p/" + next.ID().String() + "/p2p-circuit"))
	_, err = transport.Dial(nextCtx, nextAddr, target.ID())
	require.Error(t, err)
	require.False(t, errors.Is(err, context.DeadlineExceeded))
	select {
	case <-nextStarted:
	default:
		t.Fatal("目标拨号占用未释放")
	}
}

func TestSuccessfulHopSurvivesDialContextCancellation(t *testing.T) {
	relay := newCancellationTestHost(t, libp2p.EnableRelayService(), libp2p.ForceReachabilityPublic())
	target := newCancellationTestHost(t)
	h := newCancellationTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := client.Reserve(ctx, target, peer.AddrInfo{ID: relay.ID(), Addrs: relay.Addrs()})
	require.NoError(t, err)
	target.SetStreamHandler("/test/cancel-safe/1", func(s network.Stream) { defer s.Close(); s.Write([]byte("ok")) })
	addr := relay.Addrs()[0].Encapsulate(ma.StringCast("/p2p/" + relay.ID().String() + "/p2p-circuit"))
	require.NoError(t, h.Connect(ctx, peer.AddrInfo{ID: target.ID(), Addrs: []ma.Multiaddr{addr}}))
	cancel()
	streamCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	streamCtx = network.WithAllowLimitedConn(network.WithNoDial(streamCtx, "existing relay"), "test")
	s, err := h.NewStream(streamCtx, target.ID(), "/test/cancel-safe/1")
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.SetReadDeadline(time.Now().Add(time.Second)))
	buf := make([]byte, 2)
	_, err = io.ReadFull(s, buf)
	require.NoError(t, err)
	require.Equal(t, "ok", string(buf))
}
