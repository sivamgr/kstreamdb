package kstreamdb

import (
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"

	"github.com/cskr/pubsub"
	// register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

//Socket struct
type Socket struct {
	InProcPubSub   *pubsub.PubSub
	Sock           mangos.Socket
	IsStreamServer bool
}

//StartStreaming starts the stream publisher socket
func StartStreaming(url string) (Socket, error) {
	s := Socket{
		InProcPubSub:   pubsub.New(256),
		IsStreamServer: false,
	}

	var err error
	if s.Sock, err = pub.NewSocket(); err != nil {
		return s, err
	}
	if err := s.Sock.Listen(url); err != nil {
		return s, err
	}

	s.IsStreamServer = true

	return s, nil
}

//ConnectToStream connect to a tick stream
func ConnectToStream(url string) (Socket, error) {
	s := Socket{
		InProcPubSub:   pubsub.New(256),
		IsStreamServer: false,
	}

	var err error
	if s.Sock, err = pub.NewSocket(); err != nil {
		return s, err
	}
	if err := s.Sock.Dial(url); err != nil {
		return s, err
	}
	// Empty byte array effectively subscribes to everything
	err = s.Sock.SetOption(mangos.OptionSubscribe, []byte(""))
	if err == nil {
		go s.handleStream()
	}
	return s, err
}

func (s *Socket) handleStream() {
	for {
		msg, err := s.Sock.Recv()
		if err != nil {
			break
		}
		ticks := make([]TickData, 0)
		err = decodeTicksFromBytes(msg, ticks)
		s.InProcPubSub.Pub(ticks, "tick")
	}
}

// Publish publishes all ticks to channels
func (s *Socket) Publish(ticks []TickData) error {
	if s.InProcPubSub != nil {
		s.InProcPubSub.Pub(ticks, "tick")
	}

	if s.IsStreamServer {
		b, err := encodeTicks(ticks)
		if err == nil {
			return s.Sock.Send(b)
		}
		return err
	}
	return nil
}

// SubscribeTicks returns the ticks channel
func (s *Socket) SubscribeTicks() chan interface{} {
	return s.InProcPubSub.Sub("tick")
}
