package zmq_publisher

import (
	"bytes"
	"fmt"

	"github.com/al-kimmel-serj/bus"
	"github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"
)

type Publisher[Payload proto.Message] struct {
	topicPrefix []byte
	unregister  func() error
	zmqContext  *zmq4.Context
	zmqSocket   *zmq4.Socket
}

func New[Payload proto.Message](
	host string,
	port int,
	eventName bus.EventName,
	eventVersion bus.EventVersion,
	publishersRegistry bus.PublishersRegistry,
) (*Publisher[Payload], error) {
	zmqContext, _ := zmq4.NewContext()
	zmqSocket, _ := zmqContext.NewSocket(zmq4.PUB)
	err := zmqSocket.Bind(fmt.Sprintf("tcp://*:%d", port))
	if err != nil {
		return nil, fmt.Errorf("zmq4.Socket.Bind error: %w", err)
	}

	topicPrefix := []byte(fmt.Sprintf(bus.TopicPrefixFormat, eventName, eventVersion))

	unregister, err := publishersRegistry.Register(eventName, eventVersion, host, port)
	if err != nil {
		_ = zmqSocket.Close()
		return nil, fmt.Errorf("PublishersRegistry.Register error: %w", err)
	}

	return &Publisher[Payload]{
		topicPrefix: topicPrefix,
		unregister:  unregister,
		zmqContext:  zmqContext,
		zmqSocket:   zmqSocket,
	}, nil
}

func (p *Publisher[Payload]) Publish(eventKey bus.EventKey, eventPayload Payload) error {
	buf := bytes.NewBuffer(p.topicPrefix)

	if len(eventKey) > 0 {
		buf.WriteString(string(eventKey))
	}
	buf.WriteByte(bus.TopicAndPayloadDelimiter)

	msg, err := proto.Marshal(eventPayload)
	if err != nil {
		return fmt.Errorf("proto.Marshal error: %w", err)
	}

	_, err = buf.Write(msg)
	if err != nil {
		return fmt.Errorf("bytes.Buffer.Write error: %w", err)
	}

	_, err = p.zmqSocket.SendBytes(buf.Bytes(), 0)
	if err != nil {
		return fmt.Errorf("zmq4.Socket.SendBytes error: %w", err)
	}

	return nil
}

func (p *Publisher[Payload]) Stop() error {
	err := p.unregister()
	if err != nil {
		return fmt.Errorf("unregister error: %w", err)
	}

	err = p.zmqSocket.Close()
	if err != nil {
		return fmt.Errorf("zmq.Socket.Close error: %w", err)
	}

	err = p.zmqContext.Term()
	if err != nil {
		return fmt.Errorf("zmq.Context.Term error: %w", err)
	}

	return nil
}
