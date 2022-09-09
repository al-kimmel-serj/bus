package zmq_subscriber

import (
	"bytes"
	"sync"

	"github.com/al-kimmel-serj/bus"
	"github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"
)

type Subscriber[Payload proto.Message] struct {
	errorHandler       func(error)
	payloadChan        chan Payload
	payloadFactory     func() Payload
	publishers         map[bus.PublisherEndpoint]struct{}
	publishersMx       sync.Mutex
	readerShutdownChan chan struct{}
	stopWatcher        func() error
	zmqContext         *zmq4.Context
	zmqSocket          *zmq4.Socket
}

func New[Payload proto.Message](
	eventName bus.EventName,
	eventVersion bus.EventVersion,
	publishersRegistryWatcher bus.PublishersRegistry,
	payloadFactory func() Payload,
	errorHandler func(error),
) (*Subscriber[Payload], error) {
	s := &Subscriber[Payload]{
		errorHandler:       errorHandler,
		payloadChan:        make(chan Payload),
		payloadFactory:     payloadFactory,
		publishers:         make(map[bus.PublisherEndpoint]struct{}),
		readerShutdownChan: make(chan struct{}),
	}

	var err error
	s.zmqContext, err = zmq4.NewContext()
	if err != nil {
		return nil, err
	}
	s.zmqSocket, err = s.zmqContext.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, err
	}

	go s.reader()

	s.stopWatcher, err = publishersRegistryWatcher.Watch(eventName, eventVersion, func(endpoints []bus.PublisherEndpoint) {
		s.updatePublishers(endpoints)
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Subscriber[Payload]) PayloadChan() <-chan Payload {
	return s.payloadChan
}

func (s *Subscriber[Payload]) Stop() error {
	err := s.stopWatcher()
	if err != nil {
		return err
	}

	err = s.stopReader()
	if err != nil {
		return err
	}

	return nil
}

func (s *Subscriber[Payload]) Subscribe(eventFilter bus.EventFilter) error {
	return s.zmqSocket.SetSubscribe(string(eventFilter))
}

func (s *Subscriber[Payload]) Unsubscribe(eventFilter bus.EventFilter) error {
	return s.zmqSocket.SetUnsubscribe(string(eventFilter))
}

func (s *Subscriber[Payload]) publishersDiff(
	oldPublishers map[bus.PublisherEndpoint]struct{},
	freshPublishers []bus.PublisherEndpoint,
) ([]bus.PublisherEndpoint, []bus.PublisherEndpoint) {
	var endpointsForOpen, endpointsForClose []bus.PublisherEndpoint

	for _, freshEndpoint := range freshPublishers {
		if _, ok := oldPublishers[freshEndpoint]; !ok {
			endpointsForOpen = append(endpointsForOpen, freshEndpoint)
		}
	}

	for oldEndpoint := range oldPublishers {
		found := false
		for _, freshEndpoint := range freshPublishers {
			if freshEndpoint == oldEndpoint {
				found = true
				break
			}
		}
		if !found {
			endpointsForClose = append(endpointsForClose, oldEndpoint)
		}
	}

	return endpointsForOpen, endpointsForClose
}

func (s *Subscriber[Payload]) reader() {
	defer func() {
		close(s.payloadChan)
	}()

	var (
		msgBytes []byte
		err      error
	)

	for {
		msgBytes, err = s.zmqSocket.RecvBytes(0)
		if err != nil {
			if err == zmq4.ETERM {
				return
			}
			s.handleError(err)
			continue
		}

		delimiterIndex := bytes.IndexByte(msgBytes, bus.TopicAndPayloadDelimiter)

		payloadBytes := msgBytes[delimiterIndex+1:]

		payload := s.payloadFactory()

		err = proto.Unmarshal(payloadBytes, payload)
		if err != nil {
			s.handleError(err)
			continue
		}

		select {
		case s.payloadChan <- payload:
		case <-s.readerShutdownChan:
			return
		}
	}
}

func (s *Subscriber[Payload]) stopReader() error {
	for endpoint := range s.publishers {
		err := s.zmqSocket.Disconnect(string(endpoint))
		if err != nil {
			return err
		}
	}

	err := s.zmqContext.Term()
	if err != nil {
		return err
	}

	close(s.readerShutdownChan)

	return nil
}

func (s *Subscriber[Payload]) updatePublishers(endpoints []bus.PublisherEndpoint) {
	s.publishersMx.Lock()
	defer s.publishersMx.Unlock()

	endpointsForOpen, endpointsForClose := s.publishersDiff(s.publishers, endpoints)

	for _, endpoint := range endpointsForOpen {
		err := s.zmqSocket.Connect(string(endpoint))
		if err != nil {
			s.handleError(err)
		}
		s.publishers[endpoint] = struct{}{}
	}

	for _, endpoint := range endpointsForClose {
		err := s.zmqSocket.Disconnect(string(endpoint))
		if err != nil {
			s.handleError(err)
		}
		delete(s.publishers, endpoint)
	}
}

func (s *Subscriber[Payload]) handleError(err error) {
	if s.errorHandler != nil {
		s.errorHandler(err)
	}
}
