package bus

import (
	"google.golang.org/protobuf/proto"
)

const (
	TopicAndPayloadDelimiter        byte = 0
	TopicPrefixAndEventKeyDelimiter byte = 1
	TopicPrefixFormat                    = "%s:v%d" + string(TopicPrefixAndEventKeyDelimiter)
)

type (
	EventFilter       string
	EventKey          string
	EventName         string
	EventVersion      int
	PublisherEndpoint string
)

type Publisher[Payload proto.Message] interface {
	Publish(eventKey EventKey, eventPayload Payload) error
	Stop() error
}

type PublishersRegistry interface {
	Register(eventName EventName, eventVersion EventVersion, host string, port int) (unregister func() error, err error)
	Watch(eventName EventName, eventVersion EventVersion, handler func([]PublisherEndpoint)) (stop func() error, err error)
}

type Subscriber[Payload proto.Message] interface {
	EventsChan() <-chan struct {
		EventKey EventKey
		Payload  Payload
	}
	Stop() error
	Subscribe(eventFilter EventFilter) error
	Unsubscribe(eventFilter EventFilter) error
}
