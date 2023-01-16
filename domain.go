package bus

import (
	"google.golang.org/protobuf/proto"
)

const (
	TopicAndPayloadDelimiter           byte = 0
	TopicPrefixAndEventFilterDelimiter byte = ':'
	TopicPrefixFormat                       = "%s:v%d" + string(TopicPrefixAndEventFilterDelimiter)
)

type (
	EventFilter       string
	EventName         string
	EventVersion      int
	PublisherEndpoint string
)

type Publisher[Payload proto.Message] interface {
	Publish(eventFilter EventFilter, eventPayload Payload) error
	Stop() error
}

type PublishersRegistry interface {
	Register(eventName EventName, eventVersion EventVersion, host string, port int) (unregister func() error, err error)
	Watch(eventName EventName, eventVersion EventVersion, handler func([]PublisherEndpoint)) (stop func() error, err error)
}

type Subscriber[Payload proto.Message] interface {
	EventsChan() <-chan struct {
		EventFilter EventFilter
		Payload     Payload
	}
	Stop() error
	Subscribe(eventFilter EventFilter) error
	Unsubscribe(eventFilter EventFilter) error
}
