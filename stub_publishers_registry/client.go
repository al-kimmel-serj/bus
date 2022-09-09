package stub_publishers_registry

import (
	"github.com/al-kimmel-serj/zmq-bus"
)

type Client struct {
	endpoints []bus.PublisherEndpoint
}

func (c *Client) Register(_ bus.EventName, _ bus.EventVersion, _ string, _ int) (func() error, error) {
	return func() error {
		return nil
	}, nil
}

func (c *Client) Watch(_ bus.EventName, _ bus.EventVersion, handler func([]bus.PublisherEndpoint)) (func() error, error) {
	go handler(c.endpoints)
	return func() error {
		return nil
	}, nil
}

func New(endpoints []bus.PublisherEndpoint) *Client {
	return &Client{
		endpoints: endpoints,
	}
}
