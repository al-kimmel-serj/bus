package consul_publishers_registry

import (
	"fmt"
	"regexp"

	"github.com/al-kimmel-serj/zmq-bus"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
)

const (
	ServiceName = "bus"
)

var (
	serviceIDForbiddenCharsRegEx = regexp.MustCompile("[^0-9A-Za-z-]+")
)

type Client struct{}

func New() *Client {
	return &Client{}
}

func (c *Client) Register(eventName bus.EventName, eventVersion bus.EventVersion, host string, port int) (func() error, error) {
	consulClient, err := api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, err
	}

	serviceID := c.generateServiceID(host, port)
	err = consulClient.Agent().ServiceRegisterOpts(&api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    ServiceName,
		Address: host,
		Port:    port,
		Tags: []string{
			fmt.Sprintf("%s:v%d", eventName, eventVersion),
		},
	}, api.ServiceRegisterOpts{
		ReplaceExistingChecks: true,
	})
	if err != nil {
		return nil, err
	}

	return func() error {
		return consulClient.Agent().ServiceDeregister(serviceID)
	}, nil
}

func (c *Client) Watch(eventName bus.EventName, eventVersion bus.EventVersion, handler func([]bus.PublisherEndpoint)) (func() error, error) {
	plan, err := watch.Parse(map[string]interface{}{
		"type":        "service",
		"service":     ServiceName,
		"tag":         []string{fmt.Sprintf("%s:v%d", eventName, eventVersion)},
		"passingonly": true,
	})
	if err != nil {
		return nil, err
	}

	var lastIndex uint64
	plan.Handler = func(index uint64, result interface{}) {
		if lastIndex >= index {
			return
		}
		lastIndex = index

		serviceEntries := result.([]*api.ServiceEntry)

		var endpoints []bus.PublisherEndpoint
		for _, entry := range serviceEntries {
			endpoints = append(
				endpoints,
				bus.PublisherEndpoint(fmt.Sprintf("tcp://%s:%d", entry.Service.Address, entry.Service.Port)),
			)
		}

		handler(endpoints)
	}

	go func() {
		consulConfig := api.DefaultConfig()
		err = plan.Run(consulConfig.Address)
		if err != nil {
			// @todo handle error
		}
	}()

	return func() error {
		plan.Stop()
		return nil
	}, nil
}

func (c *Client) generateServiceID(host string, port int) string {
	return serviceIDForbiddenCharsRegEx.ReplaceAllString(fmt.Sprintf("bus-%s-%d", host, port), "-")
}
