package zmq_examples

import (
	"fmt"
	"time"

	"github.com/al-kimmel-serj/bus"
	"github.com/al-kimmel-serj/bus/stub_publishers_registry"
	"github.com/al-kimmel-serj/bus/zmq_examples/events"
	"github.com/al-kimmel-serj/bus/zmq_publisher"
	"github.com/al-kimmel-serj/bus/zmq_subscriber"
)

func Example() {
	registry := stub_publishers_registry.New([]bus.PublisherEndpoint{
		"tcp://127.0.0.1:5555",
	})

	var (
		err        error
		publisher  bus.Publisher[*events.Hello]
		subscriber bus.Subscriber[*events.Hello]
	)

	publisher, err = zmq_publisher.New[*events.Hello](
		"127.0.0.1",
		5555,
		"hello",
		1,
		registry,
	)
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	subscriber, err = zmq_subscriber.New[*events.Hello]("hello", 1, registry, func() *events.Hello {
		return new(events.Hello)
	}, nil)
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	err = subscriber.Subscribe("gopher")
	if err != nil {
		panic(err)
	}

	time.Sleep(100 * time.Millisecond)

	sentHello := events.Hello{
		Message: "Hello, Gopher.",
	}

	err = publisher.Publish("gopher", &sentHello)
	if err != nil {
		panic(err)
	}

	acceptedHello := <-subscriber.PayloadChan()
	fmt.Println(acceptedHello.Message)

	err = subscriber.Stop()
	if err != nil {
		panic(err)
	}

	err = publisher.Stop()
	if err != nil {
		panic(err)
	}

	// Output: Hello, Gopher.
}
