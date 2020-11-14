package main

import (
	"fmt"
	"math/rand"
	"time"

	uuid "github.com/satori/go.uuid"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
)

const natsUrl = "localhost:4222"
const eventCount = 10000  // number of events to send on the stream
const payloadSize = 10000 // payload size in bytes

func main() {

	conn, err := nats.Connect(natsUrl, nats.Timeout(time.Minute))
	panicIf(err)

	streamName := "someEvents"
	subject := "events"

	// Delete and recreate the stream if it exists
	err = recreateStream(conn, streamName, subject)

	id := uuid.NewV4().String()
	deliverySubject := id

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		panicIf(err)
	}

	start := time.Now()
	for i := 0; i < eventCount; i++ {
		_, err = conn.Request(subject, payload, 1*time.Second)
		panicIf(err)
	}

	fmt.Printf("\n%d messages sent in %d ms\n", eventCount, time.Since(start).Milliseconds())
	time.Sleep(1 * time.Second)

	doneChan := make(chan struct{})

	i := 0
	sub, err := conn.Subscribe(deliverySubject, func(m *nats.Msg) {
		i++
		fmt.Println(i)
		if i == eventCount {
			doneChan <- struct{}{}
		}
		err := m.Respond(nil)
		if err != nil {
			fmt.Println(err)
		}
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		panicIf(sub.Unsubscribe())
	}()

	start = time.Now()
	consumer, err := jsm.LoadOrNewConsumer(streamName, id,
		jsm.ConsumerConnection(jsm.WithConnection(conn)),
		jsm.DeliverySubject(deliverySubject),
		jsm.AckWait(10*time.Second),
		jsm.AcknowledgeAll(),
	)
	panicIf(err)
	defer panicIf(consumer.Delete())

	<-doneChan
	fmt.Printf("\n%d messages received in %d ms\n", eventCount, time.Since(start).Milliseconds())
}

func recreateStream(conn *nats.Conn, streamName string, subject string) error {
	stream, err := jsm.LoadOrNewStream(streamName, jsm.StreamConnection(jsm.WithConnection(conn)), jsm.Subjects(subject))
	panicIf(err)
	panicIf(stream.Delete())
	stream, err = jsm.LoadOrNewStream(streamName, jsm.StreamConnection(jsm.WithConnection(conn)), jsm.Subjects(subject))
	panicIf(err)
	return err
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}
