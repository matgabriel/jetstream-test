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
const eventCount = 10000 // number of events to send on the stream
const payloadSize = 1000 // payload size in bytes

func main() {

	nc, err := nats.Connect(natsUrl, nats.Timeout(time.Minute))
	panicIf(err)
	mgr, err := jsm.New(nc)
	panicIf(err)

	streamName := "someEvents"
	subject := "events"

	err = recreateStream(mgr, streamName, subject)

	id := uuid.NewV4().String()

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		panicIf(err)
	}

	start := time.Now()
	for i := 0; i < eventCount; i++ {
		_, err = nc.Request(subject, payload, 1*time.Second)
		panicIf(err)
	}

	fmt.Printf("\n%d messages sent in %d ms\n", eventCount, time.Since(start).Milliseconds())
	time.Sleep(1 * time.Second)

	start = time.Now()
	consumer, err := mgr.LoadOrNewConsumer(streamName, id, jsm.DurableName(id), jsm.AcknowledgeExplicit())
	panicIf(err)
	defer panicIf(consumer.Delete())

	i := 0
	for {
		m, err := consumer.NextMsg()
		panicIf(err)
		i++
		fmt.Println(i)
		if i == eventCount {
			break
		}
		err = m.Respond(nil)
		panicIf(err)
	}

	fmt.Printf("\n%d messages received in %d ms\n", eventCount, time.Since(start).Milliseconds())
}

// Delete and recreate the stream if it exists
func recreateStream(mgr *jsm.Manager, streamName string, subject string) error {
	stream, err := mgr.LoadOrNewStream(streamName, jsm.Subjects(subject), jsm.MaxAge(24*time.Hour), jsm.FileStorage())
	panicIf(err)
	panicIf(stream.Delete())
	stream, err = mgr.LoadOrNewStream(streamName, jsm.Subjects(subject), jsm.MaxAge(24*time.Hour), jsm.FileStorage())
	panicIf(err)
	return err
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}
