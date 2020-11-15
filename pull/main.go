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

	nc, err := nats.Connect(natsUrl, nats.Timeout(time.Minute))
	panicIf(err)
	mgr, err := jsm.New(nc)
	panicIf(err)

	streamName := "someEvents"
	subject := "events"

	//names, err := mgr.ConsumerNames(streamName)
	//panicIf(err)
	//fmt.Println("consumer names:")
	//for _, n := range names {
	//	fmt.Println(n)
	//}

	err = recreateStream(mgr, streamName, subject)

	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		panic(err)
	}

	start := time.Now()
	for i := 0; i < eventCount; i++ {
		_, err = nc.Request(subject, payload, 1*time.Second)
		panicIf(err)
	}

	fmt.Printf("\n%d messages sent in %d ms\n", eventCount, time.Since(start).Milliseconds())
	time.Sleep(1 * time.Second)

	consumerId := uuid.NewV4().String()
	start = time.Now()
	consumer, err := mgr.LoadOrNewConsumer(streamName, consumerId,
		jsm.DurableName(consumerId),
		jsm.AcknowledgeExplicit(),
		jsm.ReplayInstantly())
	panicIf(err)
	defer func() { panicIf(consumer.Delete()) }()

	i := 0
	for {
		m, err := consumer.NextMsg()
		panicIf(err)
		i++

		md, err := m.JetStreamMetaData()
		panicIf(err)

		fmt.Println(i, "metadata", md.ConsumerSeq, "/", md.StreamSeq, "/", md.Pending)
		if i == eventCount {
			break
		}
		err = m.Ack()
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
	fmt.Println("Stream created:", stream.Name())
	return err
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}
