module github.com/matgabriel/jetstream-test

go 1.15

require (
	github.com/nats-io/jsm.go v0.0.19
	github.com/nats-io/nats.go v1.10.1-0.20201111151633-9e1f4a0d80d8
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/satori/go.uuid v1.2.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)

//replace github.com/nats-io/nats.go => ../nats.go

//replace github.com/nats-io/jsm.go => ../jsm.go
