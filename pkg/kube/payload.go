package kube

// Payload is the interface for all data that can be sent to a sink.
type Payload interface {
	// ToJSON returns a JSON representation of the payload.
	ToJSON() []byte
}
