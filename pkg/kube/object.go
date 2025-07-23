package kube

import (
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Object is a wrapper for a generic Kubernetes object that provides
// a structured view of common metadata while keeping the rest of the
// object flexible. This is the payload sent to sinks for non-event resources.
type Object struct {
	// Common Kubernetes metadata that all objects share.
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// TypeMeta contains the API version and kind of the object.
	metav1.TypeMeta `json:",inline"`

	// Spec contains the resource-specific specification.
	// It's kept as a raw JSON message to remain generic.
	Spec json.RawMessage `json:"spec,omitempty"`

	// Status contains the resource-specific status.
	// It's also kept as a raw JSON message.
	Status json.RawMessage `json:"status,omitempty"`

	// EventType indicates whether the object was "added", "updated", or "deleted".
	EventType string `json:"eventType"`

	// ClusterName is the name of the cluster where the object was observed.
	ClusterName string `json:"clusterName,omitempty"`

	// ClusterEnvironment is the environment of the cluster (e.g., prod, staging).
	ClusterEnvironment string `json:"clusterEnvironment,omitempty"`
}

// ToJSON implements the Payload interface.
func (o *Object) ToJSON() []byte {
	// We can safely ignore the error here because the struct is designed
	// to be serializable.
	b, _ := json.Marshal(o)
	return b
}
