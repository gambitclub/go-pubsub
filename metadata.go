// =============================================================================
// Package pubsub - Event Metadata Utilities
//
// This file provides utility functions and a structure for managing event
// metadata within contexts, which is used by the PubSub client system.
// The eventMeta type contains various fields that store additional information
// about an event, such as brand, name, id, custom metadata (as key-value pairs),
// error details, an acknowledgment flag, and a timestamp.
//
// The utility functions included are:
//
// 1. WithContext:
//    Attaches an instance of eventMeta to a given context using a specific key ("metadata").
//    This allows event metadata to be passed along with the context across API boundaries.
//
// 2. metaFromContext:
//    Extracts the eventMeta instance from a context. If no metadata is present, it returns
//    an empty eventMeta instance. This function enables the retrieval of metadata that was
//    previously attached to the context.
//
// 3. contextFromRecv:
//    Creates a new context with event metadata constructed from a received PubSub message
//    (pb.Message). It takes message fields such as Brand, Name, Id, Metadata, Ack, and Timestamp,
//    and packages them into an eventMeta stored in a fresh background context.
//
// This mechanism is essential for associating custom metadata with events in a distributed system,
// enabling features such as tracking, logging, and conditional processing based on context data.
// =============================================================================

package pubsub

import (
	"context" // Provides context functionality to pass metadata along function calls.
	// Import the generated proto package under the alias "pb" to access message definitions.
	pb "github.com/gambitclub/go-pubsub/proto"
)

// eventMeta defines the structure of metadata associated with an event.
// It encapsulates several fields used to store supplementary event information.
type eventMeta struct {
	brand     string            // Brand associated with the event.
	name      string            // Name of the event.
	id        string            // Unique identifier for the event.
	meta      map[string]string // Custom metadata as key-value pairs.
	error     error             // Error information associated with the event.
	ack       bool              // Flag indicating whether an acknowledgment is expected.
	timestamp int64             // The timestamp when the event was created or received.
}

// WithContext attaches the eventMeta to a given context.
// If the eventMeta (m) is nil, the original context is returned unmodified.
// Otherwise, it returns a new context containing m under the key "metadata".
func (m *eventMeta) WithContext(ctx context.Context) context.Context {
	if m == nil {
		// If the eventMeta is nil, return the existing context without changes.
		return ctx
	}
	// Otherwise, attach the eventMeta to the context under the key "metadata".
	return context.WithValue(ctx, "metadata", m)
}

// metaFromContext extracts and returns the eventMeta from the provided context.
// If no metadata is found, or the retrieved value is not of type *eventMeta,
// the function returns an empty eventMeta instance.
func metaFromContext(ctx context.Context) *eventMeta {
	// Attempt to retrieve the value stored with key "metadata".
	md, ok := ctx.Value("metadata").(*eventMeta)
	if !ok {
		// If retrieval fails, return a pointer to an empty eventMeta.
		return &eventMeta{}
	}
	return md
}

// contextFromRecv creates a new context populated with eventMeta derived from a received message.
// It extracts necessary fields from the provided pb.Message and attaches them as metadata
// in a new context, using the key "metadata".
func contextFromRecv(msg *pb.Message) context.Context {
	// Construct a new eventMeta from the fields in the received message.
	return context.WithValue(context.Background(), "metadata", &eventMeta{
		brand:     msg.Brand,     // Set brand from the message.
		name:      msg.Name,      // Set event name from the message.
		id:        msg.Id,        // Set unique identifier from the message.
		meta:      msg.Metadata,  // Attach any additional metadata (key-value pairs).
		ack:       msg.Ack,       // Include the acknowledgment flag from the message.
		timestamp: msg.Timestamp, // Set the timestamp from the message.
	})
}
