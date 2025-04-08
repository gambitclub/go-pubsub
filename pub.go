// =============================================================================
// Package pubsub - Publisher Module
//
// This file defines the Pub type and its associated methods for publishing
// events in the PubSub system. The Pub structure encapsulates all information
// necessary to construct and send a message. It includes metadata such as ID,
// name, brand, additional metadata (key-value pairs), the payload, and error
// details. It also provides support for acknowledgments (auto-resend) via an
// ackPool channel and callback handling for response-based messaging.
//
// Main functionalities provided in this file:
//
// 1. Do():
//    - Constructs a pb.Message from the Pub instance's fields.
//    - Sends the message to the outgoing channel.
//    - If acknowledgment is enabled, it also sends the message to the ackPool.
//
// 2. Sub(event string):
//    - Sends the message and registers a callback to wait for a response on a
//      specified event.
//    - It blocks until either a response is received or the context is done.
//
// The helper function handleResponse processes the response message, checking for errors.
// =============================================================================

package pubsub

import (
	"context" // Provides context functionality for deadlines, cancellation, etc.
	"fmt"     // For formatted I/O, mainly used for error messages.
	// Import the generated proto package under the alias "pb"
	pb "github.com/gambitclub/go-pubsub/proto"
	"time" // For time operations such as timestamp generation.
)

// Pub represents a publication event that the client intends to send.
// It holds all necessary fields to build the outgoing message, as well as channels for
// sending messages, handling acknowledgments, and processing callbacks.
type Pub struct {
	id        string            // Unique identifier for the event. Generated if not provided.
	name      string            // The name of the event.
	meta      map[string]string // Additional metadata (key-value pairs).
	payload   []byte            // The event payload.
	brand     string            // The brand associated with the event.
	error     error             // Any error associated with the event.
	ack       bool              // Flag indicating if acknowledgment is required.
	out       chan *pb.Message  // Outgoing messages channel.
	ackPool   chan *pb.Message  // Channel for messages that require acknowledgment.
	callbacks *callbacks        // Callbacks manager for handling asynchronous responses.
	context   context.Context   // Context associated with the event.
}

// Do constructs and sends the event message.
// It builds a pb.Message from the data stored in Pub and sends it through the
// outgoing channel. If the message requires acknowledgment, it is also sent to
// the ackPool for tracking and possible resends.
func (p *Pub) Do() {
	// Build the message from the Pub instance's fields.
	msg := &pb.Message{
		Id:        p.id,                   // Set unique event ID.
		Name:      p.name,                 // Set event name.
		Brand:     p.brand,                // Set brand.
		Metadata:  p.meta,                 // Attach additional metadata.
		Payload:   p.payload,              // Attach payload data.
		Ack:       p.ack,                  // Set acknowledgment flag.
		Timestamp: time.Now().UnixMilli(), // Set the current timestamp (milliseconds).
	}

	// If an error exists, convert it to a string and set it in the message.
	if p.error != nil {
		msg.Error = p.error.Error()
	}

	// Send the constructed message on the outgoing channel.
	p.out <- msg
	// If acknowledgment is required, also send the message to the ackPool.
	if p.ack {
		p.ackPool <- msg
	}
}

// Sub publishes the event and waits for a response associated with a specific event.
// It registers a callback channel to receive the response and blocks until either the
// response is received or the context is canceled. The function returns the response payload
// or an error.
func (p *Pub) Sub(event string) ([]byte, error) {
	// Construct a message similarly to the Do() method.
	msg := &pb.Message{
		Id:        p.id,                   // Use existing event ID.
		Name:      p.name,                 // Use event name.
		Brand:     p.brand,                // Use brand.
		Metadata:  p.meta,                 // Attach additional metadata.
		Payload:   p.payload,              // Attach payload data.
		Error:     p.error.Error(),        // Attach error details (if any) as a string.
		Ack:       p.ack,                  // Include acknowledgment flag.
		Timestamp: time.Now().UnixMilli(), // Current timestamp in milliseconds.
	}

	// Create a new callback channel to wait for the response.
	cb := make(chan *pb.Message)
	// Register the callback associated with the specific event and message ID.
	p.callbacks.Register(event, msg.Id, cb)
	// Ensure that the callback is unregistered after receiving the response.
	defer p.callbacks.Unregister(event, msg.Id)

	// Publish the message by sending it through the outgoing channel.
	p.out <- msg
	// Retrieve the context used when publishing.
	ctx := p.context

	// Wait for a response from the callback channel or cancellation from the context.
	select {
	case res := <-cb:
		// Process and return the response received.
		return handleResponse(res)
	case <-ctx.Done():
		// Return an error if the context is canceled.
		return nil, ctx.Err()
	}
}

// handleResponse processes the received response message.
// If the response message contains an error, it returns that error;
// otherwise, it returns the payload of the message.
func handleResponse(res *pb.Message) ([]byte, error) {
	// Check if there is any error reported in the response.
	if res.Error != "" {
		return nil, fmt.Errorf(res.Error)
	}
	// Return the payload if there is no error.
	return res.Payload, nil
}
