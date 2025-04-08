// =============================================================================
// Package pubsub - PubSub Client Implementation
//
// This file implements a client for a PubSub system. The client facilitates:
//   - Publishing events with metadata and delivery guarantees.
//   - Subscribing to events with handler functions.
//   - Automatic message acknowledgment (ack) management and resending.
//
// Key responsibilities include:
//
// 1. Parsing configuration options provided via the Options struct.
// 2. Initializing internal channels for outgoing messages, incoming messages,
//    and acknowledgment handling.
// 3. Setting up internal components such as subscribers, callbacks, ackStore,
//    and shardStore (for managing multiple broker endpoints).
// 4. Enhancing context with metadata for each event (e.g., brand, error,
//    acknowledgment flags).
// 5. Providing methods to publish (Pub) events and subscribe (Sub) to events.
//
// The main entry point is the New(opts Options) function, which creates,
// configures, and returns a new Client instance based on provided options.
// =============================================================================

package pubsub

import (
	"context"                // For context propagation
	"fmt"                    // For formatted I/O and error messages
	"github.com/google/uuid" // For generating unique message IDs
	// Import the generated proto package under the alias "pb"
	pb "github.com/gambitclub/go-pubsub/proto"
	"strings" // For string parsing (e.g., host string)
	"time"    // For time-related functions (timeouts, durations)
)

// Options defines configuration options for the PubSub client.
// It specifies module identification, connection details, events to subscribe/publish,
// and the acknowledgment timeout.
type Options struct {
	// Brand - The brand of the module.
	Brand string
	// Module - The name of the module.
	Module string
	// Host - The host string representing one or more shard endpoints.
	// Expected format: "protocol.host:http.grpc, ..." (each shard separated by commas).
	Host string
	// Secret - The secret token for authentication.
	Secret string
	// Subscribers - The list of events that the module subscribes to.
	Subs []string
	// Publishers - The list of events that the module publishes.
	Pubs []string
	// AckTimeout - The timeout duration used for waiting for acknowledgments.
	AckTimeout time.Duration
}

// New creates and configures a new PubSub client instance based on provided Options.
//
// It performs the following steps:
//  1. Creates channels for outgoing, incoming, and ack-handled messages.
//  2. Initializes internal components: subscribers, callbacks, ackStore (with auto-resend),
//     and an empty shardStore.
//  3. Starts a goroutine to process incoming messages.
//  4. Parses the host string into individual shard definitions and adds them to the shardStore.
//  5. Returns the fully initialized Client.
func New(opts Options) *Client {
	// Create a channel for outgoing messages.
	out := make(chan *pb.Message, 1)
	// Create a channel for incoming messages.
	in := make(chan *pb.Message, 1)
	// Create a channel for messages that require acknowledgment.
	ackPool := make(chan *pb.Message, 1)

	// Initialize the client with its internal components.
	c := &Client{
		subscribers: newSubscribers(),                           // Initialize subscribers for event handlers.
		callbacks:   newCallbacks(),                             // Initialize callbacks for asynchronous responses.
		shard:       &shardStore{},                              // Initialize an empty shardStore for broker connections.
		out:         out,                                        // Assign the outgoing messages channel.
		ackPool:     ackPool,                                    // Assign the pool for messages pending acknowledgment.
		ackStore:    newAckStore(ackPool, out, opts.AckTimeout), // Create an ackStore with the given timeout.
	}

	// Start a goroutine to process incoming messages.
	go func() {
		// Continuously process messages from the incoming channel.
		for msg := range in {
			c.handleIncoming(msg)
		}
	}()

	// Parse the host string into individual shard definitions.
	// The host string might contain multiple shard definitions separated by commas.
	shards := strings.Split(opts.Host, ",")
	for _, s := range shards {
		s = strings.TrimSpace(s)
		if s == "" {
			panic("empty shard string")
		}
		// A shard string should be in the format: "protocol.host:port1.port2"
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			panic(fmt.Sprintf("invalid shard format: %s", s))
		}
		urls := parts[0]
		ports := parts[1]
		// Parse the URL part into protocol and host.
		parts = strings.Split(urls, ".")
		if len(parts) != 2 {
			panic(fmt.Sprintf("invalid url format: %s", urls))
		}
		protocol := parts[0]
		host := parts[1]
		// Parse the ports part into HTTP and gRPC port strings.
		parts = strings.Split(ports, ".")
		if len(parts) != 2 {
			panic(fmt.Sprintf("invalid ports format: %s", ports))
		}
		hHttp := parts[0]
		hGrpc := parts[1]
		// Create a shard with the parsed values and add it to the shardStore.
		c.shard.Add(shardAddOptions{
			Out:      out,         // Outgoing messages channel.
			In:       in,          // Incoming messages channel.
			Protocol: protocol,    // Protocol (e.g., "http").
			Host:     host,        // Host address.
			Http:     hHttp,       // HTTP port.
			Grpc:     hGrpc,       // gRPC port.
			Secret:   opts.Secret, // Authentication token.
			Brand:    opts.Brand,  // Brand identifier.
			Module:   opts.Module, // Module identifier.
			Subs:     opts.Subs,   // List of subscribed events.
			Pubs:     opts.Pubs,   // List of published events.
		})
	}
	return c
}

// Client represents the PubSub client instance, encapsulating all components needed
// for publishing, subscribing, acknowledgment handling, and shard management.
type Client struct {
	shard       *shardStore      // Manages connections to broker shards.
	subscribers *subscribers     // Manages event subscriptions and their handlers.
	callbacks   *callbacks       // Manages asynchronous callbacks for event responses.
	ackStore    *ackStore        // Manages automatic resend of messages awaiting acknowledgment.
	out         chan *pb.Message // Channel for outgoing messages.
	ackPool     chan *pb.Message // Channel for messages pending acknowledgment.
}

// handleIncoming processes each incoming message based on its name.
// If the message is an acknowledgment (".ack"), it instructs ackStore to stop resending;
// otherwise, it dispatches the message to both subscribers and callbacks.
func (c *Client) handleIncoming(msg *pb.Message) {
	switch msg.Name {
	case "_.ack":
		c.ackStore.Approve(msg) // Handle acknowledgment messages.
	default:
		c.subscribers.Recv(msg) // Deliver event message to subscribers.
		c.callbacks.Recv(msg)   // Deliver event message to registered callbacks.
	}
}

// Ack marks the context's metadata to enable delivery guarantees for subsequent events.
// It sets an "ack" flag in the metadata so that published messages are tracked for acknowledgment.
func (c *Client) Ack(ctx context.Context) context.Context {
	meta := metaFromContext(ctx)
	meta.ack = true // Enable acknowledgment requirement.
	return meta.WithContext(ctx)
}

// WithBrand attaches a brand identifier to the event metadata within the context.
func (c *Client) WithBrand(ctx context.Context, brand string) context.Context {
	meta := metaFromContext(ctx)
	meta.brand = brand // Set the brand field in metadata.
	return meta.WithContext(ctx)
}

// WithMetadata attaches additional custom metadata (key/value pairs) to the event context.
func (c *Client) WithMetadata(ctx context.Context, data map[string]string) context.Context {
	meta := metaFromContext(ctx)
	meta.meta = data // Set custom metadata.
	return meta.WithContext(ctx)
}

// Brand retrieves the brand information from the event metadata stored within the context.
func (c *Client) Brand(ctx context.Context) string {
	meta := metaFromContext(ctx)
	return meta.brand
}

// WithError attaches an error to the event's metadata within the context.
// This allows error information to be propagated along with the event.
func (c *Client) WithError(ctx context.Context, err error) context.Context {
	meta := metaFromContext(ctx)
	meta.error = err // Set the error in metadata.
	return meta.WithContext(ctx)
}

// Pub creates and configures a new publication (event) based on the provided context, event name, and payload.
// It extracts metadata from the context (such as brand, id, meta, error, and ack) and incorporates it
// into the event. A unique ID is generated if one is not already provided in the context.
func (c *Client) Pub(ctx context.Context, event string, payload []byte) *Pub {
	// Create a new publication with the basic parameters.
	p := &Pub{
		context:   ctx,         // Associate the original context.
		name:      event,       // Set the event name.
		payload:   payload,     // Set the event payload.
		out:       c.out,       // Outgoing message channel.
		ackPool:   c.ackPool,   // Channel for messages waiting for acknowledgment.
		callbacks: c.callbacks, // Manage callbacks for this event.
	}
	// Retrieve metadata from the context.
	meta := metaFromContext(ctx)
	// Use the brand from metadata if available.
	if meta.brand != "" {
		p.brand = meta.brand
	}
	// Set the event ID: use the one provided in metadata if present, else generate a new UUID.
	if meta.id != "" {
		p.id = meta.id
	} else {
		p.id = uuid.New().String()
	}
	// Attach custom metadata if present.
	if meta.meta != nil {
		p.meta = meta.meta
	}
	// Attach error details if present.
	if meta.error != nil {
		p.error = meta.error
	}
	// Set the acknowledgment flag if required.
	if meta.ack {
		p.ack = meta.ack
	}
	return p
}

// Sub subscribes to an event by registering a handler function to be invoked when the event is received.
// The provided function 'fn' will be called with the context and payload of the event.
// When the context is cancelled (or expires), the subscriber is automatically unregistered.
func (c *Client) Sub(ctx context.Context, event string, fn func(ctx context.Context, payload []byte)) {
	// Register the subscriber callback and obtain a registration ID.
	id := c.subscribers.Register(event, fn)
	// Spawn a goroutine to automatically unregister the subscriber when the context is done.
	go func() {
		<-ctx.Done() // Wait for the context to be cancelled.
		c.subscribers.Unregister(event, id)
	}()
}
