// =============================================================================
// Package pubsub - Subscriber Manager
//
// This file defines the mechanism to register, unregister, and notify subscribers
// when a PubSub message is received. Subscribers are stored in a thread-safe map
// keyed by the event name. For each event, there is a subscriber instance that maintains
// a list of handlers. Each handler is associated with a unique identifier (UUID),
// and its callback function receives the context and the message payload.
//
// Main functionalities provided in this file:
//
// 1. newSubscribers:
//    - Initializes and returns a new subscribers manager.
//
// 2. Recv (on subscribers):
//    - For a given message, it identifies the corresponding subscriber registered
//      with the event name (from msg.Name) and then calls its Recv method.
//
// 3. Register (on subscribers):
//    - Registers a new handler function for a specific event. It returns a unique ID
//      for that handler, which can later be used for unregistration.
//
// 4. Unregister (on subscribers):
//    - Unregisters a handler for a given event based on its unique ID.
//      If a subscriber has no more handlers, the subscriber entry is removed from the map.
//
// 5. subscriber type and its methods:
//    - Register: Adds a new handler (with a unique id) to the subscriber's list.
//    - Unregister: Removes a handler with the given id from the subscriber's list,
//      and returns a boolean indicating whether the subscriber now has no handlers.
//    - Recv: Invokes all registered handler callbacks concurrently with the message payload,
//      using a new context created from the received message.
//
// =============================================================================

package pubsub

import (
	"context"                // For context propagation.
	"github.com/google/uuid" // For generating unique identifiers.
	// Import the generated proto package under the alias "pb"
	pb "github.com/gambitclub/go-pubsub/proto"
	"sync" // For synchronizing access using RWMutex.
)

// newSubscribers initializes and returns a new subscribers manager.
// It creates and returns an instance of subscribers with an empty map that
// will be used to store subscribers keyed by event name.
func newSubscribers() *subscribers {
	return &subscribers{
		// Initialize the map which will hold subscriber objects.
		subscribers: make(map[string]*subscriber),
	}
}

// subscribers manages a collection of subscriber instances.
// Each key in the subscribers map corresponds to an event name, and the value
// is a pointer to a subscriber which holds one or more handlers (callbacks)
// registered for that event.
type subscribers struct {
	rw          sync.RWMutex           // RWMutex to protect concurrent access.
	subscribers map[string]*subscriber // Map of event name -> subscriber instance.
}

// Recv is called to dispatch a received message to the appropriate subscriber's handlers.
// It looks up the subscriber based on the message Name and, if found, calls its Recv method.
// The context is created from the received message using contextFromRecv.
func (c *subscribers) Recv(msg *pb.Message) {
	// Acquire read lock to safely access the subscribers map.
	c.rw.RLock()
	// Lookup a subscriber for the event name (msg.Name).
	cb, ok := c.subscribers[msg.Name]
	c.rw.RUnlock()
	// If no subscriber is registered for this event, do nothing.
	if !ok {
		return
	}
	// Create a context from the received message that contains event metadata.
	ctx := contextFromRecv(msg)
	// Invoke the subscriber's Recv method to trigger all its registered handlers.
	cb.Recv(ctx, msg)
}

// Register adds a new handler function for a specified event name.
// It returns a unique identifier for the registered handler, which can be used later for unregistering.
func (c *subscribers) Register(name string, fn func(ctx context.Context, payload []byte)) string {
	// Attempt to retrieve an existing subscriber for the event.
	c.rw.RLock()
	cb, ok := c.subscribers[name]
	c.rw.RUnlock()
	// If no subscriber exists yet for this event, create a new one.
	if !ok {
		cb = &subscriber{}
		// Lock for writing and assign the new subscriber to the map.
		c.rw.Lock()
		c.subscribers[name] = cb
		c.rw.Unlock()
	}
	// Generate a unique ID for this handler.
	id := uuid.New().String()
	// Register the handler function with the generated ID.
	cb.Register(id, fn)
	// Return the unique handler ID.
	return id
}

// Unregister removes a handler associated with an event based on its unique identifier.
// If, after removal, no handlers remain for that event, the subscriber entry is removed from the map.
func (c *subscribers) Unregister(name, id string) {
	// Retrieve the subscriber for the given event.
	c.rw.RLock()
	cb, ok := c.subscribers[name]
	c.rw.RUnlock()
	// If no subscriber exists for this event, there is nothing to unregister.
	if !ok {
		return
	}
	// Unregister the handler within the subscriber.
	ok = cb.Unregister(id)
	// If the subscriber has no remaining handlers, remove the subscriber from the map.
	if ok {
		c.rw.Lock()
		delete(c.subscribers, name)
		c.rw.Unlock()
	}
}

// subscriber represents a collection of handler functions (callbacks) for a specific event.
// Each subscriber maintains a list of handlers identified by a unique string.
type subscriber struct {
	rw       sync.RWMutex // RWMutex to protect concurrent access to handlers.
	handlers []struct { // Slice of handlers. Each handler is an anonymous struct.
		id string                                    // Unique identifier for the handler.
		cb func(ctx context.Context, payload []byte) // The callback function that processes the payload.
	}
}

// Register adds a new handler to the subscriber's list, identified by the given id.
func (s *subscriber) Register(id string, fn func(ctx context.Context, payload []byte)) {
	s.rw.Lock()
	defer s.rw.Unlock()
	// Append a new handler object with the provided id and function.
	s.handlers = append(s.handlers, struct {
		id string
		cb func(ctx context.Context, payload []byte)
	}{
		id: id,
		cb: fn,
	})
}

// Unregister removes the handler with the specified id from the subscriber.
// It returns true if the subscriber ends up with no handlers remaining.
func (s *subscriber) Unregister(id string) bool {
	s.rw.Lock()
	defer s.rw.Unlock()
	var handlers []struct {
		id string
		cb func(ctx context.Context, payload []byte)
	}
	// Loop over the current handlers and keep only those that do not match the provided id.
	for _, handler := range s.handlers {
		if handler.id == id {
			continue
		}
		handlers = append(handlers, handler)
	}
	// Assign the updated list of handlers.
	s.handlers = handlers
	// Return true if there are no more handlers left.
	return len(s.handlers) == 0
}

// Recv triggers all registered handler callbacks concurrently with the provided message payload.
// A new goroutine is spawned for each handler to avoid blocking other operations.
func (s *subscriber) Recv(ctx context.Context, msg *pb.Message) {
	go func() {
		// Acquire a read lock to safely iterate over the handlers.
		s.rw.RLock()
		defer s.rw.RUnlock()
		// For each handler, launch a new goroutine to call its callback with the context and message payload.
		for _, handler := range s.handlers {
			go handler.cb(ctx, msg.Payload)
		}
	}()
}
