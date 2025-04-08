// =============================================================================
// Package pubsub - Callback Manager
//
// This file implements a callback management system for PubSub messages.
// Its primary responsibilities are:
//
// 1. Storing callback channels that are associated with specific events
//    and message IDs so that when a message is received, it can be delivered
//    to the appropriate callback channel.
// 2. Allowing callbacks to be registered and unregistered in a thread-safe
//    manner using a map protected by a read-write mutex (RWMutex).
// 3. Dispatching messages to registered callback channels asynchronously,
//    to ensure that message delivery does not block the main execution flow.
//
// The key used to store callbacks is constructed by concatenating the event
// name and the message ID (in the format "event:id"). This allows a unique
// identification of each callback based on the event it listens to and the
// specific message associated with it.
//
// Main functions in this file include:
//   - newCallbacks: Initializes and returns a new callback manager instance.
//   - Register: Adds a callback channel for a specific event and message ID.
//   - Unregister: Removes a callback channel so it no longer receives messages.
//   - Recv: Delivers a message to the corresponding registered callback channel,
//            if one exists.
// =============================================================================

package pubsub

import (
	"fmt" // Provides string formatting functionality, used for building unique keys.
	// Import the generated proto package under the alias "pb"
	pb "github.com/gambitclub/go-pubsub/proto"
	"sync" // Provides synchronization primitives (RWMutex) for thread-safe operations.
)

// newCallbacks creates and returns a new instance of the callback manager.
// It initializes the internal map that holds channels used for sending messages
// to registered callbacks. The map keys are composed of the event name and message ID.
func newCallbacks() *callbacks {
	return &callbacks{
		// Initialize the map to hold callbacks.
		// The key is a string composed from the event name and message ID (format: "event:id")
		// and the value is a channel that delivers messages of type *pb.Message.
		callbacks: make(map[string]chan *pb.Message),
	}
}

// callbacks is a struct that manages a set of callback channels for PubSub messages.
// It utilizes a read-write mutex to synchronize access to its internal map, ensuring
// thread-safe operations when callbacks are registered, unregistered, or retrieved.
type callbacks struct {
	// rw is a read/write mutex to allow safe concurrent access to the callbacks map.
	rw sync.RWMutex
	// callbacks is the internal map where the key is constructed from the event name
	// and message ID (in the format "event:id") and the value is a channel used to deliver
	// messages of type *pb.Message.
	callbacks map[string]chan *pb.Message
}

// Register adds a new callback channel for a specific event and id combination.
// The provided callback channel 'cb' will later be used to deliver messages that correspond
// to the specified event and message ID.
func (c *callbacks) Register(event, id string, cb chan *pb.Message) {
	// Construct a unique identifier for the callback using the event and id.
	cbId := fmt.Sprintf("%s:%s", event, id)
	// Acquire a write lock because we are going to modify the internal map.
	c.rw.Lock()
	// Ensure that the lock is released when the function exits.
	defer c.rw.Unlock()
	// Store the callback channel in the map under the generated key.
	c.callbacks[cbId] = cb
}

// Unregister removes a previously registered callback channel for a given event and id.
// This prevents any further messages from being sent to that callback channel.
func (c *callbacks) Unregister(event, id string) {
	// Build the unique key in the same way as the Register method.
	cbId := fmt.Sprintf("%s:%s", event, id)
	// Acquire a write lock as we are modifying the internal map.
	c.rw.Lock()
	// Ensure that the lock is released when the function exits.
	defer c.rw.Unlock()
	// Check if the callback exists for the given key; if yes, remove it from the map.
	if _, ok := c.callbacks[cbId]; ok {
		delete(c.callbacks, cbId)
	}
}

// Recv delivers a message to the registered callback channel if it exists.
// The callback key is built using the message's Name and Id fields, and if a matching
// callback is found, the message is delivered asynchronously.
func (c *callbacks) Recv(msg *pb.Message) {
	// Construct the lookup key using the message's Name and Id fields in the format "Name:Id".
	cbId := fmt.Sprintf("%s:%s", msg.Name, msg.Id)
	// Acquire a read lock because we are only reading from the internal map.
	c.rw.RLock()
	// Ensure that the lock is released after the function execution.
	defer c.rw.RUnlock()
	// Look up the callback channel for the given key.
	if cb, ok := c.callbacks[cbId]; ok {
		// Launch a goroutine to send the message into the callback channel asynchronously.
		// This approach prevents the caller from being blocked if the callback channel is busy.
		go func() {
			cb <- msg
		}()
	}
}
