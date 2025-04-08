// =============================================================================
// Package pubsub - Acknowledgment Management Module
//
// This file implements an acknowledgment mechanism for PubSub messages.
// Its main purpose is to monitor messages that require an acknowledgment and
// automatically resend them until an acknowledgment ("ack") is received.
//
// Main components:
//   - ackStore: Holds a mapping of message IDs to individual acknowledgment (ack)
//               handlers and manages their timers.
//   - ack: Represents a single message's acknowledgment tracker.
//          It starts a timer; if the timer expires before the ack is received,
//          the associated message is resent and the timer is reset.
//   - resend mechanism: A helper function that resends messages via a provided output
//                       channel in a non-blocking manner.
//
// Thread safety is ensured by using RWMutex in the ackStore while reading and modifying
// the internal map of active acks. Each message being tracked runs its own goroutine to handle
// timing for resending, and can be stopped when an ack is received.
//
// =============================================================================

package pubsub

import (
	// Import the generated proto package under the alias "pb".
	pb "github.com/gambitclub/go-pubsub/proto"
	"sync" // For synchronization (mutexes)
	"time" // For time-related functions (timers, durations)
)

// newAckStore initializes and returns an instance of ackStore.
// The ackStore is responsible for managing message acknowledgments and automatic resends.
// It takes two channels as parameters:
//   - pool: A channel delivering messages that need an acknowledgment.
//   - out: A channel to which messages are resent if an ack is not received within the timeout.
//
// The timeout parameter defines how long to wait before resending a message.
func newAckStore(pool, out chan *pb.Message, timeout time.Duration) *ackStore {
	// If no timeout is provided, default to 5 seconds.
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	// Create a new ackStore object.
	s := &ackStore{
		pool:    pool,                  // Channel from which messages to ack are received.
		out:     out,                   // Channel used to send messages for resend.
		timeout: timeout,               // Timeout period for triggering resends.
		acks:    make(map[string]*ack), // Map to store active ack timers by message ID.
	}
	// Start watching the pool channel for incoming messages that require acknowledgment.
	s.watch()
	return s
}

// ackStore holds data necessary for managing acknowledgments.
// It maintains a map of active ack trackers and uses an RWMutex to allow safe concurrent access.
type ackStore struct {
	pool    chan *pb.Message // Channel to receive new messages that require an ack.
	out     chan *pb.Message // Channel to send messages that must be resent.
	rw      sync.RWMutex     // Mutex to control concurrent access to the acks map.
	acks    map[string]*ack  // Map from message ID to its associated ack timer.
	timeout time.Duration    // Timeout duration used to trigger a message resend.
}

// resend wraps the actual sending of the message to the out channel in a goroutine.
// This ensures that the timer goroutine is not blocked by the potentially blocking send.
func (s *ackStore) resend(msg *pb.Message) {
	go func() {
		s.out <- msg // Resend the message by sending it to the output channel.
	}()
}

// watch listens on the pool channel for messages that require tracking for acknowledgment.
// For each incoming message, an ack is created, its timer is started, and it is stored in the ackStore.
func (s *ackStore) watch() {
	go func() {
		// Continuously read messages from the pool channel.
		for msg := range s.pool {
			// Create a new ack for the incoming message.
			a := &ack{
				msg: msg, // Associate the message with the ack.
			}
			// Initialize the timer for this message acknowledgment.
			// If the timer expires before an acknowledgment is received,
			// the resend function will be called to resend the message.
			a.timerInit(s.timeout, s.resend)
			// Lock the map before adding the new ack.
			s.rw.Lock()
			// Store the ack object with the message ID as the key.
			s.acks[msg.Id] = a
			s.rw.Unlock()
		}
	}()
}

// Approve is called when an acknowledgment for a message is received.
// It stops the timer for the corresponding message and removes the ack entry from the store.
func (s *ackStore) Approve(msg *pb.Message) {
	s.rw.Lock()                        // Acquire write lock to modify the acks map.
	defer s.rw.Unlock()                // Defer unlocking until function exit.
	if ack, ok := s.acks[msg.Id]; ok { // Check if an ack entry exists for the given message.
		ack.timerStop()        // Stop the timer associated with the message.
		delete(s.acks, msg.Id) // Remove the ack entry from the map.
	}
}

// ack represents the acknowledgment tracking for a single message.
// It holds a pointer to the message and a stop channel to signal its timer goroutine.
type ack struct {
	msg  *pb.Message   // The message awaiting acknowledgment.
	stop chan struct{} // Channel used to signal the timer to stop.
}

// timerInit starts a timer for this ack. If the timer expires before a signal is received on the stop channel,
// the resend function is called to resend the message, and the timer is reset.
// This function runs in its own goroutine.
func (a *ack) timerInit(timeout time.Duration, resend func(msg *pb.Message)) {
	// Initialize the stop channel used to signal cancellation of the timer.
	a.stop = make(chan struct{})
	go func() {
		// Create a new timer that will trigger after the specified timeout.
		timer := time.NewTimer(timeout)
		// Ensure that the timer is stopped when the goroutine exits to free resources.
		defer timer.Stop()
		for {
			select {
			// When the timer expires:
			case <-timer.C:
				resend(a.msg)        // Resend the message.
				timer.Reset(timeout) // Reset the timer for the next cycle.
			// If a signal is received on the stop channel, exit the goroutine.
			case <-a.stop:
				return
			}
		}
	}()
}

// timerStop signals the timer goroutine to stop by sending an empty struct{} on the stop channel.
// The send is done in a separate goroutine to avoid potential blocking issues.
func (a *ack) timerStop() {
	go func() {
		a.stop <- struct{}{} // Signal the timer goroutine to exit.
	}()
}
