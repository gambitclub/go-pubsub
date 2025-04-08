// =============================================================================
// Package pubsub - PubSub Client Test Suite
//
// This file contains tests for various components of the PubSub client
// implementation. The tests verify that the client behaves as expected
// in the following areas:
//
// 1. AckStore Functionality:
//      - TestAckStoreResendRepeated: Verifies that if a message does not receive
//        an acknowledgment, it is resent repeatedly at the specified timeout.
//      - TestAckStoreApprove: Ensures that if a message is acknowledged, it is not
//        resent further.
//
// 2. Callback Management:
//      - TestCallbacks: Ensures that when a callback is registered for a specific
//        event and ID, the corresponding channel is notified with the correct message.
//
// 3. Subscriber Functionality:
//      - TestSubscribers: Verifies that registered subscribers receive the correct
//        payload when an event occurs.
//
// 4. Publishing Events:
//      - TestPubDo: Checks that the Pub method correctly constructs a message with
//        metadata and sends it through the outgoing channel.
//
// 5. Token Generation:
//      - TestTokenGenerateHMAC: Confirms that the HMAC for token generation is consistent
//        and correct for identical inputs.
//
// 6. Context Metadata Handling:
//      - TestMetaFromContext: Validates that metadata is correctly extracted from contexts,
//        returning empty metadata if not set and the expected values when provided.
//
// Each test sets up the necessary channels and components and then simulates the conditions
// required for verifying that part of the system. Timeouts and goroutines are used to handle
// asynchronous behavior.
// =============================================================================

package pubsub

import (
	"context"                                  // For context management in tests.
	pb "github.com/gambitclub/go-pubsub/proto" // Import the generated proto package.
	"testing"                                  // For writing and running tests.
	"time"                                     // For timeout durations and sleep operations.
)

// TestAckStoreResendRepeated verifies that if a message awaiting acknowledgment
// is not approved, it will be resent repeatedly within the specified timeout interval.
func TestAckStoreResendRepeated(t *testing.T) {
	// Create a channel (pool) for messages that need acknowledgment.
	pool := make(chan *pb.Message, 1)
	// Create an outgoing channel with a sufficient buffer to capture multiple resends.
	out := make(chan *pb.Message, 10)
	ackTimeout := 50 * time.Millisecond
	// Initialize the ackStore; we ignore the returned instance because its side effects are sufficient.
	_ = newAckStore(pool, out, ackTimeout)

	// Construct a test message that requires acknowledgment.
	msg := &pb.Message{
		Id:      "msg1",
		Name:    "test",
		Payload: []byte("data"),
	}

	// Send the message to the pool so that it starts being tracked for ack.
	pool <- msg

	// Count the number of times the message is resent within 300 milliseconds.
	count := 0
	done := time.After(300 * time.Millisecond)
loop:
	for {
		select {
		// If a message is received from the out channel, verify its correctness.
		case resentMsg := <-out:
			if resentMsg.Id != "msg1" {
				t.Errorf("Expected message with Id 'msg1', got %s", resentMsg.Id)
			}
			count++
		// Break the loop if 300 milliseconds have elapsed.
		case <-done:
			break loop
		}
	}

	// Check that the message was resent at least twice.
	if count < 2 {
		t.Errorf("Expected at least 2 resends, got %d", count)
	}
}

// TestAckStoreApprove verifies that if a message is acknowledged,
// the ackStore stops further resending of that message.
func TestAckStoreApprove(t *testing.T) {
	// Create the pool and out channels for a message.
	pool := make(chan *pb.Message, 1)
	out := make(chan *pb.Message, 1)
	ackTimeout := 50 * time.Millisecond
	// Initialize the ackStore and store its reference for later use.
	as := newAckStore(pool, out, ackTimeout)

	// Construct a test message.
	msg := &pb.Message{
		Id:      "msg2",
		Name:    "test",
		Payload: []byte("data"),
	}

	// Send the test message into the ack pool for tracking.
	pool <- msg
	// Wait briefly to allow the ackStore to process and register the message.
	time.Sleep(10 * time.Millisecond)

	// Lock the ackStore and retrieve the ack for our message.
	as.rw.Lock()
	a, ok := as.acks[msg.Id]
	if !ok {
		as.rw.Unlock()
		t.Fatal("Message not registered in ackStore")
	}
	// If the stop channel is not set, initialize it (this is a safeguard).
	if a.stop == nil {
		a.stop = make(chan struct{}, 1)
	}
	as.rw.Unlock()

	// Call Approve to signal that the message has been acknowledged.
	as.Approve(msg)

	// After waiting longer than the ackTimeout, verify that no further resends occur.
	select {
	case resentMsg := <-out:
		t.Errorf("Message was resent after approval: %v", resentMsg)
	case <-time.After(100 * time.Millisecond):
		// Success: no message was resent.
	}
}

// TestCallbacks verifies that registered callbacks are correctly notified when a relevant
// message is received. It ensures that the callback channel is triggered with the appropriate message.
func TestCallbacks(t *testing.T) {
	// Initialize a new callbacks manager.
	cbs := newCallbacks()
	// Create a test channel to act as the callback.
	testChan := make(chan *pb.Message, 1)
	// Register a callback for event "eventCb" with identifier "123".
	cbs.Register("eventCb", "123", testChan)
	// Create a test message that should trigger the callback.
	msg := &pb.Message{
		Name: "eventCb",
		Id:   "123",
	}

	// Call Recv to dispatch the message.
	cbs.Recv(msg)

	// Wait for the callback to receive the message.
	select {
	case m := <-testChan:
		if m.Name != "eventCb" {
			t.Errorf("Expected event 'eventCb', got: %s", m.Name)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Callback was not triggered")
	}
}

// TestSubscribers verifies that subscribers receive the correct payload when an event occurs.
// It registers a subscriber and checks that its callback function is called with the proper data.
func TestSubscribers(t *testing.T) {
	// Create a new subscribers manager.
	subs := newSubscribers()
	// Create a channel to signal that the subscriber's callback was triggered.
	done := make(chan bool, 1)
	// Register a subscriber for event "eventX" with a callback function.
	subs.Register("eventX", func(ctx context.Context, payload []byte) {
		if string(payload) != "testdata" {
			t.Errorf("Unexpected payload: %s", payload)
		}
		// Signal that the callback was successfully executed.
		done <- true
	})
	// Create a test message with event "eventX".
	msg := &pb.Message{
		Name:    "eventX",
		Payload: []byte("testdata"),
	}
	// Dispatch the message to the subscribers manager.
	subs.Recv(msg)

	// Wait for confirmation that the subscriber callback was triggered.
	select {
	case <-done:
		// Success: callback executed.
	case <-time.After(100 * time.Millisecond):
		t.Error("Subscriber callback was not triggered")
	}
}

// TestPubDo verifies that the Pub method in the client constructs and sends a message correctly.
// It creates a client, publishes an event, and checks that the outgoing channel receives the message
// with the correct properties.
func TestPubDo(t *testing.T) {
	// Define options for creating a client instance.
	opts := Options{
		Brand:  "testBrand",
		Module: "testModule",
		// The host string should be in the expected format, e.g., "http.localhost:80.81".
		Host:       "http.localhost:80.81",
		Secret:     "secret",
		Subs:       []string{"event1"},
		Pubs:       []string{"event2"},
		AckTimeout: 50 * time.Millisecond,
	}
	// Create a new client with the defined options.
	c := New(opts)
	// Create a background context.
	ctx := context.Background()
	// Use the client's Pub method to create a new publication (event).
	pub := c.Pub(ctx, "eventTest", []byte("payload"))
	// Trigger the actual sending of the event.
	pub.Do()

	// Check that a message is received on the outgoing channel with the expected data.
	select {
	case msg := <-c.out:
		if msg.Name != "eventTest" {
			t.Errorf("Expected 'eventTest', got %s", msg.Name)
		}
		if string(msg.Payload) != "payload" {
			t.Errorf("Expected payload 'payload', got %s", string(msg.Payload))
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Message was not sent to the outgoing channel")
	}
}

// TestTokenGenerateHMAC verifies the HMAC token generation mechanism.
// It checks that a generated HMAC signature is non-empty, and that subsequent invocations
// with the same parameters produce an identical signature.
func TestTokenGenerateHMAC(t *testing.T) {
	// Create an instance of token with known values.
	tk := token{
		Brand:     "brand",
		Module:    "module",
		Subs:      []string{"sub1", "sub2"},
		Pubs:      []string{"pub1", "pub2"},
		Timestamp: "timestamp",
	}
	// Generate the HMAC signature.
	sig1, err := tk.GenerateHMAC("secret")
	if err != nil {
		t.Fatalf("Error generating HMAC: %v", err)
	}
	if sig1 == "" {
		t.Error("Generated HMAC signature is empty")
	}
	// Generate the signature again with the same inputs.
	sig2, err := tk.GenerateHMAC("secret")
	if err != nil {
		t.Fatalf("Error generating HMAC on second attempt: %v", err)
	}
	// Verify that both signatures match.
	if sig1 != sig2 {
		t.Errorf("Signatures do not match: %s and %s", sig1, sig2)
	}
}

// TestMetaFromContext verifies that event metadata is correctly extracted from a context.
// It checks that the metadata is empty when not set and correctly populated when provided.
func TestMetaFromContext(t *testing.T) {
	// Create a background context that does not contain metadata.
	ctx := context.Background()
	meta := metaFromContext(ctx)
	if meta.brand != "" || meta.id != "" {
		t.Error("Expected empty metadata in a default context")
	}

	// Create an eventMeta instance with specific values.
	original := &eventMeta{
		brand: "testbrand",
		id:    "testid",
	}
	// Attach the metadata to a new context.
	ctx2 := context.WithValue(context.Background(), "metadata", original)
	meta2 := metaFromContext(ctx2)
	// Verify that the metadata is correctly retrieved from the context.
	if meta2.brand != "testbrand" || meta2.id != "testid" {
		t.Error("Metadata was not correctly extracted from context")
	}
}
