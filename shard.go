// =============================================================================
// Package pubsub - Shard and gRPC Connection Management
//
// This file implements the shardStore and shard types which are used to manage
// multiple shard connections to a broker in a PubSub system. It provides functionality
// to add shards based on configuration options, establish secure gRPC connections using TLS,
// and handle reconnection and error management.
//
// Key components in this file:
//
// 1. shardStore:
//    - Maintains a map of shard instances (keyed by host).
//    - Provides the Add method to add a new shard based on shardAddOptions.
//
// 2. shard:
//    - Represents a single shard (broker endpoint) with its own configuration,
//      including protocol, host, ports, and security parameters.
//    - Implements the observe method to establish and monitor the gRPC connection,
//      handle incoming messages, and send outgoing messages.
//    - Contains methods for getting the connection (getConnect),
//      intercepting gRPC streams (interceptor),
//      handling errors (handleError), and reconnecting (reconnect).
//
// 3. wrappedStream:
//    - A simple wrapper around grpc.ClientStream to allow customization if necessary.
//
// The design supports automatic reconnection with exponential backoff
// and uses TLS for secure connections. Error handling includes panics for fatal errors
// and reconnection attempts for recoverable errors.
// =============================================================================

package pubsub

import (
	"context"                                  // For managing request contexts.
	"crypto/tls"                               // For TLS configuration.
	"crypto/x509"                              // For certificate handling.
	"fmt"                                      // For formatted I/O.
	pb "github.com/gambitclub/go-pubsub/proto" // Import generated proto package.
	"google.golang.org/grpc"                   // For gRPC functionalities.
	"google.golang.org/grpc/codes"             // For gRPC error codes.
	"google.golang.org/grpc/credentials"       // For creating TLS credentials.
	"google.golang.org/grpc/metadata"          // For gRPC metadata.
	"google.golang.org/grpc/status"            // For constructing gRPC error statuses.
	"io"                                       // For I/O operations.
	"net/http"                                 // For HTTP client operations.
	"strings"                                  // For string manipulation (e.g., splitting host strings).
	"sync"                                     // For synchronizing access (mutexes).
	"time"                                     // For time-related functions (timeouts, sleep, timestamps).
)

// shardStore is responsible for managing multiple shard instances.
// It uses a RWMutex to allow safe concurrent access to the internal shards map.
type shardStore struct {
	rw     sync.RWMutex      // Mutex for safe concurrent access.
	shards map[string]*shard // Map of shard instances, keyed by host.
}

// shardAddOptions defines configuration options for adding a new shard.
// It encapsulates all configuration details required to set up a connection,
// such as channels, host information, protocols, token, and event lists.
type shardAddOptions struct {
	Out      chan *pb.Message // Outgoing messages channel.
	In       chan *pb.Message // Incoming messages channel.
	Host     string           // Host part of the shard.
	Protocol string           // Protocol used (e.g., "http").
	Http     string           // HTTP port.
	Grpc     string           // gRPC port.
	Secret   string           // Authentication token.
	OnlyRoot bool             // Flag indicating if only the root should be used.
	Brand    string           // Brand identifier.
	Module   string           // Module name.
	Subs     []string         // List of subscribed events.
	Pubs     []string         // List of published events.
}

// Add adds a new shard to the shardStore based on the provided options.
// It initializes a new shard instance, stores it in the shards map, and
// starts observing the shard connection in a separate goroutine.
func (s *shardStore) Add(opts shardAddOptions) {
	// Initialize the shards map if it is nil.
	if s.shards == nil {
		s.shards = make(map[string]*shard)
	}

	// Create a new shard with the provided configuration.
	sh := &shard{
		protocol: opts.Protocol,       // Set protocol (e.g., "http").
		host:     opts.Host,           // Set host.
		http:     opts.Http,           // Set HTTP port.
		grpc:     opts.Grpc,           // Set gRPC port.
		out:      opts.Out,            // Outgoing messages channel.
		in:       opts.In,             // Incoming messages channel.
		secret:   opts.Secret,         // Authentication token.
		onlyRoot: opts.OnlyRoot,       // Only root flag.
		brand:    opts.Brand,          // Brand identifier.
		module:   opts.Module,         // Module name.
		subs:     opts.Subs,           // Subscribed events.
		pubs:     opts.Pubs,           // Published events.
		start:    make(chan struct{}), // Channel to signal start.
		pause:    make(chan struct{}), // Channel to signal pause.
	}

	// Safely add the shard to the map keyed by its host.
	s.rw.Lock()
	s.shards[opts.Host] = sh
	s.rw.Unlock()

	// Start observing the shard connection in a new goroutine.
	go sh.observe()
}

// shard represents a single broker shard and encapsulates its configuration and state.
// It manages the connection to the broker, sending and receiving messages,
// and handles reconnection attempts.
type shard struct {
	onlyRoot bool             // Flag indicating if only root should be used.
	brand    string           // Brand associated with the shard.
	module   string           // Module associated with the shard.
	protocol string           // Protocol to use (e.g., "http").
	host     string           // Host address.
	http     string           // HTTP port.
	grpc     string           // gRPC port.
	secret   string           // Authentication token.
	subs     []string         // Subscribed events.
	pubs     []string         // Published events.
	out      chan *pb.Message // Outgoing messages channel.
	in       chan *pb.Message // Incoming messages channel.
	start    chan struct{}    // Channel to signal start.
	pause    chan struct{}    // Channel to signal pause.
	attempt  int              // Number of reconnection attempts.
}

// observe establishes and monitors the gRPC connection to the shard.
// It uses getConnect to establish the connection and then listens on a stream
// for incoming messages. Depending on the message type, it either triggers callbacks
// or sends acknowledgment messages.
func (s *shard) observe() {
	// Establish a gRPC connection to the broker.
	conn, err := s.getConnect()
	if err != nil {
		// Handle connection error and attempt reconnection.
		s.handleError(err)
		return
	}
	// Ensure the connection is closed when observe exits.
	defer conn.Close()

	// Create a new PubSub client from the connection.
	cli := pb.NewPubSubClient(conn)
	// Open a bidirectional channel (stream) with the broker.
	stream, err := cli.Channel(context.Background())
	if err != nil {
		s.handleError(err)
		return
	}
	// Create a cancellable context from the stream's context.
	ctx, cancel := context.WithCancel(stream.Context())
	// Start a goroutine to continuously receive messages from the stream.
	go func() {
		for {
			// Attempt to receive a message from the stream.
			msg, err2 := stream.Recv()
			if msg != nil {
				// Process received message based on its Name.
				switch msg.Name {
				case "_.hello":
					// Reset reconnection attempt counter.
					s.attempt = 0
					// For a "hello" message, start watching the outgoing messages.
					s.watchMessage(stream, cancel)
				default:
					// For all other messages, if acknowledgment is required, send an ack message.
					if msg.Ack {
						s.out <- &pb.Message{
							Id:        msg.Id,
							Name:      "_.ack",
							Timestamp: time.Now().UnixMilli(),
						}
					}
					// Forward the message to the incoming channel.
					s.in <- msg
				}
			}

			// If an error occurs while receiving a message, handle it.
			if err2 != nil {
				s.handleError(err2)
				return
			}
		}
	}()

	// Log the connection status.
	if s.brand == "" {
		fmt.Printf("Connect to Broker. Module: %s \n", s.module)
	} else {
		fmt.Printf("Connect to Broker. Module: %s, brand: %s \n", s.module, s.brand)
	}

	// Block until the context is canceled (i.e., connection loss or error).
	<-ctx.Done()
}

// watchMessage listens on the outgoing channel (s.out) and sends all messages
// through the provided gRPC stream. It uses the stream's context to handle cancellation.
// If sending fails, it calls handleError and cancels the stream.
func (s *shard) watchMessage(stream pb.PubSub_ChannelClient, cancel context.CancelFunc) {
	// Obtain the context from the stream.
	ctx := stream.Context()
	// Start a goroutine to listen on the outgoing channel.
	go func() {
		for {
			select {
			// When a message is available on the outgoing channel.
			case msg := <-s.out:
				// Send the message through the gRPC stream.
				err := stream.Send(msg)
				if err != nil {
					// Handle send error and cancel the context.
					s.handleError(err)
					cancel()
				}
			// If the context is done, exit the goroutine.
			case <-ctx.Done():
				return
			}
		}
	}()
}

// getConnect establishes a secure gRPC connection to the broker for the shard.
// It retrieves the certificate from the broker's /ca endpoint, creates a certificate pool,
// configures TLS, and finally creates a gRPC client connection using the configured credentials.
func (s *shard) getConnect() (*grpc.ClientConn, error) {
	// Build the certificate URL using the protocol, host, and HTTP port.
	certUrl := fmt.Sprintf("%s://%s:%s/ca", s.protocol, s.host, s.http)
	// Issue an HTTP GET request to retrieve the certificate.
	resp, err := http.Get(certUrl)
	if err != nil {
		return nil, status.Errorf(codes.Canceled, "connection error")
	}
	// Ensure the response body is closed after reading.
	defer resp.Body.Close()
	// Read the certificate data from the response.
	certData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, status.Errorf(codes.Canceled, "failed to read certificate data: %v", err)
	}
	// Create a new certificate pool.
	certPool := x509.NewCertPool()
	// Append the certificate data to the certificate pool.
	if ok := certPool.AppendCertsFromPEM(certData); !ok {
		return nil, status.Errorf(codes.Canceled, "failed to append certificate")
	}
	// Configure TLS with the created certificate pool and server name.
	tlsConfig := &tls.Config{
		RootCAs:    certPool, // Use the certificate pool.
		ServerName: s.host,   // Set the server name for verification.
	}
	// Create gRPC TLS credentials from the TLS configuration.
	creds := credentials.NewTLS(tlsConfig)
	// (Optional) Set up keepalive parameters here if needed.
	// Set up the broker URL using host and the gRPC port.
	brokerUrl := fmt.Sprintf("%s:%s", s.host, s.grpc)
	// Establish a gRPC connection using the broker URL, TLS credentials, and a stream interceptor.
	conn, err := grpc.NewClient(
		brokerUrl,
		grpc.WithTransportCredentials(creds),
		grpc.WithStreamInterceptor(s.interceptor),
	)
	if err != nil {
		return nil, status.Errorf(codes.Canceled, "did not connect: %v", err)
	}
	return conn, nil
}

// interceptor is a gRPC stream interceptor used to attach metadata (including an HMAC token)
// to outgoing gRPC streams. It generates a token based on shard information and embeds it into
// a new context which is then used to call the gRPC method.
func (s *shard) interceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// Generate the current timestamp in RFC3339 format.
	timestamp := time.Now().UTC().Format(time.RFC3339)
	// Create a new token instance using shard information and the current timestamp.
	t := &token{
		Brand:     s.brand,
		Module:    s.module,
		Subs:      s.subs,
		Pubs:      s.pubs,
		Timestamp: timestamp,
	}
	// Generate an HMAC signature using the shard token.
	signature, err := t.GenerateHMAC(s.secret)
	if err != nil {
		return nil, fmt.Errorf("failed to generate HMAC: %v", err)
	}
	// Create a new metadata map with authorization and other required fields.
	md := metadata.New(map[string]string{
		"Authorization": fmt.Sprintf("HMAC-SHA256 %s:%s", s.module, signature),
		"X-Brand":       s.brand,
		"X-Subs":        strings.Join(s.subs, ","),
		"X-Pubs":        strings.Join(s.pubs, ","),
		"X-Only-Root":   fmt.Sprintf("%t", s.onlyRoot),
		"X-Timestamp":   timestamp,
	})
	// Attach the metadata to the outgoing context.
	ctx = metadata.NewOutgoingContext(ctx, md)
	// Create the stream using the modified context.
	st, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	// Return a wrapped stream that embeds our custom stream.
	return newWrappedStream(st), nil
}

// handleError processes errors encountered during connection or stream operations.
// It panics for fatal errors (like invalid arguments or unauthenticated access)
// and, for other errors, logs the issue and initiates a reconnection attempt.
func (s *shard) handleError(err error) {
	switch status.Code(err) {
	case codes.InvalidArgument:
		panic(fmt.Sprintf("Invalid argument: %s", err.Error()))
	case codes.Unauthenticated:
		panic(fmt.Sprintf("Unauthenticated: %s", err.Error()))
	default:
		if s.brand == "" {
			fmt.Printf("Connect error: %s. Module: %s \n", err.Error(), s.module)
		} else {
			fmt.Printf("Connect error: %s. Module: %s, brand: %s \n", err.Error(), s.module, s.brand)
		}
		// For recoverable errors, initiate reconnection.
		go s.reconnect()
	}
}

// reconnect attempts to re-establish a connection to the broker shard.
// It uses an exponential backoff strategy with a maximum delay to prevent overwhelming
// the system with rapid reconnection attempts.
func (s *shard) reconnect() {
	if s.attempt > 0 {
		fmt.Println(fmt.Sprintf("Reconnect after %d seconds to: %s", s.attempt, s.host))
	}
	// Sleep for a duration based on the current attempt count.
	time.Sleep(time.Second * time.Duration(s.attempt))
	// Increase the attempt counter, doubling it, but cap it at 8 seconds.
	if s.attempt == 0 {
		s.attempt = 1
	} else {
		s.attempt = s.attempt * 2
		if s.attempt > 8 {
			s.attempt = 8
		}
	}
	// Restart the observe process in a new goroutine.
	go s.observe()
}

// wrappedStream is a simple wrapper around grpc.ClientStream.
// It allows extending or customizing the underlying stream behavior if necessary.
type wrappedStream struct {
	grpc.ClientStream
}

// RecvMsg calls the underlying stream's RecvMsg method.
func (w *wrappedStream) RecvMsg(m any) error {
	return w.ClientStream.RecvMsg(m)
}

// SendMsg calls the underlying stream's SendMsg method.
func (w *wrappedStream) SendMsg(m any) error {
	return w.ClientStream.SendMsg(m)
}

// newWrappedStream creates and returns a new wrappedStream instance that wraps the given grpc.ClientStream.
func newWrappedStream(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStream{s}
}
