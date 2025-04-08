# PubSub Package

The **PubSub** package implements a Publish/Subscribe messaging system with guaranteed delivery, acknowledgment (ack) handling, automatic resending, asynchronous callbacks, and secure gRPC connectivity using TLS and HMAC authentication.

## Table of Contents
- [Overview](#overview)
- [Main Features](#main-features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
    - [Creating a Client](#creating-a-client)
    - [Publishing Messages (Pub)](#publishing-messages-pub)
    - [Subscribing to Events (Sub)](#subscribing-to-events-sub)
    - [Request-Response Scenario (sub/pub)](#request-response-scenarios-subpub)
    - [Applying Brand and Metadata](#applying-brand-and-metadata)
    - [Adding an Error to a Response](#adding-an-error-to-a-response)
- [Authentication and Connection](#authentication-and-connection)
- [Tests](#tests)
- [License](#license)

## Overview

The **PubSub** package allows you to:
- Ensure reliable message delivery with acknowledgments (ack) and automatic resending if ack is not received.
- Register asynchronous callbacks to process responses.
- Easily subscribe to and unsubscribe from events, receiving incoming messages for those events.
- Use context propagation (`context.Context`) to attach additional information – such as brand, metadata, and errors – to the messages.
- Establish secure gRPC connections to the broker using TLS and HMAC signatures for authentication.

## Main Features

- **Delivery Acknowledgments:** Automatically resend messages until an acknowledgment is received.
- **Callbacks:** Asynchronous notifications on message reception.
- **Subscriptions:** Simple registration and removal of event subscribers.
- **Context-Based Metadata:** Ability to attach a brand, custom headers, and errors to messages.
- **Secure Connection:** TLS-secured connection to the broker with HMAC-based authentication.
- **Flexible Pub/Sub Pattern:** Supports simple publication and subscription as well as request-response scenarios.

## Installation

Use Go modules to install the package:

```bash
go get github.com/gambitclub/go-pubsub
```
Make sure the dependencies for gRPC and Protobuf are installed.

## Configuration
The package is configured via the pubsub.Options structure or through environment variables. 
Example environment variables:
- **BROKER_HOST:** A string that specifies one or more brokers in the following format: 
  `http.broker-1:8080.50051,http.broker-2:8080.50051`

  Here, each part indicates the protocol (http), the hostname (e.g., broker-1), 
  the HTTP port (8080) and the gRPC port (50051). Multiple brokers are separated by commas.
- **BROKER_SECRET:** Example value: `some_secret_key`
- **MODULE:** The module identifier (e.g., "order-service").
- **PUBS:** A comma-separated list of events for publication (e.g., "event.create,event.update").
- **SUBS:** A comma-separated list of events for subscription (e.g., "event.delete,event.query").

These variables allow you to flexibly configure the connection to the broker 
and determine which events to publish and subscribe to.

## Usage Examples
Below are examples demonstrating the key scenarios.

### Creating a Client
```go
package main

import (
	"log"
	"os"
	"strings"
	"github.com/gambitclub/go-pubsub"
)

func main() {
	// Read configuration from environment variables.
	brokerHost := os.Getenv("BROKER_HOST")   // Example: "http.broker-1:8080.50051,http.broker-2:8080.50051"
	brokerSecret := os.Getenv("BROKER_SECRET")  // Example: "some_secret"
	module := os.Getenv("MODULE")              // Module identifier
	pubs := os.Getenv("PUBS")                  // Events to publish, e.g. "event.create,event.update"
	subs := os.Getenv("SUBS")                  // Events to subscribe to, e.g. "event.delete,event.query"
	
	// Initialize a new PubSub client.
	client := pubsub.New(pubsub.Options{
		Module: module,
		Host:   brokerHost, 
		Secret: brokerSecret,
		Pubs:   strings.Split(pubs, ","),
		Subs:   strings.Split(subs, ","),
	})
	
	log.Println("PubSub client initialized:", client)
}
```

### Publishing Messages (Pub)
This example demonstrates how to publish a message with acknowledgment enabled, as well as adding metadata, 
brand, and errors to the context:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/gambitclub/go-pubsub"
)

func publishMessage(client *pubsub.Client) {
	ctx := context.Background()

	// Enable acknowledgment, so if an ack is not received the message will be resent.
	ctx = client.Ack(ctx)
	
	// Apply a brand to the message (e.g., "my-brand").
	ctx = client.WithBrand(ctx, "my-brand")
	
	// Add additional metadata to the message.
	ctx = client.WithMetadata(ctx, map[string]string{
		"lang": "en",
	})
	
	// Add an error to the context (optional, for conveying error details).
	ctx = client.WithError(ctx, fmt.Errorf("sample error message"))
	
	// Set a timeout to ensure the publishing does not wait indefinitely.
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	
	// Create and send a message with the "event.publish" event and a payload.
	pub := client.Pub(ctx, "event.publish", []byte("Hello, PubSub!"))
	pub.Do() // Send the message via the outgoing channel.
	log.Println("Message published")
}
```

### Subscribing to Events (Sub)
This example shows how to register a subscriber to process incoming messages for a specific event:
```go
package main

import (
	"context"
	"log"
	"github.com/gambitclub/go-pubsub"
)

func subscribeToEvents(client *pubsub.Client) {
	// Register a subscriber for the "event.subscribe" event.
	client.Sub(context.Background(), "event.subscribe", func(ctx context.Context, payload []byte) {
		log.Println("Received message for event.subscribe:", string(payload))
	})
}
```

### Request-Response Scenario (sub/pub)

This example demonstrates a scenario where the client publishes a request and waits for a response on another event. 
The Sub method blocks until a response is received or the timeout expires:
```go
package main

import (
	"context"
	"fmt"
	"github.com/gambitclub/go-pubsub"
)

func requestResponse(client *pubsub.Client) {
	// Publish a request on the "event.request" channel and wait for a response on "event.response".
    client.Sub(context.Background(), "event.request", func(ctx context.Context, payload []byte) {
		// Process the request and prepare a response.
		// Use the context to attach metadata, errors, etc.
		ctx = client.WithError(ctx, fmt.Errorf("sample error message"))
        client.Pub(ctx, "event.response", payload).Do()
    })
}
```

### Applying Brand and Metadata
The package allows attaching a brand and additional metadata to every message. 
This is useful for routing, filtering, or logging purposes.
```go
package main

import (
	"context"
	"log"
	"github.com/gambitclub/go-pubsub"
)

func publishWithMetadata(client *pubsub.Client) {
	ctx := context.Background()
	
	// Apply a brand (e.g., "premium-brand") to the message.
	ctx = client.WithBrand(ctx, "premium-brand")
	
	// Add custom metadata like version and region.
	ctx = client.WithMetadata(ctx, map[string]string{
		"version": "1.0",
		"region":  "us-west",
	})
	
	// Publish the message with event "event.metadata".
	client.Pub(ctx, "event.metadata", []byte("Message with metadata")).Do()
	log.Println("Published message with brand and metadata")
}
```

### Adding an Error to a Response

You can attach error information to a message by using the WithError method, 
which is useful for conveying processing errors to subscribers.
```go
package main

import (
	"context"
	"fmt"
	"log"
	"github.com/gambitclub/go-pubsub"
)

func publishWithError(client *pubsub.Client) {
	ctx := context.Background()
	
	// Inject an error into the context.
	ctx = client.WithError(ctx, fmt.Errorf("processing error occurred"))
	
	// Publish a message with the "event.error" event.
	client.Pub(ctx, "event.error", []byte("Message with error")).Do()
	log.Println("Published message with error")
}
```

### Authentication and Connection
**The package uses gRPC to connect to the broker. It ensures secure connectivity through:**
- **TLS Connection:** A secure connection is established with the broker using TLS. Certificates are retrieved from the broker's HTTP endpoint.
- **HMAC Authentication:** A HMAC signature is generated based on module data, brand, subscription and publication lists, and a timestamp. This signature is attached to each outgoing gRPC connection as metadata.

**Example environment variable settings:**
- **BROKER_HOST:** `http.broker-1:8080.50051,http.broker-2:8080.50051`
- **BROKER_SECRET:** `some_secret_key`

Make sure that BROKER_HOST and BROKER_SECRET are correctly set in your environment and that the broker’s certificates are available for TLS verification.

## Tests
The package includes unit tests to ensure the functionality of the PubSub client.
To run the tests, use the following command:
```bash
go test -v ./...
```

## License
This project is licensed under the MIT License.
