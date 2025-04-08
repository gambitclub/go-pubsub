// =============================================================================
// Package pubsub - Token and HMAC Generation
//
// This file defines the "token" type which encapsulates authentication details
// used in the PubSub system for verifying the identity and integrity of messages.
// The token includes properties such as Brand, Module, subscriptions (Subs), publications (Pubs),
// and a Timestamp. The GenerateHMAC method produces an HMAC signature using the SHA256 algorithm,
// a provided secret, and a deterministic JSON representation of the token data.
//
// Key steps in the GenerateHMAC method:
//   1. Create a map containing token properties.
//   2. Sort the slices (Subs and Pubs) to ensure a consistent order.
//   3. Sort the map keys to produce a deterministic JSON output.
//   4. Marshal the sorted map into JSON.
//   5. Generate the HMAC signature using HMAC-SHA256 and encode it as a hexadecimal string.
// =============================================================================

package pubsub

import (
	"crypto/hmac"   // Provides HMAC cryptographic functions.
	"crypto/sha256" // Provides SHA256 hash algorithm.
	"encoding/hex"  // For encoding binary data into hexadecimal strings.
	"encoding/json" // For marshaling map data into JSON.
	"sort"          // For sorting slices and keys to ensure deterministic order.
)

// token encapsulates the authentication information for the PubSub system.
// It includes identifiers and lists of events for subscriptions and publications,
// as well as a timestamp indicating when the token was generated.
type token struct {
	Brand     string   // The brand associated with the token.
	Module    string   // The module name.
	Subs      []string // List of events the module subscribes to.
	Pubs      []string // List of events the module publishes.
	Timestamp string   // A timestamp string marking when the token was created.
}

// GenerateHMAC generates an HMAC signature for the token using the provided secret.
// It returns the hexadecimal encoded signature or an error if the JSON marshaling fails.
func (t *token) GenerateHMAC(secret string) (string, error) {
	// Create a map representing the token data.
	m := map[string]interface{}{
		"module":    t.Module,
		"brand":     t.Brand,
		"pubs":      t.Pubs,
		"subs":      t.Subs,
		"timestamp": t.Timestamp,
	}

	// Sort the Subs and Pubs slices to ensure a consistent order.
	// This is critical because HMAC generation must be deterministic.
	sort.Strings(m["subs"].([]string))
	sort.Strings(m["pubs"].([]string))

	// Extract the keys from the map and sort them.
	// Sorting the keys ensures that the JSON representation is consistent.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create a new map where the keys appear in sorted order.
	// This sortedMap is used to generate a deterministic JSON representation.
	sortedMap := make(map[string]interface{}, len(m))
	for _, k := range keys {
		sortedMap[k] = m[k]
	}

	// Marshal the sorted map to JSON.
	jsonData, err := json.Marshal(sortedMap)
	if err != nil {
		return "", err // Return error if marshaling fails.
	}

	// Create a new HMAC hasher with SHA256 using the provided secret.
	h := hmac.New(sha256.New, []byte(secret))
	// Write the JSON data into the hasher.
	h.Write(jsonData)
	// Compute the final HMAC signature and encode it as a hexadecimal string.
	signature := hex.EncodeToString(h.Sum(nil))

	// Return the signature.
	return signature, nil
}
