// Package store provides data access layer for Redis operations.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisOperation represents a single Redis set operation type.
type RedisOperation string

const (
	SetOpAdd    RedisOperation = "SADD"
	SetOpRemove RedisOperation = "SREM"
	OpSet       RedisOperation = "SET"
)

// Token registry keys for mapping full strings to short integer IDs.
// This reduces Redis memory usage by storing short IDs instead of full asset/contract strings.
const (
	// Asset registry (for trustlines)
	assetToIDKey    = "asset_to_id"   // HASH: asset_string → ID
	idToAssetKey    = "id_to_asset"   // HASH: ID → asset_string
	assetCounterKey = "asset_counter" // STRING: atomic counter for ID assignment

	// Contract registry
	contractToIDKey    = "contract_to_id"   // HASH: contract_address → ID
	idToContractKey    = "id_to_contract"   // HASH: ID → contract_address
	contractCounterKey = "contract_counter" // STRING: atomic counter for ID assignment
)

// RedisPipelineOperation represents a single set operation to be executed in a pipeline.
type RedisPipelineOperation struct {
	Op      RedisOperation
	Key     string
	Members []string
	Value   string
}

// RedisStore provides generic Redis operations for caching.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore creates a new Redis store with the given connection parameters.
func NewRedisStore(host string, port int, password string) *RedisStore {
	addr := fmt.Sprintf("%s:%d", host, port)
	return &RedisStore{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
		}),
	}
}

// SAdd adds one or more members to a set stored at key.
func (r *RedisStore) SAdd(ctx context.Context, key string, members ...string) error {
	if err := r.client.SAdd(ctx, key, members).Err(); err != nil {
		return fmt.Errorf("adding members to set %s: %w", key, err)
	}
	return nil
}

// SMembers returns all members of the set stored at key.
func (r *RedisStore) SMembers(ctx context.Context, key string) ([]string, error) {
	members, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("getting members of set %s: %w", key, err)
	}
	return members, nil
}

// Get retrieves the value of a key.
func (r *RedisStore) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil // Key doesn't exist
	}
	if err != nil {
		return "", fmt.Errorf("getting key %s: %w", key, err)
	}
	return val, nil
}

// Set stores a string value at a key.
func (r *RedisStore) Set(ctx context.Context, key, value string) error {
	if err := r.client.Set(ctx, key, value, 0).Err(); err != nil {
		return fmt.Errorf("setting key %s: %w", key, err)
	}
	return nil
}

// ExecutePipeline executes multiple operations (SADD/SREM/SET) in a single pipeline.
// This reduces network round trips and improves performance when processing many operations.
// Returns an error if any operation in the pipeline fails.
func (r *RedisStore) ExecutePipeline(ctx context.Context, operations []RedisPipelineOperation) error {
	if len(operations) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, op := range operations {
		switch op.Op {
		case SetOpAdd:
			pipe.SAdd(ctx, op.Key, op.Members)
		case SetOpRemove:
			pipe.SRem(ctx, op.Key, op.Members)
		case OpSet:
			pipe.Set(ctx, op.Key, op.Value, 0)
		default:
			return fmt.Errorf("unsupported set operation: %s", op.Op)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("executing set pipeline: %w", err)
	}

	return nil
}

// GetOrCreateAssetID returns the ID for an asset string, creating a new ID if it doesn't exist.
// Uses Redis HASH for O(1) lookups and INCR for atomic ID assignment.
func (r *RedisStore) GetOrCreateAssetID(ctx context.Context, asset string) (string, error) {
	return r.getOrCreateID(ctx, asset, assetToIDKey, idToAssetKey, assetCounterKey)
}

// GetAssetsByIDs batch resolves asset IDs to their full asset strings.
// Returns a slice of asset strings in the same order as the input IDs.
func (r *RedisStore) GetAssetsByIDs(ctx context.Context, ids []string) ([]string, error) {
	return r.getValuesByIDs(ctx, ids, idToAssetKey)
}

// BatchAssignAssetIDs assigns IDs to multiple assets in a single pipeline.
// Returns a map from asset string to its assigned ID.
func (r *RedisStore) BatchAssignAssetIDs(ctx context.Context, assets []string) (map[string]string, error) {
	return r.batchAssignIDs(ctx, assets, assetToIDKey, idToAssetKey, assetCounterKey)
}

// GetOrCreateContractID returns the ID for a contract address, creating a new ID if it doesn't exist.
func (r *RedisStore) GetOrCreateContractID(ctx context.Context, contractAddr string) (string, error) {
	return r.getOrCreateID(ctx, contractAddr, contractToIDKey, idToContractKey, contractCounterKey)
}

// GetContractsByIDs batch resolves contract IDs to their full contract addresses.
// Returns a slice of contract addresses in the same order as the input IDs.
func (r *RedisStore) GetContractsByIDs(ctx context.Context, ids []string) ([]string, error) {
	return r.getValuesByIDs(ctx, ids, idToContractKey)
}

// BatchAssignContractIDs assigns IDs to multiple contracts in a single pipeline.
// Returns a map from contract address to its assigned ID.
func (r *RedisStore) BatchAssignContractIDs(ctx context.Context, contracts []string) (map[string]string, error) {
	return r.batchAssignIDs(ctx, contracts, contractToIDKey, idToContractKey, contractCounterKey)
}

// getOrCreateID is a generic helper for creating/retrieving IDs for token strings.
func (r *RedisStore) getOrCreateID(ctx context.Context, value, toIDKey, toValueKey, counterKey string) (string, error) {
	// Check if ID already exists
	existingID, err := r.client.HGet(ctx, toIDKey, value).Result()
	if err == nil {
		return existingID, nil
	}
	if !errors.Is(err, redis.Nil) {
		return "", fmt.Errorf("checking existing ID for %s: %w", value, err)
	}

	// ID doesn't exist, assign a new one atomically
	newID, err := r.client.Incr(ctx, counterKey).Result()
	if err != nil {
		return "", fmt.Errorf("incrementing counter %s: %w", counterKey, err)
	}

	idStr := fmt.Sprintf("%d", newID)

	// Store bidirectional mapping using pipeline
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, toIDKey, value, idStr)
	pipe.HSet(ctx, toValueKey, idStr, value)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("storing ID mapping for %s: %w", value, err)
	}

	return idStr, nil
}

// getValuesByIDs batch retrieves values for multiple IDs using HMGET.
func (r *RedisStore) getValuesByIDs(ctx context.Context, ids []string, toValueKey string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	values, err := r.client.HMGet(ctx, toValueKey, ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("batch getting values from %s: %w", toValueKey, err)
	}

	result := make([]string, len(ids))
	for i, v := range values {
		if v != nil {
			result[i] = v.(string)
		}
	}
	return result, nil
}

// batchAssignIDs assigns IDs to multiple values efficiently using pipelining.
// First checks which values already have IDs, then assigns new IDs only for missing ones.
func (r *RedisStore) batchAssignIDs(ctx context.Context, values []string, toIDKey, toValueKey, counterKey string) (map[string]string, error) {
	if len(values) == 0 {
		return make(map[string]string), nil
	}

	result := make(map[string]string, len(values))

	// Check which values already have IDs
	existingIDs, err := r.client.HMGet(ctx, toIDKey, values...).Result()
	if err != nil {
		return nil, fmt.Errorf("checking existing IDs: %w", err)
	}

	// Collect values that need new IDs
	var needsID []string
	for i, v := range existingIDs {
		if v != nil {
			result[values[i]] = v.(string)
		} else {
			needsID = append(needsID, values[i])
		}
	}

	if len(needsID) == 0 {
		return result, nil
	}

	// Reserve a range of IDs atomically
	endID, err := r.client.IncrBy(ctx, counterKey, int64(len(needsID))).Result()
	if err != nil {
		return nil, fmt.Errorf("reserving ID range: %w", err)
	}
	startID := endID - int64(len(needsID)) + 1

	// Assign IDs and store mappings using pipeline
	pipe := r.client.Pipeline()
	for i, value := range needsID {
		idStr := fmt.Sprintf("%d", startID+int64(i))
		result[value] = idStr
		pipe.HSet(ctx, toIDKey, value, idStr)
		pipe.HSet(ctx, toValueKey, idStr, value)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("storing batch ID mappings: %w", err)
	}

	return result, nil
}
