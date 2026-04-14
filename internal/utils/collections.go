package utils

import "fmt"

// BuildMap converts a slice into a map keyed by keyFn. Returns an error if
// any two elements produce the same key.
func BuildMap[T any](items []T, keyFn func(T) string) (map[string]T, error) {
	result := make(map[string]T, len(items))
	for _, item := range items {
		key := keyFn(item)
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		result[key] = item
	}
	return result, nil
}
