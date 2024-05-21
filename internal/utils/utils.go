package utils

import "reflect"

// IsEmpty checks if a value is empty.
func IsEmpty[T any](v T) bool {
	return reflect.ValueOf(&v).Elem().IsZero()
}

// UnwrapInterfaceToPointer unwraps an interface to a pointer of the given type.
func UnwrapInterfaceToPointer[T any](i interface{}) *T {
	t, ok := i.(*T)
	if ok {
		return t
	}
	return nil
}
