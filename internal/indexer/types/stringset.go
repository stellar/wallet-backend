package types

// StringSet is a lightweight, non-thread-safe string set for single-goroutine use.
// It replaces golang-set's thread-safe Set[string] in hot paths where concurrency
// is handled at a higher level (e.g., per-transaction buffers with RWMutex).
type StringSet map[string]struct{}

func NewStringSet(vals ...string) StringSet {
	s := make(StringSet, len(vals))
	for _, v := range vals {
		s[v] = struct{}{}
	}
	return s
}

func (s StringSet) Add(val string)      { s[val] = struct{}{} }
func (s StringSet) Has(val string) bool { _, ok := s[val]; return ok }
func (s StringSet) Len() int            { return len(s) }
func (s StringSet) IsEmpty() bool       { return len(s) == 0 }

func (s StringSet) Append(vals ...string) {
	for _, v := range vals {
		s[v] = struct{}{}
	}
}

func (s StringSet) ToSlice() []string {
	out := make([]string, 0, len(s))
	for v := range s {
		out = append(out, v)
	}
	return out
}
