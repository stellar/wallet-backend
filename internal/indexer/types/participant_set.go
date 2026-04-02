package types

// ParticipantSet is a set of participant account IDs implemented as a plain map.
// It replaces golang-set/v2 to eliminate the library's internal mutex overhead,
// since the IndexerBuffer already guarantees single-goroutine access.
type ParticipantSet map[string]struct{}

// NewParticipantSet creates a ParticipantSet initialized with the given members.
func NewParticipantSet(members ...string) ParticipantSet {
	s := make(ParticipantSet, len(members))
	for _, m := range members {
		s[m] = struct{}{}
	}
	return s
}

// Add inserts a participant into the set.
func (s ParticipantSet) Add(participant string) {
	s[participant] = struct{}{}
}

// Cardinality returns the number of elements in the set.
func (s ParticipantSet) Cardinality() int {
	return len(s)
}

// ToSlice returns all participants as a slice.
func (s ParticipantSet) ToSlice() []string {
	result := make([]string, 0, len(s))
	for p := range s {
		result = append(result, p)
	}
	return result
}

// Clone returns a deep copy of the set.
func (s ParticipantSet) Clone() ParticipantSet {
	clone := make(ParticipantSet, len(s))
	for k := range s {
		clone[k] = struct{}{}
	}
	return clone
}
