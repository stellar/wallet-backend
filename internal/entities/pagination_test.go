package entities

import (
	"encoding/json"
	"testing"
)

func TestPaginationLinks_JSONTagsMatchFieldNames(t *testing.T) {
	links := PaginationLinks{
		Self: "self-url",
		Prev: "prev-url",
		Next: "next-url",
	}

	raw, err := json.Marshal(links)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := decoded["prev"]; got != "prev-url" {
		t.Errorf(`json key "prev" = %q, want "prev-url"`, got)
	}
	if got := decoded["next"]; got != "next-url" {
		t.Errorf(`json key "next" = %q, want "next-url"`, got)
	}
	if got := decoded["self"]; got != "self-url" {
		t.Errorf(`json key "self" = %q, want "self-url"`, got)
	}
}
