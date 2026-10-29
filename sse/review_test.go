// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package sse

import (
	"strings"
	"testing"
)

func TestReviewCommentsAndID(t *testing.T) {
	r := NewReader(32)
	var ids []string
	err := r.Feed([]byte(":"+strings.Repeat("x", 100)+"\nid: abc\ndata: one\n\ndata: two\n\n: tail\n"), func(e Event) error { ids = append(ids, e.ID); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = r.End(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "abc" || ids[1] != "abc" {
		t.Fatal(ids)
	}
}
func TestReviewDiscardIncomplete(t *testing.T) {
	r := NewReader(32)
	if err := r.Feed([]byte("data: partial"), func(Event) error { t.Fatal("unexpected event"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.End(); err != nil {
		t.Fatal(err)
	}
}
