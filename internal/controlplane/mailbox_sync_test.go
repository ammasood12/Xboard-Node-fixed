package controlplane

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

// A user removed by a REST poll must not be resurrected by a later WS delta
// applied on the mailbox baseline (fixed8).
func TestSyncBaselineDropsUsersRemovedByREST(t *testing.T) {
	m := NewNodeMailbox()
	m.SeedBaseline([]model.UserSpec{{ID: 1, UUID: "u1"}, {ID: 2, UUID: "u2"}}, nil)
	m.MarkReady()

	// REST poll: user 2 expired and was removed.
	m.SyncBaseline([]model.UserSpec{{ID: 1, UUID: "u1"}})

	// WS delta for an unrelated new user.
	m.Apply(Event{
		Type:        EventSyncUserDelta,
		DeltaAction: "add",
		DeltaUsers:  []model.UserSpec{{ID: 3, UUID: "u3"}},
	})

	state := m.DrainIfReady()
	if !state.HasUsers {
		t.Fatal("expected drained users after delta")
	}
	if len(state.Users) != 2 {
		t.Fatalf("drained %d users, want 2 (ids 1 and 3)", len(state.Users))
	}
	for _, u := range state.Users {
		if u.ID == 2 {
			t.Fatal("user 2 removed by REST was resurrected from the stale baseline")
		}
	}
}

func TestSyncBaselineIgnoresNil(t *testing.T) {
	m := NewNodeMailbox()
	m.SeedBaseline([]model.UserSpec{{ID: 1, UUID: "u1"}}, nil)
	m.SyncBaseline(nil)
	m.MarkReady()
	m.Apply(Event{
		Type:        EventSyncUserDelta,
		DeltaAction: "add",
		DeltaUsers:  []model.UserSpec{{ID: 2, UUID: "u2"}},
	})
	state := m.DrainIfReady()
	if len(state.Users) != 2 {
		t.Fatalf("drained %d users, want 2", len(state.Users))
	}
}
