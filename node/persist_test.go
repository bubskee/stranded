package node

import (
	"reflect"
	"testing"
)

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()

	want := PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-b",
		Log: []LogEntry{
			{
				Term:    3,
				Index:   1,
				Command: []byte("set x 1"),
			},
			{
				Term:    7,
				Index:   2,
				Command: []byte("set y 2"),
			},
		},
	}

	writer := newFileStorage(dir)

	if err := writer.Save(want); err != nil {
		t.Fatalf("save persistent state: %v", err)
	}

	// A fresh storage value models reopening stable storage after restart.
	reader := newFileStorage(dir)

	got, err := reader.Load()
	if err != nil {
		t.Fatalf("load persistent state: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persistent state after reload:\n got: %+v\nwant: %+v", got, want)
	}
}
