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

func TestFileStorageSaveReplacesPreviousState(t *testing.T) {
	dir := t.TempDir()
	storage := newFileStorage(dir)

	first := PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node-b",
		Log: []LogEntry{
			{Term: 3, Index: 1, Command: []byte("first")},
		},
	}

	second := PersistentState{
		CurrentTerm: 8,
		VotedFor:    "node-c",
		Log: []LogEntry{
			{Term: 3, Index: 1, Command: []byte("first")},
			{Term: 8, Index: 2, Command: []byte("second")},
		},
	}

	if err := storage.Save(first); err != nil {
		t.Fatalf("save first state: %v", err)
	}

	if err := storage.Save(second); err != nil {
		t.Fatalf("save second state: %v", err)
	}

	// Reopen rather than trusting any state held by the original storage value.
	reader := newFileStorage(dir)

	got, err := reader.Load()
	if err != nil {
		t.Fatalf("load persistent state: %v", err)
	}

	if !reflect.DeepEqual(got, second) {
		t.Fatalf("persistent state after replacement:\n got: %+v\nwant: %+v", got, second)
	}
}
