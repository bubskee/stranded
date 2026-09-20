package main

import (
	"reflect"
	"testing"

	"github.com/bubskee/stranded/node"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    map[node.PeerID]string
		wantErr bool
	}{
		{
			name: "empty",
			raw:  "",
			want: map[node.PeerID]string{},
		},
		{
			name: "single peer",
			raw:  "node-b=127.0.0.1:5002",
			want: map[node.PeerID]string{
				"node-b": "127.0.0.1:5002",
			},
		},
		{
			name: "multiple peers",
			raw:  "node-b=127.0.0.1:5002,node-c=127.0.0.1:5003",
			want: map[node.PeerID]string{
				"node-b": "127.0.0.1:5002",
				"node-c": "127.0.0.1:5003",
			},
		},
		{
			name: "whitespace",
			raw:  " node-b = 127.0.0.1:5002 , node-c=127.0.0.1:5003 ",
			want: map[node.PeerID]string{
				"node-b": "127.0.0.1:5002",
				"node-c": "127.0.0.1:5003",
			},
		},
		{
			name:    "missing separator",
			raw:     "node-b",
			wantErr: true,
		},
		{
			name:    "empty id",
			raw:     "=127.0.0.1:5002",
			wantErr: true,
		},
		{
			name:    "empty address",
			raw:     "node-b=",
			wantErr: true,
		},
		{
			name:    "duplicate peer",
			raw:     "node-b=127.0.0.1:5002,node-b=127.0.0.1:6002",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePeers(tt.raw)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("parsePeers(%q): got nil error, want error", tt.raw)
				}
				return
			}

			if err != nil {
				t.Fatalf("parsePeers(%q): %v", tt.raw, err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf(
					"parsePeers(%q): got %#v, want %#v",
					tt.raw,
					got,
					tt.want,
				)
			}
		})
	}
}
