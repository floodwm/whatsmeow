// Copyright (c) 2026 Wappi
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/gcmutil"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type statusQueryStore struct {
	statusLIDStore
	saved    []store.LIDMapping
	writeErr error
}

func (s *statusQueryStore) GetLIDForPN(context.Context, types.JID) (types.JID, error) {
	return types.EmptyJID, nil
}
func (s *statusQueryStore) PutManyLIDMappings(_ context.Context, mappings []store.LIDMapping) error {
	s.saved = slices.Clone(mappings)
	return s.writeErr
}

// A local encrypted WebSocket exercises the actual USync request/response path.
// No WhatsApp account, remote socket or production session is used.
func attachStatusQueryServer(t *testing.T, client *Client, answer func(*waBinary.Node) waBinary.Node) {
	t.Helper()
	const pattern = "Wappi status LID query test"
	hash := sha256.Sum256([]byte(pattern))
	keys, err := hkdf.Key(sha256.New, nil, hash[:], "", 64)
	if err != nil {
		t.Fatal(err)
	}
	writeKey, err := gcmutil.Prepare(keys[:32])
	if err != nil {
		t.Fatal(err)
	}
	readKey, err := gcmutil.Prepare(keys[32:])
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"web.whatsapp.com"}})
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, frame, err := conn.Read(ctx)
		if err != nil || len(frame) < 3 {
			t.Errorf("read query: %v", err)
			return
		}
		plain, err := writeKey.Open(nil, make([]byte, 12), frame[3:], nil)
		if err != nil {
			t.Errorf("decrypt query: %v", err)
			return
		}
		plain, err = waBinary.Unpack(plain)
		if err != nil {
			t.Errorf("unpack query: %v", err)
			return
		}
		query, err := waBinary.Unmarshal(plain)
		if err != nil {
			t.Errorf("decode query: %v", err)
			return
		}
		response := answer(query)
		encoded, err := waBinary.Marshal(response)
		if err != nil {
			t.Errorf("encode response: %v", err)
			return
		}
		// Marshal already includes the uncompressed flag consumed by handleFrame.
		ciphertext := readKey.Seal(nil, make([]byte, 12), encoded, nil)
		size := len(ciphertext)
		wire := append([]byte{byte(size >> 16), byte(size >> 8), byte(size)}, ciphertext...)
		if err = conn.Write(ctx, websocket.MessageBinary, wire); err != nil {
			t.Errorf("write response: %v", err)
			return
		}
		_, _, _ = conn.Read(ctx) // Keep the socket alive until test cleanup closes it.
	}))
	t.Cleanup(server.Close)
	fs := socket.NewFrameSocket(waLog.Noop, server.Client())
	fs.URL, fs.Header = "ws"+strings.TrimPrefix(server.URL, "http"), nil
	if err := fs.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	handshake := socket.NewNoiseHandshake()
	handshake.Start(pattern, nil)
	noise, err := handshake.Finish(fs, client.makeFrameHandler(nil), func(context.Context, *socket.NoiseSocket, bool) {})
	if err != nil {
		fs.Close(0)
		t.Fatal(err)
	}
	client.socket = noise
	t.Cleanup(func() { noise.Stop(true, false) })
}

func TestStatusBroadcastLIDOnlyQueryUsesRealTransport(t *testing.T) {
	for _, cacheFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "cache success", true: "cache failure keeps current response"}[cacheFails], func(t *testing.T) {
			pns := statusTestPNs(2)
			want := []types.JID{types.NewJID("90000000001", types.HiddenUserServer), types.NewJID("90000000002", types.HiddenUserServer)}
			client := newStatusTestClient(nil, nil)
			cache := &statusQueryStore{}
			if cacheFails {
				cache.writeErr = errors.New("test cache write failure")
			}
			client.Store.LIDs = cache
			client.Store.PrivacyTokens = &store.NoopStore{}
			attachStatusQueryServer(t, client, func(query *waBinary.Node) waBinary.Node {
				if query.Tag != "iq" || query.Attrs["xmlns"] != "usync" {
					t.Errorf("not a USync IQ: %s", query)
				}
				usync := query.GetChildByTag("usync")
				queryFields := usync.GetChildByTag("query")
				fields := queryFields.GetChildren()
				if len(fields) != 1 || fields[0].Tag != "lid" {
					t.Errorf("request fetches unrelated fields: %v", fields)
				}
				userList := usync.GetChildByTag("list")
				users := userList.GetChildren()
				if len(users) != 2 {
					t.Errorf("not a bulk query: %d", len(users))
				}
				for i, user := range users {
					if i < len(pns) && user.Attrs["jid"] != pns[i] {
						t.Errorf("wrong requested user: %v", user.Attrs)
					}
				}
				return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": query.Attrs["id"], "type": "result"}, Content: []waBinary.Node{
					{Tag: "usync", Content: []waBinary.Node{{Tag: "list", Content: []waBinary.Node{
						{Tag: "user", Attrs: waBinary.Attrs{"jid": pns[0]}, Content: []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": want[0]}}}},
						{Tag: "user", Attrs: waBinary.Attrs{"jid": pns[1]}, Content: []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": want[1]}}}},
					}}}},
				}}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, err := client.resolveStatusBroadcastRecipients(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns})
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("audience=%v err=%v", got, err)
			}
			if len(cache.saved) != 2 || cache.saved[0].PN != pns[0] || cache.saved[0].LID != want[0] || cache.saved[1].PN != pns[1] || cache.saved[1].LID != want[1] {
				t.Fatalf("invalid persisted mappings: %v", cache.saved)
			}
		})
	}
}

func TestStatusBroadcastLIDResponseValidation(t *testing.T) {
	pns := statusTestPNs(2)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	lid2 := types.NewJID("90000000002", types.HiddenUserServer)
	user := func(pn types.JID, value any) waBinary.Node {
		return waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": pn}, Content: []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": value}}}}
	}
	legacy := types.NewJID(pns[0].User, types.LegacyUserServer)
	legacy.Device, lid.Device = 3, 4
	for _, tc := range []struct {
		name      string
		users     []waBinary.Node
		wantError bool
	}{
		{"canonical aliases and device suffixes", []waBinary.Node{user(legacy, lid), user(pns[1], lid2)}, false},
		{"duplicate identical mapping", []waBinary.Node{user(pns[0], lid), user(pns[0], lid), user(pns[1], lid2)}, false},
		{"unsolicited users are ignored", []waBinary.Node{user(pns[0], lid), user(pns[1], lid2), user(types.NewJID("79999999999", types.DefaultUserServer), types.EmptyJID)}, false},
		{"invalid user entry", []waBinary.Node{user(pns[0], lid), {Tag: "user"}}, true},
		{"conflicting mapping", []waBinary.Node{user(pns[0], lid), user(pns[0], lid2), user(pns[1], lid2)}, true},
		{"valid and unavailable duplicate conflict", []waBinary.Node{user(pns[0], lid), user(pns[0], types.EmptyJID), user(pns[1], lid2)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newStatusTestClient(nil, nil)
			cache := &statusQueryStore{}
			client.Store.LIDs = cache
			got, err := client.parseStatusBroadcastLIDs(context.Background(), pns, &waBinary.Node{Tag: "list", Content: tc.users})
			if tc.wantError {
				if err == nil || len(got) != 0 || len(cache.saved) != 0 {
					t.Fatalf("unsafe partial cache/result: %v saved=%v err=%v", got, cache.saved, err)
				}
			} else {
				if err != nil || len(got) != 2 || got[pns[0]].LID != lid.ToNonAD() || got[pns[1]].LID != lid2 || len(cache.saved) != 2 {
					t.Fatalf("invalid response normalization: %v saved=%v err=%v", got, cache.saved, err)
				}
			}
		})
	}
}

func TestStatusBroadcastPartialListCachesOnlyReturnedViewer(t *testing.T) {
	pns := statusTestPNs(2)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	client := newStatusTestClient(nil, nil)
	cache := &statusQueryStore{}
	client.Store.LIDs = cache
	got, err := client.parseStatusBroadcastLIDs(context.Background(), pns, &waBinary.Node{Tag: "list", Content: []waBinary.Node{
		{Tag: "user", Attrs: waBinary.Attrs{"jid": pns[0]}, Content: []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": lid}}}},
	}})
	if err != nil || len(got) != 1 || got[pns[0]].LID != lid || len(cache.saved) != 1 || cache.saved[0].LID != lid {
		t.Fatalf("partial response: got %v saved %v err %v", got, cache.saved, err)
	}
}
