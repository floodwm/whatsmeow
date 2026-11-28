// Copyright (c) 2026 Wappi
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestStatusBroadcastRecoversLIDsBeforeApplyingPrivacy(t *testing.T) {
	pn := types.NewJID("70000000001", types.DefaultUserServer)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	otherPN := types.NewJID("70000000002", types.DefaultUserServer)
	otherLID := types.NewJID("90000000002", types.HiddenUserServer)
	named := types.ContactInfo{FullName: "Saved"}
	for _, tc := range []struct {
		name     string
		privacy  types.StatusPrivacy
		contacts map[types.JID]types.ContactInfo
		cached   map[types.JID]types.JID
		want     []types.JID
	}{
		{"whitelist recovers and deduplicates PN/LID", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn, lid, pn}}, nil, nil, []types.JID{lid}},
		{"recovers selected contact", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{otherLID}}, map[types.JID]types.ContactInfo{pn: named, otherPN: named}, map[types.JID]types.JID{otherPN: otherLID}, []types.JID{lid}},
		{"recovers exclusion before filtering aliases", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{pn}}, map[types.JID]types.ContactInfo{lid: named, otherLID: named}, nil, []types.JID{otherLID}},
		{"invalid cache entry is refreshed", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn}}, nil, map[types.JID]types.JID{pn: pn}, []types.JID{lid}},
		{"empty cache user is refreshed", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn}}, nil, map[types.JID]types.JID{pn: types.NewJID("", types.HiddenUserServer)}, []types.JID{lid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newStatusTestClient(tc.contacts, tc.cached)
			// The returned mapping is usable even when the cache remains unchanged.
			got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), tc.privacy, func(ctx context.Context, pns []types.JID) (map[types.JID]types.UserInfo, error) {
				if !slices.Equal(pns, []types.JID{pn}) {
					t.Fatalf("requested %v; only the missing PN should be queried", pns)
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 30*time.Second {
					t.Fatal("recovery lacks the total 30-second budget")
				}
				return map[types.JID]types.UserInfo{pn: {LID: lid}, otherPN: {LID: otherLID}}, nil
			})
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("audience=%v err=%v, want %v", got, err, tc.want)
			}
		})
	}
}

func TestStatusBroadcastRecoveryPreservesQueryAndPrivacyErrors(t *testing.T) {
	pn := types.NewJID("70000000001", types.DefaultUserServer)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	lookupErr := errors.New("USync failed")
	for _, tc := range []struct {
		name   string
		result map[types.JID]types.UserInfo
		err    error
	}{
		{"missing result", nil, nil},
		{"wrong PN", map[types.JID]types.UserInfo{types.NewJID("79999999999", types.DefaultUserServer): {LID: lid}}, nil},
		{"PN instead of LID", map[types.JID]types.UserInfo{pn: {LID: pn}}, nil},
		{"empty LID user", map[types.JID]types.UserInfo{pn: {LID: types.NewJID("", types.HiddenUserServer)}}, nil},
		{"network error", nil, lookupErr},
	} {
		for _, mode := range []types.StatusPrivacyType{types.StatusPrivacyTypeWhitelist, types.StatusPrivacyTypeBlacklist} {
			t.Run(tc.name+string(mode), func(t *testing.T) {
				client := newStatusTestClient(map[types.JID]types.ContactInfo{lid: {FullName: "Saved"}}, nil)
				got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: mode, List: []types.JID{pn, lid}}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) { return tc.result, tc.err })
				if tc.result != nil && tc.err == nil && mode == types.StatusPrivacyTypeWhitelist {
					if err != nil || !slices.Equal(got, []types.JID{lid}) {
						t.Fatalf("individual viewer failure blocked the valid peer: %v %v", got, err)
					}
					return
				}
				if err == nil || len(got) != 0 || tc.err != nil && !errors.Is(err, tc.err) {
					t.Fatalf("unsafe result: %v, %v", got, err)
				}
			})
		}
	}
}

func TestStatusBroadcastRecoveryBatchesAndDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var pns []types.JID
		want := make([]types.JID, 0, 121)
		for i := range 121 {
			pns = append(pns, types.NewJID(fmt.Sprintf("7%010d", i+1), types.DefaultUserServer))
			want = append(want, types.NewJID(fmt.Sprintf("9%010d", i+1), types.HiddenUserServer))
		}
		client := newStatusTestClient(nil, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		parentDeadline, _ := ctx.Deadline()
		var deadlines []time.Time
		var batches []int
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns}, func(ctx context.Context, batch []types.JID) (map[types.JID]types.UserInfo, error) {
			deadline, _ := ctx.Deadline()
			deadlines = append(deadlines, deadline)
			batches = append(batches, len(batch))
			result := make(map[types.JID]types.UserInfo)
			for _, pn := range batch {
				result[pn] = types.UserInfo{LID: types.NewJID("9"+pn.User[1:], types.HiddenUserServer)}
			}
			return result, nil
		})
		if err != nil || !slices.Equal(got, want) || !slices.Equal(batches, []int{100, 21}) {
			t.Fatalf("got %d recipients, batches=%v err=%v", len(got), batches, err)
		}
		for _, deadline := range deadlines {
			if !deadline.Equal(parentDeadline) {
				t.Fatal("batch reset/extended the caller deadline")
			}
		}
	})
}

func TestStatusBroadcastRecoveryCancellationStopsFurtherBatches(t *testing.T) {
	var pns []types.JID
	for i := range 101 {
		pns = append(pns, types.NewJID(fmt.Sprintf("7%010d", i+1), types.DefaultUserServer))
	}
	client := newStatusTestClient(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	count := 0
	got, err := client.resolveStatusBroadcastRecipientsWithLookup(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
		count++
		cancel()
		return nil, context.Canceled
	})
	if len(got) != 0 || !errors.Is(err, context.Canceled) || count != 1 {
		t.Fatalf("audience=%v err=%v calls=%d", got, err, count)
	}
}

func TestStatusBroadcastCachedAudienceSkipsNetwork(t *testing.T) {
	pn := types.NewJID("70000000001", types.DefaultUserServer)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	client := newStatusTestClient(nil, map[types.JID]types.JID{pn: lid})
	got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn, lid}}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
		t.Fatal("cached audience must not query WhatsApp")
		return nil, nil
	})
	if err != nil || !slices.Equal(got, []types.JID{lid}) {
		t.Fatalf("audience=%v err=%v", got, err)
	}
}

func TestStatusBroadcastRecoveryDistinguishesOmittedUsersFromBatchFailure(t *testing.T) {
	var pns []types.JID
	for i := range 101 {
		pns = append(pns, types.NewJID(fmt.Sprintf("7%010d", i+1), types.DefaultUserServer))
	}
	for _, failure := range []string{"partial batch", "later batch error", "deadline expires after response"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := newStatusTestClient(nil, nil)
				ctx := context.Background()
				if failure == "deadline expires after response" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
					defer cancel()
				}
				calls := 0
				got, err := client.resolveStatusBroadcastRecipientsWithLookup(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns}, func(ctx context.Context, batch []types.JID) (map[types.JID]types.UserInfo, error) {
					calls++
					result := make(map[types.JID]types.UserInfo)
					for _, pn := range batch {
						result[pn] = types.UserInfo{LID: types.NewJID("9"+pn.User[1:], types.HiddenUserServer)}
					}
					if failure == "partial batch" {
						delete(result, batch[len(batch)-1])
					} else if failure == "later batch error" && calls == 2 {
						return nil, ErrNotConnected
					} else if failure == "deadline expires after response" {
						<-ctx.Done()
					}
					return result, nil
				})
				if failure == "partial batch" {
					if err != nil || calls != 2 || len(got) != 99 {
						t.Fatalf("valid peers lost: recipients=%d calls=%d err=%v", len(got), calls, err)
					}
					return
				}
				if err == nil || len(got) != 0 {
					t.Fatalf("partial audience leaked: %v err=%v", got, err)
				}
				if failure == "later batch error" && (!errors.Is(err, ErrNotConnected) || calls != 2) {
					t.Fatalf("late failure misclassified: calls=%d err=%v", calls, err)
				}
				if failure == "deadline expires after response" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expired result accepted/misclassified: %v", err)
				}
			})
		})
	}
}
