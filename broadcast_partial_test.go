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
	"strings"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type statusPartialLog struct {
	waLog.Logger
	warnings []string
}

func (l *statusPartialLog) Warnf(format string, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

func TestStatusBroadcastSkipsOnlyIdentifiedUnavailableViewers(t *testing.T) {
	pns := statusTestPNs(2)
	goodLID := types.NewJID("90000000002", types.HiddenUserServer)
	for _, mode := range []types.StatusPrivacyType{types.StatusPrivacyTypeContacts, types.StatusPrivacyTypeWhitelist, types.StatusPrivacyTypeBlacklist} {
		t.Run(string(mode), func(t *testing.T) {
			client := newStatusTestClient(map[types.JID]types.ContactInfo{pns[0]: {FullName: "Unavailable"}, pns[1]: {FullName: "Available"}}, nil)
			privacy := types.StatusPrivacy{Type: mode}
			if mode == types.StatusPrivacyTypeWhitelist {
				privacy.List = pns
			}
			if mode == types.StatusPrivacyTypeBlacklist {
				privacy.List = []types.JID{types.NewJID("99999999999", types.HiddenUserServer)}
			}
			got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), privacy, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
				return map[types.JID]types.UserInfo{pns[0]: {}, pns[1]: {LID: goodLID}}, nil
			})
			if err != nil || !slices.Equal(got, []types.JID{goodLID}) {
				t.Fatalf("audience=%v err=%v", got, err)
			}
		})
	}
}

func TestStatusBroadcastUnavailableExclusionStillStopsSend(t *testing.T) {
	pn := statusTestPNs(1)[0]
	good := types.NewJID("90000000002", types.HiddenUserServer)
	client := newStatusTestClient(map[types.JID]types.ContactInfo{good: {FullName: "Saved"}}, nil)
	got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{pn}}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
		return map[types.JID]types.UserInfo{pn: {}}, nil
	})
	if err == nil || len(got) != 0 || !strings.Contains(err.Error(), "exclusion") {
		t.Fatalf("privacy exclusion ignored: %v %v", got, err)
	}
}

func TestStatusBroadcastSkippedViewerDoesNotBypassResolvedExclusions(t *testing.T) {
	pns := statusTestPNs(3)
	good := types.NewJID("90000000002", types.HiddenUserServer)
	excluded := types.NewJID("90000000003", types.HiddenUserServer)
	named := types.ContactInfo{FullName: "Saved"}
	client := newStatusTestClient(map[types.JID]types.ContactInfo{pns[0]: named, pns[1]: named, pns[2]: named, excluded: named}, nil)
	got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: pns[2:]}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
		return map[types.JID]types.UserInfo{pns[0]: {}, pns[1]: {LID: good}, pns[2]: {LID: excluded}}, nil
	})
	if err != nil || !slices.Equal(got, []types.JID{good}) {
		t.Fatalf("PN/LID exclusion bypassed: %v %v", got, err)
	}
}

func TestStatusBroadcastAllSkippedDoesNotProduceSelfOnlySuccess(t *testing.T) {
	pn := statusTestPNs(1)[0]
	for _, includeSelf := range []bool{false, true} {
		t.Run(fmt.Sprint(includeSelf), func(t *testing.T) {
			client := newStatusTestClient(nil, nil)
			list := []types.JID{pn}
			if includeSelf {
				list = append(list, client.getOwnLID().ToNonAD())
			}
			got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: list}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
				return map[types.JID]types.UserInfo{pn: {}}, nil
			})
			if err == nil || len(got) != 0 || !strings.Contains(err.Error(), "no resolvable external") {
				t.Fatalf("empty audience reported as success: %v %v", got, err)
			}
		})
	}
}

func TestStatusBroadcastUnavailableViewerDoesNotLoseCachedPeers(t *testing.T) {
	pns := statusTestPNs(2)
	good := types.NewJID("90000000002", types.HiddenUserServer)
	client := newStatusTestClient(nil, map[types.JID]types.JID{pns[0]: pns[0], pns[1]: good})
	got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns}, func(_ context.Context, batch []types.JID) (map[types.JID]types.UserInfo, error) {
		if !slices.Equal(batch, pns[:1]) {
			t.Fatalf("cached peer unnecessarily queried: %v", batch)
		}
		return map[types.JID]types.UserInfo{pns[0]: {}}, nil
	})
	if err != nil || !slices.Equal(got, []types.JID{good}) {
		t.Fatalf("cached peer lost: %v %v", got, err)
	}
}

func TestStatusBroadcastUnavailableAliasesLoggedOnce(t *testing.T) {
	pn := statusTestPNs(1)[0]
	devicePN := pn
	devicePN.Device = 3
	legacy := types.NewJID(pn.User, types.LegacyUserServer)
	good := types.NewJID("90000000002", types.HiddenUserServer)
	client := newStatusTestClient(nil, nil)
	logger := &statusPartialLog{Logger: waLog.Noop}
	client.Log = logger
	got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn, devicePN, legacy, good}}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
		return map[types.JID]types.UserInfo{pn: {}}, nil
	})
	if err != nil || !slices.Equal(got, []types.JID{good}) {
		t.Fatalf("audience=%v err=%v", got, err)
	}
	identityLogs := 0
	for _, line := range logger.warnings {
		if strings.Contains(line, pn.String()) {
			identityLogs++
		}
	}
	if identityLogs != 1 || !strings.Contains(strings.Join(logger.warnings, "\n"), "skipped=1") {
		t.Fatalf("missing or duplicate skip diagnostics: %v", logger.warnings)
	}
}

func TestStatusBroadcastPerUserFailureKeepsValidCacheMappings(t *testing.T) {
	pns := statusTestPNs(2)
	goodLID := types.NewJID("90000000002", types.HiddenUserServer)
	good := waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": pns[1]}, Content: []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": goodLID}}}}
	for _, tc := range []struct {
		name    string
		content []waBinary.Node
		wantErr error
	}{
		{"missing lid", nil, nil},
		{"empty lid", []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": types.EmptyJID}}}, nil},
		{"invalid lid server", []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": pns[0]}}}, nil},
		{"malformed lid", []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": "invalid"}}}, nil},
		{"individual 400", []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": "400", "text": "bad-request"}}}, nil},
		{"individual 404", []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": "404", "text": "item-not-found"}}}, nil},
		{"individual 429 remains fatal", []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": "429", "text": "rate-overlimit"}}}, ErrIQRateOverLimit},
		{"individual 500 remains fatal", []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": "500", "text": "internal-server-error"}}}, ErrIQInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newStatusTestClient(nil, nil)
			cache := &statusQueryStore{}
			client.Store.LIDs = cache
			list := &waBinary.Node{Tag: "list", Content: []waBinary.Node{good, {Tag: "user", Attrs: waBinary.Attrs{"jid": pns[0]}, Content: tc.content}}}
			got, err := client.parseStatusBroadcastLIDs(context.Background(), pns, list)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || len(got) != 0 || len(cache.saved) != 0 {
					t.Fatalf("query error suppressed: %v %v cache=%v", got, err, cache.saved)
				}
				return
			}
			bad, identified := got[pns[0]]
			if err != nil || !identified || !bad.LID.IsEmpty() || got[pns[1]].LID != goodLID || len(cache.saved) != 1 || cache.saved[0].PN != pns[1] || cache.saved[0].LID != goodLID {
				t.Fatalf("individual failure lost valid peers or poisoned cache: %v %v cache=%v", got, err, cache.saved)
			}
		})
	}
}

func TestStatusBroadcastTransportDistinguishesUserAndBatch400(t *testing.T) {
	for _, wholeBatch := range []bool{false, true} {
		t.Run(fmt.Sprint(wholeBatch), func(t *testing.T) {
			pns := statusTestPNs(2)
			goodLID := types.NewJID("90000000002", types.HiddenUserServer)
			client := newStatusTestClient(nil, nil)
			cache := &statusQueryStore{}
			client.Store.LIDs = cache
			attachStatusQueryServer(t, client, func(query *waBinary.Node) waBinary.Node {
				errorNode := waBinary.Node{Tag: "error", Attrs: waBinary.Attrs{"code": "400", "text": "bad-request"}}
				if wholeBatch {
					return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": query.Attrs["id"], "type": "error"}, Content: []waBinary.Node{errorNode}}
				}
				return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": query.Attrs["id"], "type": "result"}, Content: []waBinary.Node{{Tag: "usync", Content: []waBinary.Node{{Tag: "list", Content: []waBinary.Node{
					{Tag: "user", Attrs: waBinary.Attrs{"jid": pns[0]}, Content: []waBinary.Node{errorNode}},
					{Tag: "user", Attrs: waBinary.Attrs{"jid": pns[1]}, Content: []waBinary.Node{{Tag: "lid", Attrs: waBinary.Attrs{"val": goodLID}}}},
				}}}}}}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, err := client.resolveStatusBroadcastRecipients(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns})
			if wholeBatch {
				if !errors.Is(err, ErrIQBadRequest) || len(got) != 0 || len(cache.saved) != 0 {
					t.Fatalf("batch 400 suppressed: %v %v", got, err)
				}
			} else if err != nil || !slices.Equal(got, []types.JID{goodLID}) || len(cache.saved) != 1 {
				t.Fatalf("individual 400 blocked remaining recipient: %v %v cache=%v", got, err, cache.saved)
			}
		})
	}
}
