// Copyright (c) 2026 Wappi
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

type statusContactsStore struct {
	store.ContactStore
	contacts map[types.JID]types.ContactInfo
	err      error
}

func (s statusContactsStore) GetAllContacts(context.Context) (map[types.JID]types.ContactInfo, error) {
	return s.contacts, s.err
}

type statusLIDStore struct {
	store.LIDStore
	mappings map[types.JID]types.JID
	err      error
}

func (s statusLIDStore) GetManyLIDsForPNs(ctx context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.err != nil {
		return nil, s.err
	}
	result := make(map[types.JID]types.JID)
	for _, pn := range pns {
		if lid, ok := s.mappings[pn]; ok {
			result[pn] = lid
		}
	}
	return result, nil
}

func newStatusTestClient(contacts map[types.JID]types.ContactInfo, mappings map[types.JID]types.JID) *Client {
	own := types.NewJID("70000000000", types.DefaultUserServer)
	return NewClient(&store.Device{
		ID:       &own,
		LID:      types.NewJID("90000000000", types.HiddenUserServer),
		Contacts: statusContactsStore{contacts: contacts},
		LIDs:     statusLIDStore{mappings: mappings},
	}, nil)
}

func TestStatusBroadcastRecipientsRespectPrivacyAndUseUniqueLIDs(t *testing.T) {
	pn := types.NewJID("70000000001", types.DefaultUserServer)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	otherPN := types.NewJID("70000000002", types.DefaultUserServer)
	otherLID := types.NewJID("90000000002", types.HiddenUserServer)
	pnDevice, lidDevice := pn, lid
	pnDevice.Device, lidDevice.Device = 4, 7
	legacyPN := types.NewJID(pn.User, types.LegacyUserServer)
	mappings := map[types.JID]types.JID{pn: lid, otherPN: otherLID}
	named := types.ContactInfo{FullName: "Saved contact"}
	for _, tc := range []struct {
		name     string
		privacy  types.StatusPrivacy
		contacts map[types.JID]types.ContactInfo
		want     []types.JID
	}{
		{"PN contacts", types.StatusPrivacy{Type: types.StatusPrivacyTypeContacts}, map[types.JID]types.ContactInfo{pn: named}, []types.JID{lid}},
		{"LID contacts", types.StatusPrivacy{Type: types.StatusPrivacyTypeContacts}, map[types.JID]types.ContactInfo{lid: named}, []types.JID{lid}},
		{"mixed aliases and devices", types.StatusPrivacy{Type: types.StatusPrivacyTypeContacts}, map[types.JID]types.ContactInfo{pn: named, lid: named, pnDevice: named, lidDevice: named, legacyPN: named, otherPN: named}, []types.JID{lid, otherLID}},
		{"push names not saved contacts", types.StatusPrivacy{Type: types.StatusPrivacyTypeContacts}, map[types.JID]types.ContactInfo{pn: named, otherPN: {PushName: "Not saved"}}, []types.JID{lid}},
		{"whitelist ignores contact store", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn, lid, lidDevice, legacyPN}}, map[types.JID]types.ContactInfo{otherPN: named}, []types.JID{lid}},
		{"empty whitelist stays empty", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist}, map[types.JID]types.ContactInfo{otherPN: named}, nil},
		{"PN blacklist excludes LID and PN aliases", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{pnDevice}}, map[types.JID]types.ContactInfo{pn: named, lidDevice: named, otherLID: named}, []types.JID{otherLID}},
		{"LID blacklist excludes PN alias", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{lidDevice}}, map[types.JID]types.ContactInfo{pnDevice: named, lid: named, otherPN: named}, []types.JID{otherLID}},
		{"legacy blacklist excludes LID alias", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{legacyPN}}, map[types.JID]types.ContactInfo{lid: named, otherLID: named}, []types.JID{otherLID}},
		{"own contact uses known device LID", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{types.NewJID("70000000000", types.DefaultUserServer)}}, nil, []types.JID{types.NewJID("90000000000", types.HiddenUserServer)}},
		{"non-user contact entries are not recipients", types.StatusPrivacy{Type: types.StatusPrivacyTypeContacts}, map[types.JID]types.ContactInfo{pn: named, types.NewJID("123", types.GroupServer): named}, []types.JID{lid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newStatusTestClient(tc.contacts, mappings)
			before := slices.Clone(tc.privacy.List)
			got, err := client.resolveStatusBroadcastRecipients(context.Background(), tc.privacy)
			if err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(got, func(a, b types.JID) int { return strings.Compare(a.String(), b.String()) })
			slices.SortFunc(tc.want, func(a, b types.JID) int { return strings.Compare(a.String(), b.String()) })
			if !slices.Equal(got, tc.want) {
				t.Fatalf("audience = %v, want %v", got, tc.want)
			}
			if !reflect.DeepEqual(tc.privacy.List, before) {
				t.Fatal("privacy list was mutated")
			}
		})
	}
}

func TestStatusBroadcastRecipientsFailClosed(t *testing.T) {
	pn := types.NewJID("70000000001", types.DefaultUserServer)
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	storeErr := errors.New("mapping store unavailable")
	contactsErr := errors.New("contacts store unavailable")
	for _, tc := range []struct {
		name                    string
		privacy                 types.StatusPrivacy
		mappings                map[types.JID]types.JID
		mappingErr, contactsErr error
		wantErr                 error
	}{
		{"missing selected mapping", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn, lid}}, nil, nil, nil, nil},
		{"missing excluded mapping", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{pn}}, nil, nil, nil, nil},
		{"invalid mapping domain", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn}}, map[types.JID]types.JID{pn: pn}, nil, nil, nil},
		{"empty mapping user", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn}}, map[types.JID]types.JID{pn: types.NewJID("", types.HiddenUserServer)}, nil, nil, nil},
		{"mapping store error", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{pn}}, nil, storeErr, nil, storeErr},
		{"contact store error", types.StatusPrivacy{Type: types.StatusPrivacyTypeContacts}, nil, nil, contactsErr, contactsErr},
		{"unknown privacy", types.StatusPrivacy{Type: "unknown"}, nil, nil, nil, nil},
		{"invalid selected JID", types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{types.NewJID("123", types.GroupServer)}}, nil, nil, nil, nil},
		{"invalid excluded JID", types.StatusPrivacy{Type: types.StatusPrivacyTypeBlacklist, List: []types.JID{types.NewJID("123", types.GroupServer)}}, nil, nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newStatusTestClient(map[types.JID]types.ContactInfo{lid: {FullName: "Saved"}}, tc.mappings)
			client.Store.LIDs = statusLIDStore{mappings: tc.mappings, err: tc.mappingErr}
			client.Store.Contacts = statusContactsStore{contacts: map[types.JID]types.ContactInfo{lid: {FullName: "Saved"}}, err: tc.contactsErr}
			got, err := client.resolveStatusBroadcastRecipients(context.Background(), tc.privacy)
			if err == nil || len(got) != 0 || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("unsafe or misclassified result: audience=%v error=%v", got, err)
			}
		})
	}
	client := newStatusTestClient(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := client.resolveStatusBroadcastRecipients(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: []types.JID{lid}}); !errors.Is(err, context.Canceled) || len(got) != 0 {
		t.Fatalf("canceled lookup: audience=%v error=%v", got, err)
	}
}

func TestStatusBroadcastRejectsOtherBroadcastLists(t *testing.T) {
	client := newStatusTestClient(nil, nil)
	got, err := client.getBroadcastListParticipants(context.Background(), types.NewJID("123", types.BroadcastServer))
	if !errors.Is(err, ErrBroadcastListUnsupported) || len(got) != 0 {
		t.Fatalf("unsupported broadcast changed: audience=%v error=%v", got, err)
	}
}

func TestStatusBroadcastLIDAudienceDoesNotRequirePNOrContactLookup(t *testing.T) {
	lid := types.NewJID("90000000001", types.HiddenUserServer)
	client := newStatusTestClient(nil, nil)
	storeErr := errors.New("store must not be used for an explicit LID audience")
	client.Store.Contacts = statusContactsStore{err: storeErr}
	client.Store.LIDs = statusLIDStore{err: storeErr}
	got, err := client.resolveStatusBroadcastRecipients(context.Background(), types.StatusPrivacy{
		Type: types.StatusPrivacyTypeWhitelist,
		List: []types.JID{lid},
	})
	if err != nil || !slices.Equal(got, []types.JID{lid}) {
		t.Fatalf("explicit LID audience: got=%v err=%v", got, err)
	}
}

func TestStatusBroadcastMissingIdentityDoesNotAttemptPrivacyRequest(t *testing.T) {
	client := newStatusTestClient(nil, nil)
	client.Store.ID = nil
	if got, err := client.getBroadcastListParticipants(context.Background(), types.StatusBroadcastJID); !errors.Is(err, ErrNotLoggedIn) || len(got) != 0 {
		t.Fatalf("missing session: got=%v err=%v", got, err)
	}
	client = newStatusTestClient(nil, nil)
	client.Store.LID = types.EmptyJID
	got, err := client.getBroadcastListParticipants(context.Background(), types.StatusBroadcastJID)
	if err == nil || errors.Is(err, ErrNotConnected) || errors.Is(err, ErrNotLoggedIn) || len(got) != 0 {
		t.Fatalf("missing LID must fail before network without declaring logout: got=%v err=%v", got, err)
	}
}

func TestStatusBroadcastMissingLIDStoreFailsRatherThanSendingMixedIDs(t *testing.T) {
	client := newStatusTestClient(nil, nil)
	client.Store.LIDs = nil
	got, err := client.resolveStatusBroadcastRecipients(context.Background(), types.StatusPrivacy{
		Type: types.StatusPrivacyTypeWhitelist,
		List: []types.JID{types.NewJID("70000000001", types.DefaultUserServer)},
	})
	if err == nil || len(got) != 0 {
		t.Fatalf("missing store: got=%v err=%v", got, err)
	}
}
