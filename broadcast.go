// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"fmt"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func (cli *Client) getBroadcastListParticipants(ctx context.Context, jid types.JID) ([]types.JID, error) {
	if jid != types.StatusBroadcastJID {
		return nil, ErrBroadcastListUnsupported
	}
	if cli.getOwnID().IsEmpty() {
		return nil, ErrNotLoggedIn
	}
	ownID := cli.getOwnLID().ToNonAD()
	if ownID.User == "" || ownID.Server != types.HiddenUserServer {
		return nil, errors.New("own LID is unavailable for status broadcast")
	}
	list, err := cli.getStatusBroadcastRecipients(ctx)
	if err != nil {
		return nil, err
	}

	selfIndex := -1
	for i, participant := range list {
		if participant == ownID {
			selfIndex = i
			break
		}
	}
	if selfIndex < 0 {
		list = append(list, ownID)
	}
	return list, nil
}

func (cli *Client) getStatusBroadcastRecipients(ctx context.Context) ([]types.JID, error) {
	statusPrivacyOptions, err := cli.GetStatusPrivacy(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get status privacy: %w", err)
	}
	return cli.resolveStatusBroadcastRecipients(ctx, statusPrivacyOptions[0])
}

func (cli *Client) resolveStatusBroadcastRecipients(ctx context.Context, statusPrivacy types.StatusPrivacy) ([]types.JID, error) {
	return cli.resolveStatusBroadcastRecipientsWithLookup(ctx, statusPrivacy, cli.getStatusBroadcastLIDs)
}

func (cli *Client) resolveStatusBroadcastRecipientsWithLookup(ctx context.Context, statusPrivacy types.StatusPrivacy, lookup func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error)) ([]types.JID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var candidates, exclusions []types.JID
	switch statusPrivacy.Type {
	case types.StatusPrivacyTypeWhitelist:
		candidates = statusPrivacy.List
	case types.StatusPrivacyTypeContacts, types.StatusPrivacyTypeBlacklist:
		contacts, err := cli.Store.Contacts.GetAllContacts(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get contact list from db: %w", err)
		}
		for jid, contact := range contacts {
			// Keep the saved-contact rule: a push name alone is not a contact.
			if contact.FullName != "" && (jid.Server == types.DefaultUserServer || jid.Server == types.LegacyUserServer || jid.Server == types.HiddenUserServer) {
				candidates = append(candidates, jid)
			}
		}
		if statusPrivacy.Type == types.StatusPrivacyTypeBlacklist {
			exclusions = statusPrivacy.List
		}
	default:
		return nil, fmt.Errorf("unsupported status privacy type %q", statusPrivacy.Type)
	}

	baseJID := func(jid types.JID) types.JID {
		jid = jid.ToNonAD()
		if jid.Server == types.LegacyUserServer {
			jid.Server = types.DefaultUserServer
		}
		return jid
	}
	ownPN, ownLID := cli.getOwnID().ToNonAD(), cli.getOwnLID().ToNonAD()
	var pns []types.JID
	seenPNs := make(map[types.JID]struct{})
	for _, list := range [][]types.JID{candidates, exclusions} {
		for _, jid := range list {
			jid = baseJID(jid)
			if jid.User == "" || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) {
				return nil, errors.New("invalid user identity in status audience")
			}
			if jid.Server == types.DefaultUserServer && !(jid == ownPN && ownLID.User != "" && ownLID.Server == types.HiddenUserServer) {
				if _, seen := seenPNs[jid]; !seen {
					seenPNs[jid] = struct{}{}
					pns = append(pns, jid)
				}
			}
		}
	}
	var lidMap map[types.JID]types.JID
	var unavailable map[types.JID]struct{}
	if len(pns) > 0 {
		if cli.Store.LIDs == nil {
			return nil, errors.New("LID store is unavailable for status audience")
		}
		var err error
		lidMap, err = cli.Store.LIDs.GetManyLIDsForPNs(ctx, pns)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve status audience LIDs: %w", err)
		}
		var missing []types.JID
		for _, pn := range pns {
			lid := lidMap[pn].ToNonAD()
			if lid.User == "" || lid.Server != types.HiddenUserServer {
				missing = append(missing, pn)
			}
		}
		if len(missing) > 0 {
			// One budget for all batches, never extending the caller's deadline.
			lookupCtx, cancel := context.WithTimeout(ctx, statusLIDLookupTimeout)
			defer cancel()
			if lidMap == nil {
				lidMap = make(map[types.JID]types.JID, len(missing))
			}
			cli.Log.Debugf("Recovering status audience LIDs: missing=%d", len(missing))
			for start := 0; start < len(missing); start += statusLIDBatchSize {
				if start > 0 {
					if err = waitStatusLIDQuery(lookupCtx, statusLIDBatchDelay); err != nil {
						return nil, fmt.Errorf("status audience LID recovery canceled: %w", err)
					}
				}
				if err = lookupCtx.Err(); err != nil {
					return nil, fmt.Errorf("status audience LID recovery canceled: %w", err)
				}
				batch := missing[start:min(start+statusLIDBatchSize, len(missing))]
				var info map[types.JID]types.UserInfo
				info, err = cli.lookupStatusBroadcastLIDBatch(lookupCtx, batch, lookup)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch missing status audience LIDs: %w", err)
				}
				if err = lookupCtx.Err(); err != nil {
					return nil, fmt.Errorf("status audience LID recovery canceled: %w", err)
				}
				for _, pn := range batch {
					user, returned := info[pn]
					if !returned {
						return nil, errors.New("WhatsApp returned an incomplete status audience LID list")
					}
					lid := user.LID.ToNonAD()
					if lid.User == "" || lid.Server != types.HiddenUserServer {
						if unavailable == nil {
							unavailable = make(map[types.JID]struct{})
						}
						unavailable[pn] = struct{}{}
						continue
					}
					// The query may succeed even if persisting its cache failed.
					// Use the validated response for this send, not another cache read.
					lidMap[pn] = lid
				}
			}
		}
	}
	resolve := func(jid types.JID) (types.JID, error) {
		jid = baseJID(jid)
		if jid.Server == types.DefaultUserServer {
			if jid == ownPN {
				jid = ownLID
			} else {
				jid = lidMap[jid].ToNonAD()
			}
		}
		if jid.User == "" || jid.Server != types.HiddenUserServer {
			return types.EmptyJID, errors.New("missing or invalid LID mapping for status audience")
		}
		return jid, nil
	}
	blacklist := make(map[types.JID]struct{}, len(exclusions))
	for _, jid := range exclusions {
		lid, err := resolve(jid)
		if err != nil {
			return nil, fmt.Errorf("cannot resolve status privacy exclusion: %w", err)
		}
		blacklist[lid] = struct{}{}
	}
	seen := make(map[types.JID]struct{}, len(candidates))
	var skipped map[types.JID]struct{}
	hasOtherRecipient := false
	var recipients []types.JID
	for _, jid := range candidates {
		lid, err := resolve(jid)
		if err != nil {
			pn := baseJID(jid)
			if _, individuallyUnavailable := unavailable[pn]; !individuallyUnavailable {
				return nil, fmt.Errorf("cannot resolve status recipient: %w", err)
			}
			if skipped == nil {
				skipped = make(map[types.JID]struct{})
			}
			if _, logged := skipped[pn]; !logged {
				cli.Log.Warnf("Skipping unresolved status recipient: recipient=%s reason=missing_or_invalid_lid", pn)
				skipped[pn] = struct{}{}
			}
			continue
		}
		if _, excluded := blacklist[lid]; excluded {
			continue
		}
		if _, duplicate := seen[lid]; !duplicate {
			seen[lid] = struct{}{}
			recipients = append(recipients, lid)
			if lid != ownLID {
				hasOtherRecipient = true
			}
		}
	}
	if len(skipped) > 0 {
		cli.Log.Warnf("Status audience has skipped recipients: skipped=%d recipients=%d", len(skipped), len(recipients))
		if !hasOtherRecipient {
			return nil, errors.New("no resolvable external status recipients remain after skipping unavailable viewers")
		}
	}
	cli.Log.Debugf("Status broadcast audience: privacy=%s candidates=%d recipients=%d exclusions=%d skipped=%d", statusPrivacy.Type, len(candidates), len(recipients), len(exclusions), len(skipped))
	return recipients, nil
}

var DefaultStatusPrivacy = []types.StatusPrivacy{{
	Type:      types.StatusPrivacyTypeContacts,
	IsDefault: true,
}}

// GetStatusPrivacy gets the user's status privacy settings (who to send status broadcasts to).
//
// There can be multiple different stored settings, the first one is always the default.
func (cli *Client) GetStatusPrivacy(ctx context.Context) ([]types.StatusPrivacy, error) {
	resp, err := cli.sendIQ(ctx, infoQuery{
		Namespace: "status",
		Type:      iqGet,
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "privacy",
		}},
	})
	if err != nil {
		if errors.Is(err, ErrIQNotFound) {
			return DefaultStatusPrivacy, nil
		}
		return nil, err
	}
	privacyLists := resp.GetChildByTag("privacy")
	var outputs []types.StatusPrivacy
	for _, list := range privacyLists.GetChildren() {
		if list.Tag != "list" {
			continue
		}

		ag := list.AttrGetter()
		var out types.StatusPrivacy
		out.IsDefault = ag.OptionalBool("default")
		out.Type = types.StatusPrivacyType(ag.String("type"))
		children := list.GetChildren()
		if len(children) > 0 {
			out.List = make([]types.JID, 0, len(children))
			for _, child := range children {
				jid, ok := child.Attrs["jid"].(types.JID)
				if child.Tag == "user" && ok {
					out.List = append(out.List, jid)
				}
			}
		}
		outputs = append(outputs, out)
		if out.IsDefault {
			// Move default to always be first in the list
			outputs[len(outputs)-1] = outputs[0]
			outputs[0] = out
		}
		if len(ag.Errors) > 0 {
			return nil, ag.Error()
		}
	}
	if len(outputs) == 0 {
		return DefaultStatusPrivacy, nil
	}
	return outputs, nil
}
