// Copyright (c) 2026 Wappi
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// Local policy, not a published WhatsApp rate limit. Only status LID recovery
// uses these budgets: ordinary user-info queries and sends are unchanged.
const (
	statusLIDBatchSize      = 100
	statusLIDLookupTimeout  = 30 * time.Second
	statusLIDAttemptTimeout = 10 * time.Second
	statusLIDBatchDelay     = time.Second
	statusLIDRateLimitDelay = 10 * time.Second
)

func waitStatusLIDQuery(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func (cli *Client) lookupStatusBroadcastLIDBatch(ctx context.Context, pns []types.JID, lookup func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error)) (map[types.JID]types.UserInfo, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The deadline timer's goroutine may not have run yet at this boundary.
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		attemptCtx, cancel := context.WithTimeout(ctx, statusLIDAttemptTimeout)
		if err := attemptCtx.Err(); err != nil {
			cancel()
			return nil, err
		}
		info, err := lookup(attemptCtx, pns)
		if ctxErr := attemptCtx.Err(); ctxErr != nil {
			err = ctxErr
		}
		cancel()
		if err == nil {
			return info, nil
		}
		if attempt > 0 || !errors.Is(err, ErrIQRateOverLimit) {
			return nil, err
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= statusLIDRateLimitDelay {
			return nil, err // No time for the cooldown and any useful retry.
		}
		cli.Log.Warnf("Status audience LID query rate-limited: count=%d retry_delay=%s", len(pns), statusLIDRateLimitDelay)
		if err = waitStatusLIDQuery(ctx, statusLIDRateLimitDelay); err != nil {
			return nil, err
		}
	}
}

func (cli *Client) getStatusBroadcastLIDs(ctx context.Context, pns []types.JID) (map[types.JID]types.UserInfo, error) {
	// Preserve the existing background USync mode but request only LID mappings:
	// no profile/status/device data or per-contact privacy-token lookups.
	list, err := cli.usync(ctx, pns, "full", "background", []waBinary.Node{{Tag: "lid"}})
	if err != nil {
		return nil, err
	}
	return cli.parseStatusBroadcastLIDs(ctx, pns, list)
}

func (cli *Client) parseStatusBroadcastLIDs(ctx context.Context, pns []types.JID, list *waBinary.Node) (map[types.JID]types.UserInfo, error) {
	requested := make(map[types.JID]struct{}, len(pns))
	for _, pn := range pns {
		requested[pn] = struct{}{}
	}
	info := make(map[types.JID]types.UserInfo, len(pns))
	for _, user := range list.GetChildren() {
		pn, ok := user.Attrs["jid"].(types.JID)
		if user.Tag != "user" || !ok {
			continue
		}
		pn = pn.ToNonAD()
		if pn.Server == types.LegacyUserServer {
			pn.Server = types.DefaultUserServer
		}
		if _, wanted := requested[pn]; !wanted {
			continue
		}
		var lid types.JID
		if _, hasError := user.GetOptionalChildByTag("error"); hasError {
			userErr := parseIQError(&user)
			var iqErr *IQError
			if !errors.As(userErr, &iqErr) || (iqErr.Code != 400 && iqErr.Code != 404) {
				return nil, userErr
			}
			cli.Log.Warnf("Status audience identity unavailable: recipient=%s code=%d", pn, iqErr.Code)
		} else {
			lidNode := user.GetChildByTag("lid")
			ag := lidNode.AttrGetter()
			lid = ag.OptionalJIDOrEmpty("val").ToNonAD()
			if len(ag.Errors) > 0 || lid.User == "" || lid.Server != types.HiddenUserServer {
				lid = types.EmptyJID
			}
		}
		if previous, exists := info[pn]; exists && previous.LID != lid {
			return nil, errors.New("WhatsApp returned conflicting LIDs for status audience")
		}
		// Presence with an empty LID is an individual failure; absence remains
		// an incomplete batch. Only the resolver knows whether this is a viewer
		// that can be skipped or a privacy exclusion that must stop the send.
		info[pn] = types.UserInfo{LID: lid}
	}
	mappings := make([]store.LIDMapping, 0, len(info))
	for _, pn := range pns {
		if _, exists := info[pn]; !exists {
			return nil, errors.New("WhatsApp returned an incomplete status audience LID list")
		}
		if !info[pn].LID.IsEmpty() {
			mappings = append(mappings, store.LIDMapping{PN: pn, LID: info[pn].LID})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(mappings) > 0 {
		if err := cli.Store.LIDs.PutManyLIDMappings(ctx, mappings); err != nil {
			// The validated response remains usable for this send, even if caching fails.
			cli.Log.Warnf("Failed to cache status audience LIDs: count=%d error=%v", len(mappings), err)
		}
	}
	return info, nil
}
