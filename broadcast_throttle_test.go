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

func statusTestPNs(count int) []types.JID {
	pns := make([]types.JID, count)
	for i := range pns {
		pns[i] = types.NewJID(fmt.Sprintf("7%010d", i+1), types.DefaultUserServer)
	}
	return pns
}

func statusTestLIDResponse(pns []types.JID) (map[types.JID]types.UserInfo, error) {
	result := make(map[types.JID]types.UserInfo, len(pns))
	for _, pn := range pns {
		result[pn] = types.UserInfo{LID: types.NewJID("9"+pn.User[1:], types.HiddenUserServer)}
	}
	return result, nil
}

func TestStatusBroadcastBulkQueryPacing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pns := statusTestPNs(201)
		client := newStatusTestClient(nil, nil)
		start := time.Now()
		var sizes []int
		var starts []time.Duration
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns}, func(ctx context.Context, batch []types.JID) (map[types.JID]types.UserInfo, error) {
			deadline, ok := ctx.Deadline()
			if !ok || deadline.Sub(time.Now()) != 10*time.Second {
				t.Fatalf("attempt budget=%v, want 10s", deadline.Sub(time.Now()))
			}
			sizes = append(sizes, len(batch))
			starts = append(starts, time.Since(start))
			return statusTestLIDResponse(batch)
		})
		if err != nil || len(got) != 201 || !slices.Equal(sizes, []int{100, 100, 1}) || !slices.Equal(starts, []time.Duration{0, time.Second, 2 * time.Second}) {
			t.Fatalf("recipients=%d sizes=%v starts=%v err=%v", len(got), sizes, starts, err)
		}
	})
}

func TestStatusBroadcastRateLimitRetriesOnlyCurrentBatchOnce(t *testing.T) {
	for _, secondFails := range []bool{false, true} {
		t.Run(fmt.Sprint(secondFails), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pns := statusTestPNs(101)
				client := newStatusTestClient(nil, nil)
				start := time.Now()
				var queried [][]types.JID
				var starts []time.Duration
				got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: pns}, func(ctx context.Context, batch []types.JID) (map[types.JID]types.UserInfo, error) {
					queried = append(queried, slices.Clone(batch))
					starts = append(starts, time.Since(start))
					if len(queried) == 2 || secondFails && len(queried) == 3 {
						return nil, fmt.Errorf("USync wrapped: %w", ErrIQRateOverLimit)
					}
					return statusTestLIDResponse(batch)
				})
				if len(queried) != 3 || !slices.Equal(queried[0], pns[:100]) || !slices.Equal(queried[1], pns[100:]) || !slices.Equal(queried[2], pns[100:]) || !slices.Equal(starts, []time.Duration{0, time.Second, 11 * time.Second}) {
					t.Fatalf("query_count=%d starts=%v", len(queried), starts)
				}
				if secondFails {
					if len(got) != 0 || !errors.Is(err, ErrIQRateOverLimit) {
						t.Fatalf("partial audience or wrong error: %v, %v", got, err)
					}
				} else if err != nil || len(got) != 101 {
					t.Fatalf("recovered audience=%v err=%v", got, err)
				}
			})
		})
	}
}

func TestStatusBroadcastNonRateErrorsAreNotRetried(t *testing.T) {
	for _, lookupErr := range []error{ErrIQBadRequest, ErrNotConnected, errors.New("arbitrary text containing rate-overlimit")} {
		client := newStatusTestClient(nil, nil)
		calls := 0
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: statusTestPNs(1)}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
			calls++
			return nil, lookupErr
		})
		if len(got) != 0 || calls != 1 || !errors.Is(err, lookupErr) {
			t.Fatalf("calls=%d audience=%v error=%v", calls, got, err)
		}
	}
}

func TestStatusBroadcastRateLimitInsufficientBudgetSkipsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newStatusTestClient(nil, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start, calls := time.Now(), 0
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: statusTestPNs(1)}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
			calls++
			return nil, ErrIQRateOverLimit
		})
		if len(got) != 0 || calls != 1 || !errors.Is(err, ErrIQRateOverLimit) || time.Since(start) != 0 {
			t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
		}
	})
}

func TestStatusBroadcastCancellationInterruptsRetryWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newStatusTestClient(nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stopped := make(chan struct{})
		go func() { defer close(stopped); time.Sleep(time.Second); cancel() }()
		start, calls := time.Now(), 0
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(ctx, types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: statusTestPNs(1)}, func(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
			calls++
			return nil, ErrIQRateOverLimit
		})
		<-stopped
		if len(got) != 0 || calls != 1 || !errors.Is(err, context.Canceled) || time.Since(start) != time.Second {
			t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
		}
	})
}

func TestStatusBroadcastAttemptTimeoutIsTenSeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newStatusTestClient(nil, nil)
		start, calls := time.Now(), 0
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: statusTestPNs(1)}, func(ctx context.Context, _ []types.JID) (map[types.JID]types.UserInfo, error) {
			calls++
			<-ctx.Done()
			return nil, ctx.Err()
		})
		if len(got) != 0 || calls != 1 || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 10*time.Second {
			t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
		}
	})
}

func TestStatusBroadcastTotalBudgetIncludesBatchPauses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newStatusTestClient(nil, nil)
		start, calls := time.Now(), 0
		got, err := client.resolveStatusBroadcastRecipientsWithLookup(context.Background(), types.StatusPrivacy{Type: types.StatusPrivacyTypeWhitelist, List: statusTestPNs(301)}, func(ctx context.Context, batch []types.JID) (map[types.JID]types.UserInfo, error) {
			calls++
			select {
			case <-time.After(9 * time.Second):
				return statusTestLIDResponse(batch)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		if len(got) != 0 || calls != 3 || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 30*time.Second {
			t.Fatalf("calls=%d elapsed=%v err=%v", calls, time.Since(start), err)
		}
	})
}
