package whatsmeow

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type unavailableRequestLogger struct {
	waLog.Logger
	requests atomic.Int32
	panics   atomic.Int32
}

func (l *unavailableRequestLogger) Warnf(format string, args ...any) {
	if strings.HasPrefix(format, "Failed to send request for unavailable message") {
		l.requests.Add(1)
	}
}

func (l *unavailableRequestLogger) Errorf(format string, args ...any) {
	if strings.Contains(format, "panicked") {
		l.panics.Add(1)
	}
}

// An unpaired device makes real SendPeerMessage return ErrNotLoggedIn. Its
// warning records the attempt without contacting a phone or needing session keys.
func TestImmediateUnavailableRequestHonorsClientSettings(t *testing.T) {
	for _, tc := range []struct {
		name      string
		automatic bool
		messenger bool
		want      int32
	}{
		{"enabled", true, false, 1},
		{"disabled", false, false, 0},
		{"messenger", true, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := &unavailableRequestLogger{Logger: waLog.Noop}
			client := NewClient(&store.Device{}, logger)
			client.AutomaticMessageRerequestFromPhone = tc.automatic
			if tc.messenger {
				client.MessengerConfig = &MessengerConfig{}
			}
			client.immediateRequestMessageFromPhone(context.Background(), &types.MessageInfo{})
			if got := logger.requests.Load(); got != tc.want {
				t.Fatalf("request attempts = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestUnavailableViewOnceKeepsEventAndAckWithoutPhoneRequest(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		for _, unavailableType := range []events.UnavailableType{events.UnavailableTypeViewOnce, events.UnavailableTypeUnknown} {
			t.Run(string(unavailableType)+map[bool]string{false: "/async", true: "/sync"}[synchronous], func(t *testing.T) {
				logger := &unavailableRequestLogger{Logger: waLog.Noop}
				client := NewClient(&store.Device{}, logger)
				client.AutomaticMessageRerequestFromPhone = true
				client.SynchronousAck = synchronous
				info := types.MessageInfo{ID: "unavailable-test"}
				info.Chat = types.NewJID("70000000001", types.DefaultUserServer)
				info.Sender = info.Chat
				var eventCount int
				client.AddEventHandler(func(raw any) {
					evt, ok := raw.(*events.UndecryptableMessage)
					if !ok || !evt.IsUnavailable || evt.UnavailableType != unavailableType || evt.Info.ID != info.ID {
						t.Errorf("unexpected event: %#v", raw)
					}
					eventCount++
				})
				acks := make(chan waBinary.Node, 1)
				attachStatusQueryServer(t, client, func(node *waBinary.Node) waBinary.Node {
					acks <- *node
					return waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"type": "result", "id": "test-response"}}
				})
				client.decryptMessages(context.Background(), &info, &waBinary.Node{
					Tag: "message", Attrs: waBinary.Attrs{"id": info.ID, "from": info.Chat},
					Content: []waBinary.Node{{Tag: "unavailable", Attrs: waBinary.Attrs{"type": string(unavailableType)}}},
				})
				select {
				case ack := <-acks:
					if ack.Tag != "ack" || ack.Attrs["id"] != info.ID || ack.Attrs["to"] != info.Chat || ack.Attrs["class"] != "message" {
						t.Errorf("invalid ACK: %v", ack)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("ACK not received on local encrypted transport")
				}
				want := int32(1)
				if unavailableType == events.UnavailableTypeViewOnce {
					want = 0
				}
				if got := logger.requests.Load(); got != want || eventCount != 1 || logger.panics.Load() != 0 {
					t.Fatalf("requests=%d (want %d), events=%d, panics=%d", got, want, eventCount, logger.panics.Load())
				}
			})
		}
	}
}
