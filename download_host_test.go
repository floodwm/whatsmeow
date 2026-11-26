package whatsmeow

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestMediaDownloadHostSelection(t *testing.T) {
	wire := bytes.Repeat([]byte{42}, 42)
	hash := sha256.Sum256(wire)
	for _, tc := range []struct {
		name        string
		checksum    []byte
		firstStatus int
		wantErr     error
		wantSecond  int32
	}{
		{"success", nil, 200, nil, 0},
		{"host unavailable", nil, 421, nil, 1},
		{"ciphertext hash", make([]byte, 32), 200, ErrInvalidMediaEncSHA256, 0},
		{"invalid MAC", hash[:], 200, ErrInvalidMediaHMAC, 0},
	} {
		for _, mode := range []string{"memory", "file"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				var firstHits, secondHits atomic.Int32
				host := func(status int, hits *atomic.Int32) *httptest.Server {
					s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						hits.Add(1)
						w.WriteHeader(status)
						if status == 200 {
							_, _ = w.Write(wire)
						}
					}))
					t.Cleanup(s.Close)
					return s
				}
				a, b := host(tc.firstStatus, &firstHits), host(200, &secondHits)
				cli := &Client{Log: waLog.Noop, mediaHTTP: a.Client(), mediaConnCache: &MediaConn{
					TTL: 60, FetchedAt: time.Now(), Hosts: []MediaConnHost{{Hostname: strings.TrimPrefix(a.URL, "https://")}, {Hostname: strings.TrimPrefix(b.URL, "https://")}},
				}}
				var key, got []byte
				if tc.checksum != nil {
					key = make([]byte, 32)
				}
				var err error
				if mode == "memory" {
					got, err = cli.DownloadMediaWithPath(t.Context(), "/file?test=1", tc.checksum, hash[:], key, MediaAudio, "", false)
				} else {
					f, createErr := os.CreateTemp(t.TempDir(), "media")
					if createErr != nil {
						t.Fatal(createErr)
					}
					defer f.Close()
					err = cli.DownloadMediaWithPathToFile(t.Context(), "/file?test=1", tc.checksum, hash[:], key, MediaAudio, "", false, f)
					if err == nil {
						got, err = os.ReadFile(f.Name())
					}
				}
				if !errors.Is(err, tc.wantErr) || firstHits.Load() != 1 || secondHits.Load() != tc.wantSecond || (err == nil && !bytes.Equal(got, wire)) {
					t.Fatalf("err=%v first=%d second=%d payload=%x", err, firstHits.Load(), secondHits.Load(), got)
				}
			})
		}
	}
}
