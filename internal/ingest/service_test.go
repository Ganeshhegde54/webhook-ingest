package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	svc := ingest.New(st, stats.NewCache(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "",
		OccurredAt:   time.Now(),
	}

	const concurrency = 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	start := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-start
			if err := svc.Ingest(ctx, evt); err != nil {
				t.Errorf("Ingest: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	var count int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 1 {
		t.Fatalf("stored %d copies of %s, want 1", count, eventID)
	}
}

func TestConcurrentAccountStatsProcessing(t *testing.T) {
	st := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	cache := stats.NewCache()
	svc := ingest.New(st, cache, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	const numCalls = 30
	const callDuration = 10
	var wg sync.WaitGroup
	wg.Add(numCalls)

	start := make(chan struct{})
	for i := 0; i < numCalls; i++ {
		callIdx := i
		go func() {
			defer wg.Done()
			<-start
			evt := ingest.Event{
				EventID:      fmt.Sprintf("evt_%s_%d", accountID, callIdx),
				CallID:       fmt.Sprintf("call_%s_%d", accountID, callIdx),
				AccountID:    accountID,
				Status:       "completed",
				DurationSec:  callDuration,
				RecordingURL: "",
				OccurredAt:   time.Now(),
			}
			if err := svc.Ingest(ctx, evt); err != nil {
				t.Errorf("Ingest: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	// Verify in-memory cache
	cached := cache.Get(accountID)
	if cached.CallCount != int64(numCalls) || cached.TotalDurationSec != int64(numCalls*callDuration) {
		t.Fatalf("cache: got %+v, want CallCount=%d TotalDurationSec=%d", cached, numCalls, numCalls*callDuration)
	}

	// Verify database account_stats
	dbStats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if dbStats.CallCount != int64(numCalls) || dbStats.TotalDurationSec != int64(numCalls*callDuration) {
		t.Fatalf("db stats: got %+v, want CallCount=%d TotalDurationSec=%d", dbStats, numCalls, numCalls*callDuration)
	}
}




