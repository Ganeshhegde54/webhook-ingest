# Solution

## 1. Duplicate Webhook Events

### Problem
Duplicate call records and duplicate webhook events appear in the database when the provider delivers events at-least-once or retries requests concurrently.

### Root Cause
The service used a non-atomic `EventExists` check followed by a separate `InsertEvent`. Under concurrent deliveries with the same `event_id`, both requests found that the event did not exist yet and both executed `INSERT INTO events`. Furthermore, the `events` table lacked a `UNIQUE` constraint on `event_id`.

### Fix
1. Updated `001_init.sql` to enforce `CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events (event_id)`.
2. Changed `Store.InsertEvent` to execute `INSERT INTO events ... ON CONFLICT (event_id) DO NOTHING` and return whether the row was actually inserted.
3. Updated `Service.Ingest` to rely directly on the atomic insert result: if not inserted, it logs the duplicate and exits immediately with `nil` (returning HTTP 200 OK without double-processing).

### Test
Added `TestConcurrentDuplicateDeliveryIsIgnored` in `internal/ingest/service_test.go`, which dispatches 50 concurrent requests with the identical `event_id` and verifies that exactly 1 event row is persisted in PostgreSQL.

### Verification
- `go test ./...` passed.
- `go test -race ./...` (via Docker `golang:1.25`) passed without race warnings.

## 2. Account Call Counts Concurrency

### Problem
Account call counts and aggregate statistics drift or become corrupted under concurrent webhook deliveries for the same account.

### Root Cause
`Cache.Record` in `internal/stats/cache.go` mutated the internal map `m` and individual pointer values (`s.CallCount`, `s.TotalDurationSec`) without acquiring a mutex lock. Concurrent requests caused data races, lost increments, and map corruption.

### Fix
Added `c.mu.Lock()` and `defer c.mu.Unlock()` to `Cache.Record` in `internal/stats/cache.go` to synchronize all map lookups and counter modifications.

### Test
1. Added `TestCacheConcurrentRecord` in `internal/stats/cache_test.go` to test 100 concurrent record operations on the same account.
2. Added `TestConcurrentAccountStatsProcessing` in `internal/ingest/service_test.go` to verify end-to-end ingestion and accurate totals in both memory cache and PostgreSQL.

### Verification
- `go test ./...` passed.
- `go test -race ./...` (via Docker `golang:1.25`) passed with zero race warnings.

## 3. Unprocessed Call Recordings

### Problem
Calls landed in the system and were saved in the `calls` table, but their recordings were never marked processed (`recording_processed` stayed `false`).

### Root Cause
The asynchronous background goroutine executing `s.processRecording(ctx, rec)` was passed `ctx` directly from the incoming HTTP request context (`r.Context()`). As soon as the HTTP handler returned `200 OK`, `net/http` canceled `r.Context()`. When `processRecording` attempted to update the database after its simulated processing sleep (`time.Sleep(50ms)`), `pgxpool` immediately aborted with `context canceled`.

### Fix
Decoupled background recording processing from the transient HTTP request lifecycle by passing `context.Background()` to `processRecording` inside the asynchronous goroutine in `internal/ingest/service.go`.

### Test
Added `TestRecordingProcessedAfterHTTPRequest` in `internal/ingest/service_test.go`, which delivers a webhook with a `recording_url` via HTTP, verifies the immediate HTTP 200 response, and verifies that `recording_processed` transitions to `true` in PostgreSQL.

### Verification
- `go test ./...` passed.
- `go test -race ./...` (via Docker `golang:1.25`) passed with zero race warnings.

## 4. Recording Processing Failure Logging

### Problem
When recording processing failed (due to network/database connectivity or context timeouts), no errors appeared in the service logs, making background failures completely invisible to operators.

### Root Cause
In `internal/ingest/service.go`, the background goroutine executing `s.processRecording` had an empty error handling block (`// TODO: handle`), discarding returned errors silently.

### Fix
Replaced the empty error block with structured error logging using the service's logger:
`s.log.Error("process recording failed", "call_id", rec.CallID, "account_id", rec.AccountID, "err", err)`.

### Test
Added `TestRecordingProcessingErrorIsLogged` in `internal/ingest/service_test.go`, which forces a recording processing failure by terminating the database pool and asserts that an `ERROR` entry with the corresponding `call_id` is emitted.

### Verification
- `go test ./...` passed.
- `go test -race ./...` (via Docker `golang:1.25`) passed with zero race warnings.

## 5. In-Flight Recording Work Lost During Shutdown

### Problem
When the server received `SIGTERM` (e.g., during a rolling deployment), recording work that had already been started was silently dropped, leaving calls with `recording_processed = false` permanently.

### Root Cause
`Service.Ingest` spawned background goroutines with `go func()` but the `Service` struct had no `sync.WaitGroup` to track them. `main.go` called `srv.Shutdown` (which only drains HTTP connections) and then returned immediately, killing the process while goroutines inside `processRecording` were still sleeping or waiting for the database. There was no mechanism to wait for in-flight work to complete before exit.

### Fix
1. Added a `sync.WaitGroup wg` field to `Service` in `internal/ingest/service.go`.
2. Before each background goroutine is spawned, `s.wg.Add(1)` is called; `defer s.wg.Done()` is placed inside the goroutine so it always decrements on exit.
3. Added a `Wait()` method on `Service` that calls `s.wg.Wait()`.
4. In `cmd/server/main.go`, after `srv.Shutdown` returns (HTTP connections drained), `svc.Wait()` is called to block until all recording goroutines finish.

### Test
Added `TestInFlightRecordingNotLostOnShutdown` in `internal/ingest/service_test.go`, which ingests a call with a `recording_url`, immediately calls `svc.Wait()` (simulating shutdown while processing is still running), and asserts that `recording_processed` is `true` in PostgreSQL after `Wait()` returns.

### Verification
- `go test ./...` passed.
- `go test -race ./...` (via Docker `golang:1.25`) passed with zero race warnings.
