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
