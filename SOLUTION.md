# Solution

## What was broken

1. **Duplicate events** — `EventExists` was checked, then `InsertEvent` ran
   separately. Concurrent deliveries of the same `event_id` both passed the
   check before either insert landed, and `events` had no unique constraint
   to stop it.
2. **Drifting call counts** — `Cache.Record` mutated its map and counters
   without a lock, so concurrent webhooks for the same account raced and
   lost increments.
3. **Recordings never marked processed** — the background goroutine that
   processes recordings was given the *request's* context. The moment the
   handler returned 200, `net/http` canceled that context, so the later
   database update failed silently.
4. **Silent failures** — the goroutine's error path was an empty
   `// TODO: handle`, so failures never reached the logs.
5. **In-flight work lost on deploy** — background goroutines were spawned
   with no `WaitGroup`, so `SIGTERM` killed the process mid-processing.

## Fixes

1. Added `UNIQUE INDEX` on `event_id` + `INSERT ... ON CONFLICT DO NOTHING`;
   `Ingest` now trusts the atomic insert result instead of a separate check.
2. Added a mutex around all reads/writes in `Cache.Record`.
3. Passed `context.Background()` to `processRecording` instead of the
   request context.
4. Replaced the empty error block with structured logging
   (`s.log.Error(...)`).
5. Added a `sync.WaitGroup` to `Service`; `main.go` calls `svc.Wait()` after
   `srv.Shutdown()` so in-flight recordings finish before exit.

Each fix has a test that fails before and passes after (see
`internal/ingest/service_test.go`, `internal/stats/cache_test.go`). All
verified with `go test ./...` and `go test -race ./...` — no failures or
race warnings.

## Why this deduplication strategy

I used a Postgres unique constraint + `ON CONFLICT DO NOTHING` rather than a
Redis-based dedup key (`SETNX event_id`). The insert and the duplicate check
happen atomically in the same store as the data itself, so there's no way
for Redis to say "new" while the Postgres write fails, or vice versa — a
Redis-based approach would need a TTL and a reconciliation path to guard
against that drift. Redis would be faster on the hot path, but at this
volume we're not contention-bound on Postgres, so the extra moving part
isn't worth the consistency risk yet.

## At 10,000 webhooks/sec

- Move the dedup check to Redis (`SETNX` + TTL) to take duplicate checks off
  the Postgres hot path, keeping the unique constraint as a safety net.
- Batch inserts instead of one write per event.
- Move `account_stats` aggregation off the request path — write raw events
  fast, aggregate asynchronously via a queue or periodic rollup.
- Increase the Postgres pool size and consider partitioning by `account_id`
  if writes start contending.
- Add backpressure (a bounded queue in front of ingestion) so a downstream
  slowdown produces 429s instead of unbounded goroutine growth.