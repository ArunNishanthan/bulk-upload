# Ingestion Service Agenda

## Immediate Cleanups
- ✅ Refactor `BulkUploadService` to separate parsing, batching, and metrics tracking for easier maintenance.
- ✅ Centralise gzip handling in `CompressionSupport` so future formats (zip, bzip2) can be plugged in quickly.
- ✅ Ensure local MongoDB defaults live in `application.properties` for developer convenience.
- ✅ Queue uploads asynchronously with virtual-thread worker pool, supporting optional collection wipe per job.

## Performance Fine Tuning
- Keep the batch size configurable (`app.ingestion.batch-size`) and benchmark 5k, 7.5k, and 10k depending on CPU quota; larger batches reduce Mongo round-trips at the cost of higher per-flush latency.
- If a pod has spare CPU headroom, introduce a small worker pool (2–3 threads) that drains a blocking queue of batches while parsing continues on the request thread. This overlaps parsing and network I/O without spiking memory.
- Add Micrometer gauges for queue depth, batch latency, and Mongo RTT so you can spot regression early. Export them to Prometheus/Grafana.
- For gzip uploads, favour `pigz` (parallel gzip) on the producer side to shrink upload time while keeping server CPU use similar.

## Preventing Request Timeouts
- Switch the controller to return `202 Accepted` immediately with a job identifier. Process the work asynchronously via `@Async`, Project Loom virtual threads, or a lightweight `TaskExecutor`. This avoids servlet timeouts when a single file takes >5 minutes.
- Persist job metadata: `{ jobId, filename, status (PENDING/RUNNING/SUCCEEDED/FAILED), processedRows, insertedRows, invalidRows, startedAt, finishedAt }`.
- Expose `GET /api/v1/ingestions/{jobId}` so the UI can poll progress. Consider server-sent events or WebSocket notifications if near-real-time feedback is needed.

## Future Enhancements
- Add checksum support (MD5/SHA-256) in the request payload to detect corruption before ingest.
- Store invalid rows (up to a threshold) in a separate Mongo collection or object store so analysts can remediate quickly.
- Provide a dry-run flag that validates and counts records without inserting, useful for early QA checks.
- Roll out circuit breakers and retry policies around Mongo writes to handle transient network hiccups cleanly.
