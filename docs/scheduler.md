# Background Scheduler

The scheduler runs audit and repair as a continuous maintenance loop.

## Components

- `SQLiteStateStore` persists tracked object manifests, shard placements, peer health counters, repair events, transitions, and bounded scheduler run history.
- `BackgroundRepairScheduler` loads tracked objects from the store, calls local repair for each object, records the updated manifests, and updates health history.
- `runOnce()` performs one deterministic maintenance pass for tests and command-line simulations.
- `start()` and `stop()` run the same pass on an interval for a long-lived process.

## SQLite Tables

The SQLite state store starts with these tables:

- `peers`: known peers and their latest public key and failure-domain labels.
- `peer_health`: audit pass/fail counts, consecutive failures, last seen errors, and repair placement counts.
- `schema_meta`: explicit state schema metadata, including `state_schema_version`.
- `manifests`: latest tracked manifest for each content id.
- `shard_placements`: current normalized shard placement rows for every tracked manifest.
- `scheduler_runs`: bounded run summaries with audit and repair totals, metrics JSON, and replay trace JSON.
- `scheduler_events`: normalized replayable scheduler trace events.
- `state_transitions`: running, committed, and abandoned maintenance transitions.
- `repair_events`: per-shard successful and failed repair events.

The state database does not store shard bytes or client content keys. The store also stamps `PRAGMA user_version` with `SQLITE_STATE_SCHEMA_VERSION`; newer schema versions fail open attempts instead of being migrated blindly.

## Observability

Every committed run records three observability layers:

- `metrics`: aggregate counters for tracked, eligible, skipped, audited, healthy, repaired, and degraded objects, plus audit/repair totals and recovered transition count.
- `trace`: ordered JSON events embedded in the `scheduler_runs` row. This is the replayable explanation for why each object was skipped, audited, repaired, left degraded, or marked healthy.
- `scheduler_events`: the same trace events normalized into rows keyed by `run_id` and `sequence` for filtering and inspection.

Trace event types include `scheduler.object.skipped`, `scheduler.audit.failed`, `scheduler.repair.succeeded`, `scheduler.repair.failed`, and `scheduler.object.degraded`.

The CLI exposes the same state through operator inspection commands:

- `kryden inspect run latest --state <db>` prints the latest run, metrics, and replay trace.
- `kryden inspect run <run-id> --state <db>` prints a specific run.
- `kryden inspect object <content-id> --state <db>` prints manifest placement, repair cooldown/backoff state, and shard commitments.
- `kryden inspect peer <peer-id> --state <db>` prints peer accounting, health counters, current placements, and repair events.
- `kryden inspect degraded --state <db>` lists objects with active degraded/backoff state.

## State Transitions

Scheduler repair is one SQLite transaction boundary.

1. The store records a `running` transition with the object ids under maintenance.
2. The scheduler performs audits and repair against the in-memory swarm.
3. The store commits updated manifests, shard placements, peer health, repair events, transition status, and the run summary in one SQLite transaction.
4. If repair throws, the transition is marked `abandoned` and tracked manifests remain unchanged.

If the process exits between steps 1 and 3, durable state still points at the previous manifest. Replacement shard writes may have occurred in the local swarm, but no half-updated manifest is published.

## Crash Recovery

On restart, `BackgroundRepairScheduler.runOnce()` asks the store to recover interrupted transitions before opening a new one. Any stale `running` transition is marked `abandoned` with a recovery error. The next repair cycle starts from the last committed manifest, so the durable state never publishes partial placement changes from the killed process.

The test suite covers this with a child process that opens the SQLite database, records a `running` transition, and is killed with `SIGKILL` before commit. The parent process then restarts the scheduler against the same database and verifies that the stale transition is abandoned, the previous manifest remains intact until the new commit, and the new repair cycle commits cleanly.

## Health Semantics

- A passing audit increments `auditsPassed`, resets `consecutiveFailures`, and updates `lastOkAt`.
- A failed audit increments `auditsFailed`, increments `consecutiveFailures`, and records `lastError`.
- A successful repair increments `repairedShards` on the replacement peer.

## Placement Feedback

Before every run, the scheduler loads persisted `peer_health` and gives it to the swarm placement layer. Repair selection then uses the latest audit and repair history instead of only static peer labels. Chronically failing peers can be denied admission, and degraded peers that remain admitted receive a worse placement score.

## Anti-Thrash Controls

The scheduler has bounded repair behavior:

- `maxRepairsPerRun` caps how many shard placements can be repaired in one scheduler pass. Excess failed placements are reported as deferred repair failures.
- `objectCooldownMs` suppresses an object for a short window after successful repair so the scheduler does not immediately churn it again.
- `degradedBackoffBaseMs` and `degradedBackoffMaxMs` apply exponential backoff when an object remains degraded or repair work is deferred.
- Suppressed objects are skipped by `getEligibleTrackedObjects()` until their `nextEligibleAt` time.

The suppression state lives on the `manifests` row next to the latest committed manifest: `consecutive_degraded_runs`, `next_eligible_at`, `last_scheduler_run_at`, and `last_repair_at`.

## Current Limits

- Scheduler state is durable SQLite, and peer runtimes can now persist shard payloads when configured with a storage directory.
- Local simulator peers remain memory-only unless they are replaced by configured peer runtimes.
- Timer errors are swallowed so later intervals can try again; production observability still needs structured logging.
- Audit randomness is local; production should anchor challenge selection to public or committed randomness.
