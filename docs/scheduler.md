# Background Scheduler

The scheduler runs audit and repair as a continuous maintenance loop.

## Components

- `JsonStateStore` persists tracked object manifests, peer health counters, and bounded scheduler run history.
- `BackgroundRepairScheduler` loads tracked objects from the store, calls local repair for each object, records the updated manifests, and updates health history.
- `runOnce()` performs one deterministic maintenance pass for tests and command-line simulations.
- `start()` and `stop()` run the same pass on an interval for a long-lived process.

## State File

The JSON state file stores:

- `objects`: content ids mapped to the latest known manifest.
- `peers`: audit pass/fail counts, consecutive failures, last seen errors, and repair placement counts.
- `runs`: bounded run summaries with audit and repair totals.
- `transitions`: running, committed, and abandoned state transitions.

The state file does not store shard bytes or client content keys.

## State Transitions

Scheduler repair is idempotent at the manifest-state boundary.

1. The store records a `running` transition with the object ids under maintenance.
2. The scheduler performs audits and repair against the in-memory swarm.
3. The store atomically commits updated manifests, peer health, and the run summary in one file replace.
4. If repair throws, the transition is marked `abandoned` and tracked manifests remain unchanged.

If the process exits between steps 1 and 3, durable state still points at the previous manifest. Replacement shard writes may have occurred in the local swarm, but no half-updated manifest is published.

## Health Semantics

- A passing audit increments `auditsPassed`, resets `consecutiveFailures`, and updates `lastOkAt`.
- A failed audit increments `auditsFailed`, increments `consecutiveFailures`, and records `lastError`.
- A successful repair increments `repairedShards` on the replacement peer.

## Current Limits

- Scheduler state is durable, but shard payloads are still in memory.
- A restarted process can reload state, but cannot repair until shard persistence or real peer transport exists.
- Timer errors are swallowed so later intervals can try again; production observability needs structured logging.
- Audit randomness is local; production should anchor challenge selection to public or committed randomness.
