# Roadmap

## Milestone 1: Storage Primitive

- Implement encryption, erasure coding, manifests, and local reconstruction.
- Validate recovery under shard and peer loss.
- Keep keys out of public manifests.

Status: implemented in this prototype.

## Milestone 2: Peer Identity And Transport

- Use stable peer identities and signed peer records.
- Add signed sampled proof-of-storage audits.
- Add a process-boundary peer runtime for local transport tests.
- Add membership bootstrap, signed heartbeats, and heartbeat expiry.
- Add libp2p or another production peer transport.
- Add content-addressed shard retrieval over the transport.
- Persist peer stores on disk.
- Version manifests, peer records, heartbeats, and scheduler schema before widening the protocol.
- Exercise hostile distributed runtime conditions.
- Add optional TLS transport for local peer runtimes.

Status: identity, local peer-record serialization, local audit proofs, a local HTTP/HTTPS peer runtime, signed runtime request envelopes with nonce replay protection, durable peer-runtime shard storage, signed heartbeat membership, membership expiry, explicit format/schema version gates, and hostile distributed runtime tests are implemented; production transport is not.

## Milestone 3: Repair And Audit Scheduling

- Add periodic shard audits.
- Add repair jobs when redundancy drops below policy.
- Persist tracked manifests, peer health, and scheduler run history.
- Add basic failure-domain-aware placement.
- Split peer capacity into allocatable bytes, reserved bytes, and repair headroom.
- Make scheduler manifest transitions idempotent.
- Prove restart behavior after a scheduler process dies mid-transition.
- Combine domain diversity, capacity, repair headroom, audit history, and repair success into placement admission and scoring.
- Add repair caps, cooldown windows, and degraded-object retry backoff.
- Add repair/audit metrics, normalized event logs, and replayable scheduler traces.
- Add operator inspection tools for runs, objects, peers, and degraded state.
- Anchor audit sampling to public randomness or client-committed randomness.
- Track peer reliability over time.

Status: local repair, failure-domain placement, capacity headroom, peer-health-aware admission/scoring, anti-thrash scheduler controls, SQLite-backed scheduler transactions, crash-recovery tests, durable state-backed scheduling, scheduler observability, and inspection CLI commands are implemented; public-randomness sampling is not.

## Milestone 4: Incentives

- Define storage receipts, pricing, collateral, and slashing rules.
- Separate bandwidth payments from storage payments.
- Model churn, fraud, and collusion before committing to token mechanics.

## Milestone 5: Product Surface

- Add a file namespace layer.
- Add sync clients and restore flows.
- Add billing controls and operator dashboards.
- Benchmark real cost against centralized providers.
