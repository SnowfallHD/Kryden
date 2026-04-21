# Roadmap

## Milestone 1: Storage Primitive

- Implement encryption, erasure coding, manifests, and local reconstruction.
- Validate recovery under shard and peer loss.
- Keep keys out of public manifests.

Status: implemented in this prototype.

## Milestone 2: Peer Identity And Transport

- Use stable peer identities and signed peer records.
- Add signed sampled proof-of-storage audits.
- Add libp2p or another production peer transport.
- Add content-addressed shard retrieval over the transport.
- Persist peer stores on disk.

Status: identity, local peer-record serialization, and local audit proofs are implemented; production transport and durable shard persistence are not.

## Milestone 3: Repair And Audit Scheduling

- Add periodic shard audits.
- Add repair jobs when redundancy drops below policy.
- Persist tracked manifests, peer health, and scheduler run history.
- Add basic failure-domain-aware placement.
- Split peer capacity into allocatable bytes, reserved bytes, and repair headroom.
- Make scheduler manifest transitions idempotent.
- Anchor audit sampling to public randomness or client-committed randomness.
- Track peer reliability over time.

Status: local repair, failure-domain placement, capacity headroom, idempotent JSON-backed scheduler transitions, and durable state-backed scheduling are implemented; public-randomness sampling, SQLite storage, and durable shard persistence are not.

## Milestone 4: Incentives

- Define storage receipts, pricing, collateral, and slashing rules.
- Separate bandwidth payments from storage payments.
- Model churn, fraud, and collusion before committing to token mechanics.

## Milestone 5: Product Surface

- Add a file namespace layer.
- Add sync clients and restore flows.
- Add billing controls and operator dashboards.
- Benchmark real cost against centralized providers.
