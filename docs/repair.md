# Repair

Kryden repair is intentionally plaintext-blind.

1. Audit every shard placement in the manifest.
2. Treat failed audits as unavailable placements.
3. Fetch surviving encrypted shards.
4. If at least `k` shards are available, reconstruct the encrypted object.
5. Re-run Reed-Solomon encoding with the same erasure configuration.
6. Store replacement shard indexes on online peers.
7. Return an updated manifest with new peer public keys and Merkle commitments.
8. Persist scheduler run history and peer health changes when repair is scheduler-driven.

The repair worker does not need the client content key. That means a future storage coordinator can restore redundancy without gaining the ability to decrypt user data.

## Placement Rules

Repair placement avoids the failed peer and prefers buckets that are not already represented in the updated manifest. If the swarm does not have enough independent buckets, repair falls back to the least-bad available peer instead of refusing a recoverable object.

Repair writes can consume peer repair headroom. Regular writes cannot. Within the admitted candidate set, the score combines:

- deterministic shard/peer hash as a tie-breaker;
- failure-domain reuse penalty;
- free capacity pressure;
- repair headroom pressure;
- audit failure rate;
- consecutive audit failures;
- prior successful repair placements as a small positive signal.

## Current Limits

- Repair runs synchronously in the local simulator.
- Failed peers keep their stale in-memory shard data, but the updated manifest stops pointing at them.
- Peer identities can be exported and restored; shard payloads are not persisted to disk yet.
- There is no reputation penalty or collateral yet.
