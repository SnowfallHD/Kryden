# Kryden

Kryden is the start of a decentralized storage network: clients encrypt data, split it into erasure-coded shards, and place those shards across independent peers. The first milestone is not a production network. It is a rigorous local prototype that proves the core storage primitive before adding identity, transport, economics, and adversarial network behavior.

## Current Prototype

This repo currently implements:

- AES-256-GCM client-side encryption.
- Systematic Reed-Solomon erasure coding over GF(256).
- Ed25519 peer identities for local swarm peers.
- Merkle shard commitments and signed proof-of-storage audits.
- Persistent local peer records for stable simulated peer identity.
- Encrypted-object repair that restores failed shard placements when enough shards survive.
- A background audit/repair scheduler backed by durable SQLite state.
- Failure-domain-aware placement using labeled peer buckets.
- Peer capacity accounting with allocatable bytes, reserved bytes, and repair headroom.
- Local peer swarm simulation with deterministic placement and capacity checks.
- CLI flows for encoding, decoding, and failure simulation.
- Tests for shard recovery, tamper detection, and peer-loss reconstruction.

## Quick Start

```bash
npm install
npm test
npm run demo
```

Encode a file into encrypted shards:

```bash
npx tsx src/cli.ts encode ./input.bin --out ./tmp/object --data-shards 6 --parity-shards 3
```

Decode it back:

```bash
npx tsx src/cli.ts decode ./tmp/object --out ./tmp/recovered.bin
```

Run a local swarm failure simulation:

```bash
npx tsx src/cli.ts simulate --size 1048576 --peers 12 --data-shards 6 --parity-shards 3 --fail-peers 3
```

The simulation also audits every shard. Online peers return signed Merkle proofs; offline peers fail the audit while the object can still recover if enough shards remain.
By default the simulation then runs repair and re-audits the updated manifest.

Run the durable scheduler simulation:

```bash
npx tsx src/cli.ts simulate-scheduler --state tmp/kryden-scheduler-state.sqlite
```

This writes tracked objects, peer health, shard placements, repair events, transitions, and scheduler run history to SQLite.
Use `--failure-domains`, `--reserved-bytes`, and `--repair-headroom-bytes` to exercise correlated-risk and capacity-pressure scenarios.
Use `--max-repairs-per-run`, `--object-cooldown-ms`, and `--degraded-backoff-base-ms` to exercise anti-thrash behavior.

## Architecture

Kryden's storage path is:

1. Encrypt plaintext locally with a fresh content key.
2. Hash encrypted bytes to derive a content identifier.
3. Split encrypted bytes into `k` data shards and `m` parity shards.
4. Commit each shard to a Merkle root for sampled storage audits.
5. Place shards across peers using deterministic placement weighted by available capacity.
6. Challenge peers to sign sampled Merkle proofs for stored shards.
7. Keep repair headroom separate from regular allocation.
8. Repair failed shard placements by reconstructing encrypted bytes from surviving shards and re-placing missing shard indexes.
9. Persist scheduler run history and peer health counters through atomic state transitions.
10. Reconstruct encrypted bytes from any `k` valid shards.
11. Decrypt locally with the client-held secret.

The public manifest does not contain the encryption key. In the CLI prototype, the key is written to a separate `secret.kryden-secret.json` file so the trust boundary stays explicit.

## Project Layout

```text
src/crypto/       Client-side encryption and integrity checks
src/erasure/      Reed-Solomon coding over GF(256)
src/storage/      Manifest, secret, and Merkle commitment schemas
src/swarm/        Local peer identity, placement, audit, and repair simulation
src/state/        Durable SQLite scheduler state
src/scheduler/    Background audit and repair scheduler
src/cli.ts        Developer CLI
tests/            End-to-end and primitive tests
docs/             Protocol, threat model, and roadmap
```

## Status

The current code proves five early invariants: data survives shard loss up to the configured parity budget, online peers can be challenged for signed sampled possession of encrypted shards, failed placements can be repaired without exposing plaintext or client keys, scheduler state survives process exits, and placement/repair respect basic failure-domain and capacity constraints. The next step is to replace the local swarm with real transport, durable shard storage, and adversarial audit randomness.
