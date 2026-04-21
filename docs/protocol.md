# Kryden Protocol Sketch

This document captures the first protocol boundary. It is intentionally scoped to the storage primitive that can be tested locally.

## Object Lifecycle

### 1. Seal

The client generates a random 256-bit content key and encrypts plaintext using AES-256-GCM. The encrypted bytes are hashed with SHA-256. This hash becomes the content-addressed object identity for the prototype.

### 2. Shard

Encrypted bytes are split into `k` data shards. Kryden then generates `m` parity shards using systematic Reed-Solomon coding over GF(256). Any `k` valid shards can reconstruct the encrypted payload.

### 3. Commit

Each shard is committed to a Merkle root. The manifest records that root, leaf size, and leaf count. A client can later challenge a peer to reveal sampled leaves plus Merkle branches without downloading the whole shard.

### 4. Place

The client ranks peers deterministically per object and shard. Placement currently combines rendezvous-style hashing with a capacity bias so fuller peers become less attractive.

### 5. Audit

Each peer has an Ed25519 identity. During an audit, the client sends an object id, shard index, nonce, and deterministic sampled leaf indices. The peer returns leaf bytes, Merkle branches, and a signature over the full transcript. The client verifies the signature against the peer public key in the manifest and verifies every sampled leaf against the shard Merkle root.

### 6. Retrieve

The client requests shard descriptors from the manifest, verifies shard checksums, reconstructs encrypted bytes from any `k` shards, verifies ciphertext integrity, then decrypts locally.

### 7. Repair

Repair starts with an audit pass. Failed shard placements are treated as unavailable. If at least `k` shards can still be fetched, Kryden reconstructs the encrypted object, re-runs erasure coding deterministically, and stores replacement shard indexes on online peers. The content key is not needed for repair.

### 8. Schedule

A background scheduler tracks manifests in a durable JSON state file, runs audit and repair passes, updates manifests after successful repairs, and accumulates peer health history over time. This creates the first feedback loop needed for future reputation and incentive accounting.

## Public Manifest

The manifest may be stored publicly because it only contains object metadata, encrypted payload metadata, shard checksums, Merkle commitments, peer public keys, and peer placement descriptors. It does not contain the content key.

## Private Secret

The private secret contains the content key. Production Kryden needs a durable key-management layer: passphrases, social recovery, hardware keys, or wallet-controlled key wrapping.

## Non-Goals In This Prototype

- No production peer transport.
- No Sybil resistance.
- No payment settlement.
- No distributed production repair daemon.
- No durable shard persistence across process restarts.
- No namespace or file-system layer.
