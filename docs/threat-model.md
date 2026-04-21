# Threat Model

## Protected In The Prototype

- **Peer confidentiality:** Peers receive only encrypted shards.
- **Shard tampering:** Shards carry checksums and are rejected before reconstruction.
- **Manifest/key separation:** Public manifests do not include decryption keys.
- **Availability under loss:** Objects survive up to `m` shard losses when encoded as `k + m`.
- **Sampled possession audits:** Online peers can sign Merkle leaf samples for stored encrypted shards.
- **Peer proof attribution:** Audit signatures bind the proof transcript to the peer public key in the manifest.
- **Plaintext-safe repair:** Failed placements can be repaired from encrypted shards without requiring the client content key.
- **Durable maintenance memory:** Scheduler state records tracked manifests, run history, and peer health across process exits.
- **Correlated-risk reduction:** Placement and repair prefer distinct failure-domain buckets.
- **Repair capacity reservation:** Regular writes cannot consume the headroom reserved for repair.
- **Manifest transition atomicity:** Scheduler-driven manifest updates, placement changes, peer health, repair events, and run history commit in one SQLite transaction.
- **Restart after interruption:** Stale running transitions are abandoned on restart before the next repair cycle commits from the last durable manifest.

## Not Yet Protected

- **Sybil attacks:** A hostile operator can pretend to be many peers.
- **Eclipse attacks:** Discovery and routing do not exist yet.
- **Full storage fraud:** Sampled audits raise fraud cost but do not prove every byte is present on every challenge.
- **Economic griefing:** No collateral, reputation, or pricing model exists yet.
- **Challenge grinding:** Audit sampling is local and not yet anchored to public randomness.
- **Shard durability across restarts:** Peer identities can be serialized, but shard payloads are still in-memory only.
- **State confidentiality:** Scheduler SQLite state includes manifests, placements, repair events, and peer health; production needs access controls around the database file.
- **Failure-domain truthfulness:** Buckets are self-reported simulator labels. Production needs signed, policy-checked, and abuse-resistant topology claims.
- **Shard side effects before commit:** A killed local repair can leave extra in-memory shard copies on replacement peers even though the manifest is not updated. Production durable shard stores need garbage collection for uncommitted writes.
- **Traffic analysis:** The local simulator does not hide access patterns.
- **Malicious clients:** Quotas, abuse controls, and spam resistance are not implemented.

## Security Direction

The next security milestone should add durable shard persistence, public-randomness audit scheduling, and repair workflows before adding payments. Incentives built on unverifiable storage claims will reward fraud.
