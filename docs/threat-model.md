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
- **Quality-aware placement:** Peers with poor audit history or repeated failures are penalized or denied admission before receiving new shards.
- **Repair-thrash reduction:** Repair caps, object cooldown, and degraded-object backoff prevent one unstable object from monopolizing maintenance cycles.
- **Runtime request attribution:** Protected peer-runtime write, repair, retrieve, audit, and admin requests must be signed by a configured coordinator or owner authority.
- **Replay resistance:** Peer runtimes reject stale signed request envelopes and remember accepted nonces until expiry.
- **Transport encryption option:** Peer runtimes can serve HTTPS when configured with TLS key and certificate material. This protects transport confidentiality separately from signed request authorization.
- **Shard durability across restarts:** Peer runtimes can persist shard payloads with temp-file writes, atomic rename, startup index rebuild, and obsolete-generation cleanup.
- **Distributed degradation coverage:** Runtime tests exercise peer partitions, delayed responses, stale membership records, replayed heartbeats, and repair write interruption across process boundaries.

## Not Yet Protected

- **Decentralized control-plane authority:** The prototype intentionally uses a trusted coordinator model. It does not yet support consensus over placement, repair, membership, or ownership state.
- **Sybil attacks:** A hostile operator can pretend to be many peers.
- **Eclipse attacks:** Discovery and routing do not exist yet.
- **Full storage fraud:** Sampled audits raise fraud cost but do not prove every byte is present on every challenge.
- **Economic griefing:** No collateral, reputation, or pricing model exists yet.
- **Challenge grinding:** Audit sampling is local and not yet anchored to public randomness.
- **State confidentiality:** Scheduler SQLite state includes manifests, placements, repair events, and peer health; production needs access controls around the database file.
- **Failure-domain truthfulness:** Buckets are self-reported simulator labels. Production needs signed, policy-checked, and abuse-resistant topology claims.
- **Shard side effects before commit:** A killed local repair can leave extra in-memory shard copies on replacement peers even though the manifest is not updated. Production durable shard stores need garbage collection for uncommitted writes.
- **Suppression tuning:** Overly aggressive cooldown or backoff settings can delay repair under real degradation.
- **Traffic analysis:** HTTPS protects payloads on the wire but does not hide timing, endpoint, object access, or repair patterns.
- **Malicious clients:** Quotas, abuse controls, and spam resistance are not implemented.

## Security Direction

The next security milestone should add public-randomness audit scheduling and more hostile distributed repair workflows before adding payments. Incentives built on unverifiable storage claims will reward fraud.
