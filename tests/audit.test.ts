import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";
import {
  createStorageAuditChallenge,
  verifyStorageAuditProof
} from "../src/swarm/audit.js";

describe("storage audits", () => {
  it("accepts signed Merkle proofs from the assigned peer", () => {
    const swarm = createLocalSwarm(6, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(16 * 1024), { dataShards: 4, parityShards: 2 });

    const audits = client.audit(stored.manifest, 2);

    expect(audits).toHaveLength(6);
    expect(audits.every((audit) => audit.ok)).toBe(true);
    expect(audits.every((audit) => audit.proof?.signature)).toBe(true);
  });

  it("reports offline peers as failed audits", () => {
    const swarm = createLocalSwarm(6, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(16 * 1024), { dataShards: 4, parityShards: 2 });
    swarm.setPeerOnline(stored.manifest.shards[0].peerId, false);

    const audits = client.audit(stored.manifest);

    expect(audits.filter((audit) => audit.ok)).toHaveLength(5);
    expect(audits.find((audit) => !audit.ok)?.error).toMatch(/proof/i);
  });

  it("rejects forged proofs even when Merkle branches are valid", () => {
    const swarm = createLocalSwarm(6, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(16 * 1024), { dataShards: 4, parityShards: 2 });
    const descriptor = stored.manifest.shards[0];
    const peer = swarm.peers.find((candidate) => candidate.id === descriptor.peerId);
    if (!peer) {
      throw new Error("test peer missing");
    }

    const challenge = createStorageAuditChallenge(descriptor, stored.manifest.contentId, 2);
    const proof = peer.respondToAudit(challenge, descriptor);
    if (!proof) {
      throw new Error("test proof missing");
    }

    proof.peerId = stored.manifest.shards[1].peerId;

    expect(verifyStorageAuditProof(stored.manifest, descriptor, challenge, proof)).toBe(false);
  });
});

