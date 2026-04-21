import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";

describe("repair scheduler", () => {
  it("repairs failed shard placements onto online peers", () => {
    const plaintext = randomBytes(64 * 1024);
    const swarm = createLocalSwarm(9, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(plaintext, { dataShards: 4, parityShards: 2 });

    for (const descriptor of stored.manifest.shards.slice(0, 2)) {
      swarm.setPeerOnline(descriptor.peerId, false);
    }

    const report = client.repair(stored.manifest, 2);
    const postRepairAudits = client.audit(report.updatedManifest, 2);
    const recovered = client.get(report.updatedManifest, stored.secret);

    expect(report.repaired).toHaveLength(2);
    expect(report.failed).toHaveLength(0);
    expect(postRepairAudits.every((audit) => audit.ok)).toBe(true);
    expect(recovered.equals(plaintext)).toBe(true);
    expect(report.repaired.every((repair) => repair.oldPeerId !== repair.newPeerId)).toBe(true);
  });

  it("does not repair when the object is below reconstruction threshold", () => {
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(64 * 1024), { dataShards: 4, parityShards: 2 });

    for (const descriptor of stored.manifest.shards.slice(0, 3)) {
      swarm.setPeerOnline(descriptor.peerId, false);
    }

    const report = client.repair(stored.manifest);

    expect(report.repaired).toHaveLength(0);
    expect(report.failed).toHaveLength(3);
    expect(report.failed[0].reason).toMatch(/need 4/i);
  });

  it("excludes failed-audit shards from reconstruction", () => {
    const plaintext = randomBytes(64 * 1024);
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(plaintext, { dataShards: 4, parityShards: 2 });
    const manifest = {
      ...stored.manifest,
      shards: stored.manifest.shards.map((descriptor, index) =>
        index === 0 ? { ...descriptor, merkleRoot: "00".repeat(32) } : { ...descriptor }
      )
    };

    const report = client.repair(manifest, 2);
    const postRepairAudits = client.audit(report.updatedManifest, 2);
    const recovered = client.get(report.updatedManifest, stored.secret);

    expect(report.repaired).toHaveLength(1);
    expect(report.failed).toHaveLength(0);
    expect(postRepairAudits.every((audit) => audit.ok)).toBe(true);
    expect(recovered.equals(plaintext)).toBe(true);
  });
});
