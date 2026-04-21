import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";

describe("failure-domain-aware placement", () => {
  it("spreads shard placements across buckets when enough domains exist", () => {
    const swarm = createLocalSwarm(8, 1024 * 1024, { failureDomainCount: 8 });
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });

    const buckets = stored.manifest.shards.map((descriptor) => descriptor.failureDomain?.bucket);

    expect(new Set(buckets).size).toBe(stored.manifest.shards.length);
  });

  it("keeps repaired shards out of already-used domains when alternatives exist", () => {
    const swarm = createLocalSwarm(10, 1024 * 1024, { failureDomainCount: 10 });
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const failedDescriptor = stored.manifest.shards[0];

    swarm.setPeerOnline(failedDescriptor.peerId, false);
    const report = client.repair(stored.manifest);
    const repairedDescriptor = report.updatedManifest.shards.find(
      (descriptor) => descriptor.index === failedDescriptor.index
    );
    const otherBuckets = new Set(
      report.updatedManifest.shards
        .filter((descriptor) => descriptor.index !== failedDescriptor.index)
        .map((descriptor) => descriptor.failureDomain?.bucket)
    );

    expect(report.repaired).toHaveLength(1);
    expect(repairedDescriptor?.failureDomain?.bucket).toBeDefined();
    expect(otherBuckets.has(repairedDescriptor?.failureDomain?.bucket)).toBe(false);
  });
});

