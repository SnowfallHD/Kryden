import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";
import { rankPeersForShard } from "../src/swarm/placement.js";
import { PeerStore } from "../src/swarm/peer.js";

describe("failure-domain-aware placement", () => {
  it("spreads shard placements across buckets when enough domains exist", () => {
    const swarm = createLocalSwarm(8, 1024 * 1024, { failureDomainCount: 8 });
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });

    const buckets = stored.manifest.shards.map((descriptor) => descriptor.failureDomain?.bucket);

    expect(new Set(buckets).size).toBe(stored.manifest.shards.length);
  });

  it("falls back to duplicate buckets when failure domains are exhausted", () => {
    const swarm = createLocalSwarm(8, 1024 * 1024, { failureDomainCount: 2 });
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const buckets = stored.manifest.shards.map((descriptor) => descriptor.failureDomain?.bucket);
    const uniqueBuckets = new Set(buckets);

    expect(stored.manifest.shards).toHaveLength(6);
    expect(uniqueBuckets.size).toBe(2);
    expect(uniqueBuckets.size).toBeLessThan(stored.manifest.shards.length);
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

  it("admits placement candidates based on audit health", () => {
    const peers = [
      new PeerStore("peer-bad", 1024 * 1024, undefined, { failureDomain: { bucket: "a" } }),
      new PeerStore("peer-good", 1024 * 1024, undefined, { failureDomain: { bucket: "b" } })
    ];
    const ranked = rankPeersForShard("object-a", 0, peers, 4096, {
      peerHealth: new Map([
        [
          "peer-bad",
          {
            auditsPassed: 0,
            auditsFailed: 8,
            consecutiveFailures: 6,
            repairedShards: 0
          }
        ],
        [
          "peer-good",
          {
            auditsPassed: 10,
            auditsFailed: 0,
            consecutiveFailures: 0,
            repairedShards: 3
          }
        ]
      ])
    });

    expect(ranked.map((peer) => peer.id)).toEqual(["peer-good"]);
  });

  it("scores free capacity above hash luck", () => {
    const fullPeer = new PeerStore("peer-full", 16 * 1024, undefined, {
      repairHeadroomBytes: 1024,
      failureDomain: { bucket: "a" }
    });
    fullPeer.store("filler", { index: 0, checksum: "unused", data: Buffer.alloc(12 * 1024) }, "repair");
    const openPeer = new PeerStore("peer-open", 16 * 1024, undefined, {
      repairHeadroomBytes: 1024,
      failureDomain: { bucket: "b" }
    });

    const ranked = rankPeersForShard("object-a", 0, [fullPeer, openPeer], 1024, {
      purpose: "repair"
    });

    expect(ranked[0].id).toBe("peer-open");
  });

  it("uses persisted peer health during scheduler repair placement", async () => {
    const swarm = createLocalSwarm(8, 1024 * 1024, { failureDomainCount: 8 });
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const failedDescriptor = stored.manifest.shards[0];
    const candidatePeer = swarm.peers.find(
      (peer) =>
        peer.id !== failedDescriptor.peerId &&
        !stored.manifest.shards.some((descriptor) => descriptor.peerId === peer.id)
    );
    if (!candidatePeer) {
      throw new Error("test candidate peer missing");
    }

    swarm.setPeerOnline(failedDescriptor.peerId, false);
    swarm.setPeerPlacementHealth({
      [candidatePeer.id]: {
        auditsPassed: 0,
        auditsFailed: 10,
        consecutiveFailures: 7,
        repairedShards: 0
      }
    });

    const report = client.repair(stored.manifest);

    expect(report.repaired).toHaveLength(1);
    expect(report.repaired[0].newPeerId).not.toBe(candidatePeer.id);
  });
});
