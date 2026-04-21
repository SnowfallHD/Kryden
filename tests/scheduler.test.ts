import { mkdtemp } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";
import { BackgroundRepairScheduler } from "../src/scheduler/backgroundRepairScheduler.js";
import { SQLiteStateStore } from "../src/state/store.js";

describe("background audit and repair scheduler", () => {
  it("repairs tracked objects and persists peer health", async () => {
    const statePath = await createStatePath();
    const plaintext = randomBytes(64 * 1024);
    const swarm = createLocalSwarm(10, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, { sampleCount: 2 });
    const stored = client.put(plaintext, { dataShards: 4, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    const failedPeerIds = stored.manifest.shards.slice(0, 2).map((descriptor) => descriptor.peerId);
    for (const peerId of failedPeerIds) {
      swarm.setPeerOnline(peerId, false);
    }

    const summary = await scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"));
    const state = await store.load();
    const updatedManifest = state.objects[stored.manifest.contentId].manifest;
    const recovered = client.get(updatedManifest, stored.secret);
    const postRepairAudits = client.audit(updatedManifest, 2);

    expect(summary.run.objectsAudited).toBe(1);
    expect(summary.run.repairsSucceeded).toBe(2);
    expect(summary.run.repairsFailed).toBe(0);
    expect(recovered.equals(plaintext)).toBe(true);
    expect(postRepairAudits.every((audit) => audit.ok)).toBe(true);
    expect(state.runs).toHaveLength(1);

    for (const peerId of failedPeerIds) {
      expect(state.peers[peerId].auditsFailed).toBe(1);
      expect(state.peers[peerId].consecutiveFailures).toBe(1);
    }

    const repairedPeerIds = summary.run.objects.flatMap((object) =>
      object.repaired.map((repair) => repair.newPeerId)
    );
    expect(repairedPeerIds.length).toBe(2);
    for (const peerId of repairedPeerIds) {
      expect(state.peers[peerId].repairedShards).toBeGreaterThan(0);
    }
  });

  it("accumulates health history across scheduler runs", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, { sampleCount: 2 });
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    await scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"));
    await scheduler.runOnce(new Date("2026-04-21T08:01:00.000Z"));
    const state = await store.load();

    expect(state.runs).toHaveLength(2);
    for (const descriptor of stored.manifest.shards) {
      expect(state.peers[descriptor.peerId].auditsPassed).toBe(2);
      expect(state.peers[descriptor.peerId].consecutiveFailures).toBe(0);
    }
  });

  it("stores state in durable SQLite tables", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(6, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store);
    const stored = client.put(randomBytes(4096), { dataShards: 3, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    await scheduler.runOnce();

    const state = await store.load();
    const db = new DatabaseSync(statePath);
    const tables = db.prepare(`
      SELECT name FROM sqlite_master
      WHERE type = 'table'
      ORDER BY name
    `).all().map((row) => (row as { name: string }).name);
    const placementCount = db.prepare("SELECT COUNT(*) AS count FROM shard_placements").get() as {
      count: number;
    };
    const repairEventCount = db.prepare("SELECT COUNT(*) AS count FROM repair_events").get() as {
      count: number;
    };
    const peerAccounting = db.prepare(`
      SELECT COUNT(*) AS count FROM peers
      WHERE capacity_bytes IS NOT NULL
        AND reserved_bytes IS NOT NULL
        AND repair_headroom_bytes IS NOT NULL
        AND used_bytes IS NOT NULL
        AND allocatable_bytes IS NOT NULL
    `).get() as { count: number };
    db.close();

    expect(state.version).toBe(1);
    expect(Object.keys(state.objects)).toEqual([stored.manifest.contentId]);
    expect(state.runs).toHaveLength(1);
    expect(tables).toEqual(
      expect.arrayContaining([
        "peers",
        "peer_health",
        "manifests",
        "shard_placements",
        "scheduler_runs",
        "state_transitions",
        "repair_events"
      ])
    );
    expect(placementCount.count).toBe(stored.manifest.shards.length);
    expect(repairEventCount.count).toBe(0);
    expect(peerAccounting.count).toBeGreaterThan(0);
  });

  it("does not update durable manifests until a transition commits", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const failedPeerId = stored.manifest.shards[0].peerId;

    await store.trackObject(stored.manifest, new Date("2026-04-21T08:00:00.000Z"));
    swarm.setPeerOnline(failedPeerId, false);
    const transition = await store.beginSchedulerRun(
      [stored.manifest.contentId],
      new Date("2026-04-21T08:01:00.000Z")
    );
    const report = client.repair(stored.manifest, 2);
    const beforeCommit = await store.load();

    expect(beforeCommit.transitions[transition.transitionId].status).toBe("running");
    expect(beforeCommit.objects[stored.manifest.contentId].manifest.shards[0].peerId).toBe(failedPeerId);

    await store.commitSchedulerRun(
      transition.transitionId,
      [report],
      new Date("2026-04-21T08:01:00.000Z"),
      new Date("2026-04-21T08:01:01.000Z")
    );
    const afterCommit = await store.load();

    expect(afterCommit.transitions[transition.transitionId].status).toBe("committed");
    expect(afterCommit.objects[stored.manifest.contentId].manifest.shards[0].peerId).not.toBe(failedPeerId);
  });

  it("marks failed transitions abandoned without changing tracked manifests", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(6, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const stored = client.put(randomBytes(4096), { dataShards: 3, parityShards: 2 });

    await store.trackObject(stored.manifest);
    const transition = await store.beginSchedulerRun([stored.manifest.contentId]);
    await store.abandonTransition(transition.transitionId, "simulated crash");
    const state = await store.load();

    expect(state.transitions[transition.transitionId].status).toBe("abandoned");
    expect(state.transitions[transition.transitionId].error).toBe("simulated crash");
    expect(state.objects[stored.manifest.contentId].manifest).toEqual(stored.manifest);
  });
});

async function createStatePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "kryden-state-"));
  return join(directory, "state.sqlite");
}
