import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
  BackgroundRepairScheduler,
  JsonStateStore,
  KrydenClient,
  createLocalSwarm
} from "../src/kryden.js";

describe("background audit and repair scheduler", () => {
  it("repairs tracked objects and persists peer health", async () => {
    const statePath = await createStatePath();
    const plaintext = randomBytes(64 * 1024);
    const swarm = createLocalSwarm(10, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new JsonStateStore(statePath);
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
    const store = new JsonStateStore(statePath);
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

  it("stores state as durable JSON", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(6, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new JsonStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store);
    const stored = client.put(randomBytes(4096), { dataShards: 3, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    await scheduler.runOnce();

    const raw = await readFile(statePath, "utf8");
    const parsed = JSON.parse(raw) as { version: number; objects: Record<string, unknown>; runs: unknown[] };

    expect(parsed.version).toBe(1);
    expect(Object.keys(parsed.objects)).toEqual([stored.manifest.contentId]);
    expect(parsed.runs).toHaveLength(1);
  });
});

async function createStatePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "kryden-state-"));
  return join(directory, "state.json");
}

