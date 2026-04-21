import { spawnSync } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";
import { BackgroundRepairScheduler } from "../src/scheduler/backgroundRepairScheduler.js";
import { SQLITE_STATE_SCHEMA_VERSION, SQLiteStateStore } from "../src/state/store.js";
import { rankPeersForShard } from "../src/swarm/placement.js";

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
    const schedulerEventCount = db.prepare("SELECT COUNT(*) AS count FROM scheduler_events").get() as {
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
    const schemaVersion = db.prepare("PRAGMA user_version").get() as { user_version: number };
    const schemaMeta = db.prepare(`
      SELECT value FROM schema_meta
      WHERE key = 'state_schema_version'
    `).get() as { value: string };
    db.close();

    expect(state.version).toBe(1);
    expect(state.schemaVersion).toBe(SQLITE_STATE_SCHEMA_VERSION);
    expect(schemaVersion.user_version).toBe(SQLITE_STATE_SCHEMA_VERSION);
    expect(Number(schemaMeta.value)).toBe(SQLITE_STATE_SCHEMA_VERSION);
    expect(Object.keys(state.objects)).toEqual([stored.manifest.contentId]);
    expect(state.runs).toHaveLength(1);
    expect(tables).toEqual(
      expect.arrayContaining([
        "peers",
        "peer_health",
        "schema_meta",
        "manifests",
        "shard_placements",
        "scheduler_runs",
        "state_transitions",
        "scheduler_events",
        "repair_events"
      ])
    );
    expect(placementCount.count).toBe(stored.manifest.shards.length);
    expect(repairEventCount.count).toBe(0);
    expect(schedulerEventCount.count).toBeGreaterThan(0);
    expect(peerAccounting.count).toBeGreaterThan(0);
  });

  it("rejects newer SQLite schemas instead of migrating blindly", async () => {
    const statePath = await createStatePath();
    const db = new DatabaseSync(statePath);
    db.exec(`PRAGMA user_version = ${SQLITE_STATE_SCHEMA_VERSION + 1}`);
    db.close();

    expect(() => new SQLiteStateStore(statePath)).toThrow(/Unsupported SQLite schema version/i);
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

  it("rolls back repair state when durable commit fails mid-transaction", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, { sampleCount: 2 });
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const failedPeerId = stored.manifest.shards[0].peerId;

    await scheduler.trackObject(stored.manifest);
    swarm.setPeerOnline(failedPeerId, false);
    store.injectCommitFaultOnce("database disk full during repair commit");

    await expect(scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"))).rejects.toThrow(/disk full/i);
    const state = await store.load();
    const transitions = Object.values(state.transitions);
    const db = new DatabaseSync(statePath);
    const repairEventCount = db.prepare("SELECT COUNT(*) AS count FROM repair_events").get() as {
      count: number;
    };
    db.close();

    expect(transitions).toHaveLength(1);
    expect(transitions[0].status).toBe("abandoned");
    expect(state.runs).toHaveLength(0);
    expect(repairEventCount.count).toBe(0);
    expect(state.objects[stored.manifest.contentId].manifest).toEqual(stored.manifest);
    expect(state.peers[failedPeerId].auditsFailed).toBe(0);
    expect(state.peers[failedPeerId].consecutiveFailures).toBe(0);
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

  it("recovers after a scheduler process is killed mid-transition", async () => {
    const directory = await mkdtemp(join(tmpdir(), "kryden-crash-"));
    const statePath = join(directory, "state.sqlite");
    const crashScript = join(directory, "crash-mid-transition.mjs");
    const plaintext = randomBytes(32 * 1024);
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const initialStore = new SQLiteStateStore(statePath);
    const stored = client.put(plaintext, { dataShards: 4, parityShards: 2 });
    const failedPeerId = stored.manifest.shards[0].peerId;

    await initialStore.trackObject(stored.manifest, new Date("2026-04-21T08:00:00.000Z"));
    initialStore.close();
    swarm.setPeerOnline(failedPeerId, false);
    await writeFile(
      crashScript,
      `
        import { SQLiteStateStore } from ${JSON.stringify(`${process.cwd()}/src/state/store.ts`)};
        const store = new SQLiteStateStore(process.env.KRYDEN_STATE_PATH);
        const trackedObjects = await store.getTrackedObjects();
        await store.beginSchedulerRun(
          trackedObjects.map((object) => object.contentId),
          new Date("2026-04-21T08:01:00.000Z")
        );
        process.kill(process.pid, "SIGKILL");
      `
    );

    const killed = spawnSync(process.execPath, ["--import", "tsx", crashScript], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        KRYDEN_STATE_PATH: statePath
      },
      encoding: "utf8"
    });

    expect(killed.signal).toBe("SIGKILL");

    const crashedStore = new SQLiteStateStore(statePath);
    const crashedState = await crashedStore.load();
    const runningTransition = Object.values(crashedState.transitions).find(
      (transition) => transition.status === "running"
    );

    expect(runningTransition).toBeDefined();
    expect(crashedState.runs).toHaveLength(0);
    expect(crashedState.objects[stored.manifest.contentId].manifest).toEqual(stored.manifest);
    crashedStore.close();

    const restartStore = new SQLiteStateStore(statePath);
    const restartedScheduler = new BackgroundRepairScheduler(swarm, restartStore, { sampleCount: 2 });
    const summary = await restartedScheduler.runOnce(new Date("2026-04-21T08:02:00.000Z"));
    const recoveredState = await restartStore.load();
    const transitions = Object.values(recoveredState.transitions);
    const recoveredManifest = recoveredState.objects[stored.manifest.contentId].manifest;
    const recovered = client.get(recoveredManifest, stored.secret);

    expect(summary.run.repairsSucceeded).toBe(1);
    expect(recovered.equals(plaintext)).toBe(true);
    expect(recoveredManifest.shards[0].peerId).not.toBe(failedPeerId);
    expect(transitions.some((transition) => transition.status === "abandoned")).toBe(true);
    expect(transitions.some((transition) => transition.status === "committed")).toBe(true);
    expect(transitions.every((transition) => transition.status !== "running")).toBe(true);
    expect(recoveredState.runs).toHaveLength(1);
  });

  it("remembers repeated invalid proofs from the same peer and excludes it from placement", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024, { failureDomainCount: 8 });
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const corruptedDescriptor = stored.manifest.shards[0];
    const scheduler = new BackgroundRepairScheduler(swarm, store, {
      sampleCount: corruptedDescriptor.merkleLeafCount ?? 1,
      maxRepairsPerRun: 0,
      degradedBackoffBaseMs: 0,
      degradedBackoffMaxMs: 0
    });

    await scheduler.trackObject(stored.manifest);
    swarm.corruptShard(
      corruptedDescriptor.peerId,
      stored.manifest.contentId,
      corruptedDescriptor.index
    );

    for (let runIndex = 0; runIndex < 6; runIndex += 1) {
      await scheduler.runOnce(new Date(`2026-04-21T08:0${runIndex}:00.000Z`));
    }

    const state = await store.load();
    const corruptPeerHealth = state.peers[corruptedDescriptor.peerId];
    const ranked = rankPeersForShard(
      "new-object-after-invalid-proofs",
      0,
      swarm.peers,
      corruptedDescriptor.size,
      { peerHealth: new Map(Object.entries(state.peers)) }
    );

    expect(corruptPeerHealth.auditsFailed).toBe(6);
    expect(corruptPeerHealth.consecutiveFailures).toBe(6);
    expect(state.runs).toHaveLength(6);
    expect(state.runs.every((run) => run.auditsFailed === 1)).toBe(true);
    expect(ranked.some((peer) => peer.id === corruptedDescriptor.peerId)).toBe(false);
  });

  it("enforces a per-run repair cap and backs off deferred degraded objects", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(10, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, {
      sampleCount: 2,
      maxRepairsPerRun: 1,
      degradedBackoffBaseMs: 60_000,
      degradedBackoffMaxMs: 60_000,
      objectCooldownMs: 10_000
    });
    const stored = client.put(randomBytes(64 * 1024), { dataShards: 4, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    for (const descriptor of stored.manifest.shards.slice(0, 2)) {
      swarm.setPeerOnline(descriptor.peerId, false);
    }

    const summary = await scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"));
    const state = await store.load();
    const tracked = state.objects[stored.manifest.contentId];

    expect(summary.run.repairsSucceeded).toBe(1);
    expect(summary.run.repairsFailed).toBe(1);
    expect(summary.run.objects[0].failedRepairs[0].reason).toMatch(/cap/i);
    expect(tracked.consecutiveDegradedRuns).toBe(1);
    expect(tracked.nextEligibleAt).toBe("2026-04-21T08:01:00.000Z");
  });

  it("persists degraded backoff across restart during consecutive failed cycles", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, {
      sampleCount: 2,
      maxRepairsPerRun: 0,
      degradedBackoffBaseMs: 60_000,
      degradedBackoffMaxMs: 120_000
    });
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });
    const failedPeerId = stored.manifest.shards[0].peerId;

    await scheduler.trackObject(stored.manifest);
    swarm.setPeerOnline(failedPeerId, false);

    const first = await scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"));
    const firstState = await store.load();
    store.close();

    const restartStore = new SQLiteStateStore(statePath);
    const restartedScheduler = new BackgroundRepairScheduler(swarm, restartStore, {
      sampleCount: 2,
      maxRepairsPerRun: 0,
      degradedBackoffBaseMs: 60_000,
      degradedBackoffMaxMs: 120_000
    });
    const skipped = await restartedScheduler.runOnce(new Date("2026-04-21T08:00:30.000Z"));
    const second = await restartedScheduler.runOnce(new Date("2026-04-21T08:01:00.000Z"));
    const finalState = await restartStore.load();
    const tracked = finalState.objects[stored.manifest.contentId];

    expect(first.run.repairsFailed).toBe(1);
    expect(firstState.objects[stored.manifest.contentId].consecutiveDegradedRuns).toBe(1);
    expect(firstState.objects[stored.manifest.contentId].nextEligibleAt).toBe("2026-04-21T08:01:00.000Z");
    expect(skipped.run.objectsAudited).toBe(0);
    expect(second.run.repairsFailed).toBe(1);
    expect(tracked.consecutiveDegradedRuns).toBe(2);
    expect(tracked.nextEligibleAt).toBe("2026-04-21T08:03:00.000Z");
  });

  it("suppresses objects during cooldown windows", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, {
      sampleCount: 2,
      objectCooldownMs: 60_000
    });
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    swarm.setPeerOnline(stored.manifest.shards[0].peerId, false);

    const first = await scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"));
    const second = await scheduler.runOnce(new Date("2026-04-21T08:00:30.000Z"));
    const third = await scheduler.runOnce(new Date("2026-04-21T08:01:01.000Z"));
    const state = await store.load();

    expect(first.run.objectsAudited).toBe(1);
    expect(first.run.repairsSucceeded).toBe(1);
    expect(second.run.objectsAudited).toBe(0);
    expect(second.run.repairsSucceeded).toBe(0);
    expect(third.run.objectsAudited).toBe(1);
    expect(state.objects[stored.manifest.contentId].nextEligibleAt).toBeUndefined();
  });

  it("persists metrics, event logs, and replayable traces for scheduler decisions", async () => {
    const statePath = await createStatePath();
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const store = new SQLiteStateStore(statePath);
    const scheduler = new BackgroundRepairScheduler(swarm, store, {
      sampleCount: 2,
      objectCooldownMs: 60_000
    });
    const stored = client.put(randomBytes(32 * 1024), { dataShards: 4, parityShards: 2 });

    await scheduler.trackObject(stored.manifest);
    swarm.setPeerOnline(stored.manifest.shards[0].peerId, false);

    const first = await scheduler.runOnce(new Date("2026-04-21T08:00:00.000Z"));
    const second = await scheduler.runOnce(new Date("2026-04-21T08:00:30.000Z"));
    const state = await store.load();
    const db = new DatabaseSync(statePath);
    const firstEventRows = db.prepare(`
      SELECT event_type, content_id, peer_id, shard_index, message, details_json
      FROM scheduler_events
      WHERE run_id = ?
      ORDER BY sequence ASC
    `).all(first.run.runId) as Array<{
      event_type: string;
      content_id: string | null;
      peer_id: string | null;
      shard_index: number | null;
      message: string;
      details_json: string | null;
    }>;
    db.close();

    expect(state.runs).toHaveLength(2);
    expect(first.run.metrics).toMatchObject({
      objectsTracked: 1,
      objectsEligible: 1,
      objectsSkipped: 0,
      objectsAudited: 1,
      auditsFailed: 1,
      repairsSucceeded: 1
    });
    expect(first.run.trace.events.map((event) => event.type)).toEqual(
      expect.arrayContaining([
        "scheduler.run.started",
        "scheduler.object.audited",
        "scheduler.audit.failed",
        "scheduler.repair.succeeded",
        "scheduler.object.repaired",
        "scheduler.run.completed"
      ])
    );
    expect(firstEventRows.map((row) => row.event_type)).toEqual(
      first.run.trace.events.map((event) => event.type)
    );
    expect(second.run.metrics.objectsTracked).toBe(1);
    expect(second.run.metrics.objectsEligible).toBe(0);
    expect(second.run.metrics.objectsSkipped).toBe(1);
    expect(second.run.trace.events.some((event) => event.type === "scheduler.object.skipped")).toBe(true);
  });
});

async function createStatePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "kryden-state-"));
  return join(directory, "state.sqlite");
}
