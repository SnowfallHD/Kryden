import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";

import type { StoredObjectManifest } from "../storage/manifest.js";
import type { StorageAuditResult } from "../swarm/audit.js";
import type { RepairReport, ShardRepair, ShardRepairFailure } from "../swarm/repair.js";

export interface TrackedObjectRecord {
  contentId: string;
  manifest: StoredObjectManifest;
  registeredAt: string;
  updatedAt: string;
}

export interface PeerHealthRecord {
  peerId: string;
  auditsPassed: number;
  auditsFailed: number;
  consecutiveFailures: number;
  repairedShards: number;
  lastAuditAt?: string;
  lastOkAt?: string;
  lastFailureAt?: string;
  lastError?: string;
}

export interface SchedulerObjectRunRecord {
  contentId: string;
  healthyShards: number;
  requiredShards: number;
  auditsPassed: number;
  auditsFailed: number;
  repaired: ShardRepair[];
  failedRepairs: ShardRepairFailure[];
}

export interface SchedulerRunRecord {
  runId: string;
  startedAt: string;
  completedAt: string;
  objectsAudited: number;
  shardsAudited: number;
  auditsPassed: number;
  auditsFailed: number;
  repairsSucceeded: number;
  repairsFailed: number;
  objects: SchedulerObjectRunRecord[];
}

export interface KrydenStateSnapshot {
  version: 1;
  objects: Record<string, TrackedObjectRecord>;
  peers: Record<string, PeerHealthRecord>;
  runs: SchedulerRunRecord[];
}

export class JsonStateStore {
  readonly path: string;
  private readonly maxRunHistory: number;

  constructor(path: string, maxRunHistory = 100) {
    if (!path) {
      throw new Error("State store path is required");
    }

    if (!Number.isInteger(maxRunHistory) || maxRunHistory <= 0) {
      throw new Error("maxRunHistory must be a positive integer");
    }

    this.path = path;
    this.maxRunHistory = maxRunHistory;
  }

  async load(): Promise<KrydenStateSnapshot> {
    try {
      const raw = await readFile(this.path, "utf8");
      const parsed = JSON.parse(raw) as KrydenStateSnapshot;
      if (parsed.version !== 1) {
        throw new Error(`Unsupported Kryden state version: ${String(parsed.version)}`);
      }

      return {
        version: 1,
        objects: parsed.objects ?? {},
        peers: parsed.peers ?? {},
        runs: parsed.runs ?? []
      };
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        return emptyState();
      }

      throw error;
    }
  }

  async save(snapshot: KrydenStateSnapshot): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    const tmpPath = `${this.path}.${process.pid}.${randomUUID()}.tmp`;
    await writeFile(tmpPath, `${JSON.stringify(snapshot, null, 2)}\n`);
    await rename(tmpPath, this.path);
  }

  async trackObject(manifest: StoredObjectManifest, now = new Date()): Promise<TrackedObjectRecord> {
    const snapshot = await this.load();
    const timestamp = now.toISOString();
    const existing = snapshot.objects[manifest.contentId];
    const record: TrackedObjectRecord = {
      contentId: manifest.contentId,
      manifest: cloneManifest(manifest),
      registeredAt: existing?.registeredAt ?? timestamp,
      updatedAt: timestamp
    };

    snapshot.objects[manifest.contentId] = record;
    await this.save(snapshot);
    return cloneTrackedObject(record);
  }

  async updateObjectManifest(manifest: StoredObjectManifest, now = new Date()): Promise<void> {
    const snapshot = await this.load();
    const timestamp = now.toISOString();
    const existing = snapshot.objects[manifest.contentId];
    snapshot.objects[manifest.contentId] = {
      contentId: manifest.contentId,
      manifest: cloneManifest(manifest),
      registeredAt: existing?.registeredAt ?? timestamp,
      updatedAt: timestamp
    };
    await this.save(snapshot);
  }

  async getTrackedObjects(): Promise<TrackedObjectRecord[]> {
    const snapshot = await this.load();
    return Object.values(snapshot.objects).map(cloneTrackedObject);
  }

  async recordSchedulerRun(
    reports: readonly RepairReport<StoredObjectManifest>[],
    startedAt: Date,
    completedAt: Date
  ): Promise<SchedulerRunRecord> {
    const snapshot = await this.load();
    const run = buildRunRecord(reports, startedAt, completedAt);

    for (const report of reports) {
      snapshot.objects[report.updatedManifest.contentId] = {
        contentId: report.updatedManifest.contentId,
        manifest: cloneManifest(report.updatedManifest),
        registeredAt:
          snapshot.objects[report.updatedManifest.contentId]?.registeredAt ?? startedAt.toISOString(),
        updatedAt: completedAt.toISOString()
      };
      updatePeerHealth(snapshot.peers, report.audits, report.repaired, completedAt);
    }

    snapshot.runs = [...snapshot.runs, run].slice(-this.maxRunHistory);
    await this.save(snapshot);
    return run;
  }
}

export function emptyState(): KrydenStateSnapshot {
  return {
    version: 1,
    objects: {},
    peers: {},
    runs: []
  };
}

function buildRunRecord(
  reports: readonly RepairReport<StoredObjectManifest>[],
  startedAt: Date,
  completedAt: Date
): SchedulerRunRecord {
  const objects = reports.map((report) => {
    const auditsPassed = report.audits.filter((audit) => audit.ok).length;
    const auditsFailed = report.audits.length - auditsPassed;
    return {
      contentId: report.updatedManifest.contentId,
      healthyShards: report.healthyShards,
      requiredShards: report.requiredShards,
      auditsPassed,
      auditsFailed,
      repaired: report.repaired,
      failedRepairs: report.failed
    };
  });

  return {
    runId: randomUUID(),
    startedAt: startedAt.toISOString(),
    completedAt: completedAt.toISOString(),
    objectsAudited: objects.length,
    shardsAudited: objects.reduce((total, object) => total + object.auditsPassed + object.auditsFailed, 0),
    auditsPassed: objects.reduce((total, object) => total + object.auditsPassed, 0),
    auditsFailed: objects.reduce((total, object) => total + object.auditsFailed, 0),
    repairsSucceeded: objects.reduce((total, object) => total + object.repaired.length, 0),
    repairsFailed: objects.reduce((total, object) => total + object.failedRepairs.length, 0),
    objects
  };
}

function updatePeerHealth(
  peers: Record<string, PeerHealthRecord>,
  audits: readonly StorageAuditResult[],
  repairs: readonly ShardRepair[],
  timestamp: Date
): void {
  const auditedAt = timestamp.toISOString();

  for (const audit of audits) {
    const record = (peers[audit.peerId] ??= {
      peerId: audit.peerId,
      auditsPassed: 0,
      auditsFailed: 0,
      consecutiveFailures: 0,
      repairedShards: 0
    });

    record.lastAuditAt = auditedAt;
    if (audit.ok) {
      record.auditsPassed += 1;
      record.consecutiveFailures = 0;
      record.lastOkAt = auditedAt;
      delete record.lastError;
    } else {
      record.auditsFailed += 1;
      record.consecutiveFailures += 1;
      record.lastFailureAt = auditedAt;
      record.lastError = audit.error ?? "Audit failed";
    }
  }

  for (const repair of repairs) {
    const record = (peers[repair.newPeerId] ??= {
      peerId: repair.newPeerId,
      auditsPassed: 0,
      auditsFailed: 0,
      consecutiveFailures: 0,
      repairedShards: 0
    });
    record.repairedShards += 1;
  }
}

function cloneTrackedObject(record: TrackedObjectRecord): TrackedObjectRecord {
  return {
    ...record,
    manifest: cloneManifest(record.manifest)
  };
}

function cloneManifest(manifest: StoredObjectManifest): StoredObjectManifest {
  return {
    ...manifest,
    encryption: { ...manifest.encryption },
    erasure: { ...manifest.erasure },
    shards: manifest.shards.map((descriptor) => ({ ...descriptor }))
  };
}

