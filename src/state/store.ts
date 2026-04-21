import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

import type { FailureDomain, ShardDescriptor, StoredObjectManifest } from "../storage/manifest.js";
import type { StorageAuditResult } from "../swarm/audit.js";
import type { LocalPeerRecord } from "../swarm/peer.js";
import type { RepairReport, ShardRepair, ShardRepairFailure } from "../swarm/repair.js";

export interface TrackedObjectRecord {
  contentId: string;
  manifest: StoredObjectManifest;
  registeredAt: string;
  updatedAt: string;
  consecutiveDegradedRuns: number;
  nextEligibleAt?: string;
  lastSchedulerRunAt?: string;
  lastRepairAt?: string;
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

export type StateTransitionStatus = "running" | "committed" | "abandoned";

export interface StateTransitionRecord {
  transitionId: string;
  status: StateTransitionStatus;
  startedAt: string;
  completedAt?: string;
  objectIds: string[];
  committedRunId?: string;
  error?: string;
}

export interface SchedulerMaintenancePolicy {
  objectCooldownMs: number;
  degradedBackoffBaseMs: number;
  degradedBackoffMaxMs: number;
}

export interface KrydenStateSnapshot {
  version: 1;
  objects: Record<string, TrackedObjectRecord>;
  peers: Record<string, PeerHealthRecord>;
  runs: SchedulerRunRecord[];
  transitions: Record<string, StateTransitionRecord>;
}

interface ManifestRow {
  content_id: string;
  manifest_json: string;
  registered_at: string;
  updated_at: string;
  consecutive_degraded_runs: number;
  next_eligible_at: string | null;
  last_scheduler_run_at: string | null;
  last_repair_at: string | null;
}

interface PeerHealthRow {
  peer_id: string;
  audits_passed: number;
  audits_failed: number;
  consecutive_failures: number;
  repaired_shards: number;
  last_audit_at: string | null;
  last_ok_at: string | null;
  last_failure_at: string | null;
  last_error: string | null;
}

interface SchedulerRunRow {
  run_id: string;
  started_at: string;
  completed_at: string;
  objects_audited: number;
  shards_audited: number;
  audits_passed: number;
  audits_failed: number;
  repairs_succeeded: number;
  repairs_failed: number;
  objects_json: string;
}

interface StateTransitionRow {
  transition_id: string;
  status: StateTransitionStatus;
  started_at: string;
  completed_at: string | null;
  object_ids_json: string;
  committed_run_id: string | null;
  error: string | null;
}

export class SQLiteStateStore {
  readonly path: string;
  private readonly maxRunHistory: number;
  private readonly db: DatabaseSync;
  private transactionDepth = 0;
  private commitFaultMessage: string | undefined;

  constructor(path: string, maxRunHistory = 100) {
    if (!path) {
      throw new Error("State store path is required");
    }

    if (!Number.isInteger(maxRunHistory) || maxRunHistory <= 0) {
      throw new Error("maxRunHistory must be a positive integer");
    }

    mkdirSync(dirname(path), { recursive: true });
    this.path = path;
    this.maxRunHistory = maxRunHistory;
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA foreign_keys = ON");
    this.db.exec("PRAGMA journal_mode = WAL");
    this.migrate();
  }

  async load(): Promise<KrydenStateSnapshot> {
    return this.loadSync();
  }

  async trackObject(manifest: StoredObjectManifest, now = new Date()): Promise<TrackedObjectRecord> {
    const timestamp = now.toISOString();
    const existing = this.getTrackedObjectSync(manifest.contentId);
    this.upsertManifest(manifest, existing?.registeredAt ?? timestamp, timestamp);
    return {
      contentId: manifest.contentId,
      manifest: cloneManifest(manifest),
      registeredAt: existing?.registeredAt ?? timestamp,
      updatedAt: timestamp,
      consecutiveDegradedRuns: existing?.consecutiveDegradedRuns ?? 0,
      nextEligibleAt: existing?.nextEligibleAt,
      lastSchedulerRunAt: existing?.lastSchedulerRunAt,
      lastRepairAt: existing?.lastRepairAt
    };
  }

  async updateObjectManifest(manifest: StoredObjectManifest, now = new Date()): Promise<void> {
    const timestamp = now.toISOString();
    const existing = this.getTrackedObjectSync(manifest.contentId);
    this.upsertManifest(manifest, existing?.registeredAt ?? timestamp, timestamp);
  }

  async getTrackedObjects(): Promise<TrackedObjectRecord[]> {
    return this.getTrackedObjectsSync();
  }

  async getEligibleTrackedObjects(now = new Date()): Promise<TrackedObjectRecord[]> {
    return this.db.prepare(`
      SELECT * FROM manifests
      WHERE next_eligible_at IS NULL OR next_eligible_at <= ?
      ORDER BY registered_at ASC, content_id ASC
    `).all(now.toISOString()).map((row) => trackedObjectFromRow(row as unknown as ManifestRow));
  }

  async beginSchedulerRun(objectIds: readonly string[], startedAt = new Date()): Promise<StateTransitionRecord> {
    const transition: StateTransitionRecord = {
      transitionId: randomUUID(),
      status: "running",
      startedAt: startedAt.toISOString(),
      objectIds: [...objectIds]
    };

    this.db.prepare(`
      INSERT INTO state_transitions (
        transition_id, status, started_at, object_ids_json
      ) VALUES (?, ?, ?, ?)
    `).run(
      transition.transitionId,
      transition.status,
      transition.startedAt,
      JSON.stringify(transition.objectIds)
    );

    return { ...transition };
  }

  async commitSchedulerRun(
    transitionId: string,
    reports: readonly RepairReport<StoredObjectManifest>[],
    startedAt: Date,
    completedAt: Date,
    peerRecords: readonly LocalPeerRecord[] = [],
    maintenancePolicy: SchedulerMaintenancePolicy = DEFAULT_MAINTENANCE_POLICY
  ): Promise<SchedulerRunRecord> {
    const transition = this.getTransitionSync(transitionId);
    if (!transition) {
      throw new Error(`Unknown scheduler transition ${transitionId}`);
    }

    if (transition.status !== "running") {
      throw new Error(`Scheduler transition ${transitionId} is already ${transition.status}`);
    }

    const run = buildRunRecord(reports, startedAt, completedAt);
    this.transaction(() => {
      for (const peer of peerRecords) {
        this.upsertPeerAccounting(peer, completedAt.toISOString());
      }

      for (const report of reports) {
        const existing = this.getTrackedObjectSync(report.updatedManifest.contentId);
        this.upsertManifest(
          report.updatedManifest,
          existing?.registeredAt ?? startedAt.toISOString(),
          completedAt.toISOString()
        );
        this.updateObjectMaintenance(
          report,
          existing,
          startedAt,
          completedAt,
          maintenancePolicy
        );
        this.updatePeerHealth(report.audits, report.repaired, completedAt);
      }

      this.throwInjectedCommitFault();
      this.insertSchedulerRun(run);
      this.insertRepairEvents(run);
      this.trimRunHistory();
      this.db.prepare(`
        UPDATE state_transitions
        SET status = 'committed',
          completed_at = ?,
          committed_run_id = ?,
          error = NULL
        WHERE transition_id = ? AND status = 'running'
      `).run(completedAt.toISOString(), run.runId, transitionId);
    });

    return run;
  }

  async abandonTransition(
    transitionId: string,
    error: string,
    completedAt = new Date()
  ): Promise<void> {
    this.db.prepare(`
      UPDATE state_transitions
      SET status = 'abandoned',
        completed_at = ?,
        error = ?
      WHERE transition_id = ? AND status = 'running'
    `).run(completedAt.toISOString(), error, transitionId);
  }

  async recoverInterruptedTransitions(
    error = "Recovered interrupted scheduler transition",
    completedAt = new Date()
  ): Promise<StateTransitionRecord[]> {
    const running = this.db.prepare(`
      SELECT * FROM state_transitions
      WHERE status = 'running'
      ORDER BY started_at ASC, transition_id ASC
    `).all().map((row) => transitionFromRow(row as unknown as StateTransitionRow));

    if (running.length === 0) {
      return [];
    }

    this.transaction(() => {
      this.db.prepare(`
        UPDATE state_transitions
        SET status = 'abandoned',
          completed_at = ?,
          error = ?
        WHERE status = 'running'
      `).run(completedAt.toISOString(), error);
    });

    return running.map((transition) => ({
      ...transition,
      status: "abandoned",
      completedAt: completedAt.toISOString(),
      error
    }));
  }

  injectCommitFaultOnce(message = "Injected SQLite commit failure"): void {
    this.commitFaultMessage = message;
  }

  async recordSchedulerRun(
    reports: readonly RepairReport<StoredObjectManifest>[],
    startedAt: Date,
    completedAt: Date,
    peerRecords: readonly LocalPeerRecord[] = [],
    maintenancePolicy: SchedulerMaintenancePolicy = DEFAULT_MAINTENANCE_POLICY
  ): Promise<SchedulerRunRecord> {
    const transition = await this.beginSchedulerRun(
      reports.map((report) => report.updatedManifest.contentId),
      startedAt
    );
    return this.commitSchedulerRun(
      transition.transitionId,
      reports,
      startedAt,
      completedAt,
      peerRecords,
      maintenancePolicy
    );
  }

  close(): void {
    this.db.close();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS peers (
        peer_id TEXT PRIMARY KEY,
        public_key_pem TEXT,
        capacity_bytes INTEGER,
        reserved_bytes INTEGER,
        repair_headroom_bytes INTEGER,
        used_bytes INTEGER,
        allocatable_bytes INTEGER,
        regular_free_bytes INTEGER,
        repair_free_bytes INTEGER,
        online INTEGER,
        failure_bucket TEXT,
        device_group TEXT,
        host TEXT,
        subnet TEXT,
        first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS peer_health (
        peer_id TEXT PRIMARY KEY REFERENCES peers(peer_id) ON DELETE CASCADE,
        audits_passed INTEGER NOT NULL DEFAULT 0,
        audits_failed INTEGER NOT NULL DEFAULT 0,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        repaired_shards INTEGER NOT NULL DEFAULT 0,
        last_audit_at TEXT,
        last_ok_at TEXT,
        last_failure_at TEXT,
        last_error TEXT
      );

      CREATE TABLE IF NOT EXISTS manifests (
        content_id TEXT PRIMARY KEY,
        manifest_json TEXT NOT NULL,
        registered_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        consecutive_degraded_runs INTEGER NOT NULL DEFAULT 0,
        next_eligible_at TEXT,
        last_scheduler_run_at TEXT,
        last_repair_at TEXT
      );

      CREATE TABLE IF NOT EXISTS shard_placements (
        content_id TEXT NOT NULL REFERENCES manifests(content_id) ON DELETE CASCADE,
        shard_index INTEGER NOT NULL,
        peer_id TEXT NOT NULL REFERENCES peers(peer_id),
        peer_public_key TEXT,
        failure_bucket TEXT,
        device_group TEXT,
        host TEXT,
        subnet TEXT,
        size INTEGER NOT NULL,
        checksum TEXT NOT NULL,
        merkle_root TEXT,
        merkle_leaf_size INTEGER,
        merkle_leaf_count INTEGER,
        PRIMARY KEY (content_id, shard_index)
      );

      CREATE TABLE IF NOT EXISTS scheduler_runs (
        run_id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        completed_at TEXT NOT NULL,
        objects_audited INTEGER NOT NULL,
        shards_audited INTEGER NOT NULL,
        audits_passed INTEGER NOT NULL,
        audits_failed INTEGER NOT NULL,
        repairs_succeeded INTEGER NOT NULL,
        repairs_failed INTEGER NOT NULL,
        objects_json TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS state_transitions (
        transition_id TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK (status IN ('running', 'committed', 'abandoned')),
        started_at TEXT NOT NULL,
        completed_at TEXT,
        object_ids_json TEXT NOT NULL,
        committed_run_id TEXT REFERENCES scheduler_runs(run_id) ON DELETE SET NULL,
        error TEXT
      );

      CREATE TABLE IF NOT EXISTS repair_events (
        event_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES scheduler_runs(run_id) ON DELETE CASCADE,
        content_id TEXT NOT NULL,
        shard_index INTEGER NOT NULL,
        event_type TEXT NOT NULL CHECK (event_type IN ('repaired', 'failed')),
        old_peer_id TEXT,
        new_peer_id TEXT,
        reason TEXT NOT NULL
      );
    `);
    this.ensurePeerColumns();
    this.ensureManifestColumns();
  }

  private ensurePeerColumns(): void {
    const existing = new Set(
      this.db.prepare("PRAGMA table_info(peers)").all().map((row) => (row as { name: string }).name)
    );
    const columns: Array<[string, string]> = [
      ["capacity_bytes", "INTEGER"],
      ["reserved_bytes", "INTEGER"],
      ["repair_headroom_bytes", "INTEGER"],
      ["used_bytes", "INTEGER"],
      ["allocatable_bytes", "INTEGER"],
      ["regular_free_bytes", "INTEGER"],
      ["repair_free_bytes", "INTEGER"],
      ["online", "INTEGER"]
    ];

    for (const [name, definition] of columns) {
      if (!existing.has(name)) {
        this.db.exec(`ALTER TABLE peers ADD COLUMN ${name} ${definition}`);
      }
    }
  }

  private ensureManifestColumns(): void {
    const existing = new Set(
      this.db.prepare("PRAGMA table_info(manifests)").all().map((row) => (row as { name: string }).name)
    );
    const columns: Array<[string, string]> = [
      ["consecutive_degraded_runs", "INTEGER NOT NULL DEFAULT 0"],
      ["next_eligible_at", "TEXT"],
      ["last_scheduler_run_at", "TEXT"],
      ["last_repair_at", "TEXT"]
    ];

    for (const [name, definition] of columns) {
      if (!existing.has(name)) {
        this.db.exec(`ALTER TABLE manifests ADD COLUMN ${name} ${definition}`);
      }
    }
  }

  private loadSync(): KrydenStateSnapshot {
    const objects = Object.fromEntries(
      this.getTrackedObjectsSync().map((object) => [object.contentId, object])
    );
    const peers = Object.fromEntries(
      this.db.prepare("SELECT * FROM peer_health ORDER BY peer_id").all().map((row) => {
        const health = peerHealthFromRow(row as unknown as PeerHealthRow);
        return [health.peerId, health];
      })
    );
    const runs = this.db.prepare(`
      SELECT * FROM scheduler_runs
      ORDER BY completed_at ASC, run_id ASC
    `).all().map((row) => schedulerRunFromRow(row as unknown as SchedulerRunRow));
    const transitions = Object.fromEntries(
      this.db.prepare(`
        SELECT * FROM state_transitions
        ORDER BY started_at ASC, transition_id ASC
      `).all().map((row) => {
        const transition = transitionFromRow(row as unknown as StateTransitionRow);
        return [transition.transitionId, transition];
      })
    );

    return {
      version: 1,
      objects,
      peers,
      runs,
      transitions
    };
  }

  private getTrackedObjectsSync(): TrackedObjectRecord[] {
    return this.db.prepare(`
      SELECT * FROM manifests
      ORDER BY registered_at ASC, content_id ASC
    `).all().map((row) => trackedObjectFromRow(row as unknown as ManifestRow));
  }

  private getTrackedObjectSync(contentId: string): TrackedObjectRecord | undefined {
    const row = this.db.prepare("SELECT * FROM manifests WHERE content_id = ?").get(contentId);
    return row ? trackedObjectFromRow(row as unknown as ManifestRow) : undefined;
  }

  private getTransitionSync(transitionId: string): StateTransitionRecord | undefined {
    const row = this.db.prepare("SELECT * FROM state_transitions WHERE transition_id = ?").get(transitionId);
    return row ? transitionFromRow(row as unknown as StateTransitionRow) : undefined;
  }

  private upsertManifest(manifest: StoredObjectManifest, registeredAt: string, updatedAt: string): void {
    this.transaction(() => {
      this.db.prepare(`
        INSERT INTO manifests (content_id, manifest_json, registered_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(content_id) DO UPDATE SET
          manifest_json = excluded.manifest_json,
          updated_at = excluded.updated_at
      `).run(manifest.contentId, JSON.stringify(manifest), registeredAt, updatedAt);

      this.db.prepare("DELETE FROM shard_placements WHERE content_id = ?").run(manifest.contentId);
      for (const descriptor of manifest.shards) {
        this.upsertPeerFromDescriptor(descriptor, updatedAt);
        this.insertShardPlacement(manifest.contentId, descriptor);
      }
    });
  }

  private upsertPeerFromDescriptor(descriptor: ShardDescriptor, seenAt: string): void {
    const domain = descriptor.failureDomain;
    this.db.prepare(`
      INSERT INTO peers (
        peer_id, public_key_pem, failure_bucket, device_group, host, subnet, first_seen_at, last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(peer_id) DO UPDATE SET
        public_key_pem = COALESCE(excluded.public_key_pem, peers.public_key_pem),
        failure_bucket = COALESCE(excluded.failure_bucket, peers.failure_bucket),
        device_group = COALESCE(excluded.device_group, peers.device_group),
        host = COALESCE(excluded.host, peers.host),
        subnet = COALESCE(excluded.subnet, peers.subnet),
        last_seen_at = excluded.last_seen_at
    `).run(
      descriptor.peerId,
      descriptor.peerPublicKey ?? null,
      domain?.bucket ?? null,
      domain?.deviceGroup ?? null,
      domain?.host ?? null,
      domain?.subnet ?? null,
      seenAt,
      seenAt
    );

    this.db.prepare(`
      INSERT INTO peer_health (peer_id)
      VALUES (?)
      ON CONFLICT(peer_id) DO NOTHING
    `).run(descriptor.peerId);
  }

  private upsertPeerAccounting(peer: LocalPeerRecord, seenAt: string): void {
    const domain = peer.failureDomain;
    this.db.prepare(`
      INSERT INTO peers (
        peer_id, public_key_pem, capacity_bytes, reserved_bytes, repair_headroom_bytes,
        used_bytes, allocatable_bytes, regular_free_bytes, repair_free_bytes, online,
        failure_bucket, device_group, host, subnet, first_seen_at, last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(peer_id) DO UPDATE SET
        public_key_pem = excluded.public_key_pem,
        capacity_bytes = excluded.capacity_bytes,
        reserved_bytes = excluded.reserved_bytes,
        repair_headroom_bytes = excluded.repair_headroom_bytes,
        used_bytes = excluded.used_bytes,
        allocatable_bytes = excluded.allocatable_bytes,
        regular_free_bytes = excluded.regular_free_bytes,
        repair_free_bytes = excluded.repair_free_bytes,
        online = excluded.online,
        failure_bucket = excluded.failure_bucket,
        device_group = excluded.device_group,
        host = excluded.host,
        subnet = excluded.subnet,
        last_seen_at = excluded.last_seen_at
    `).run(
      peer.peerId,
      peer.publicKeyPem,
      peer.capacityBytes,
      peer.reservedBytes ?? null,
      peer.repairHeadroomBytes ?? null,
      peer.usedBytes ?? null,
      peer.allocatableBytes ?? null,
      peer.regularFreeBytes ?? null,
      peer.repairFreeBytes ?? null,
      peer.online ? 1 : 0,
      domain?.bucket ?? null,
      domain?.deviceGroup ?? null,
      domain?.host ?? null,
      domain?.subnet ?? null,
      seenAt,
      seenAt
    );

    this.db.prepare(`
      INSERT INTO peer_health (peer_id)
      VALUES (?)
      ON CONFLICT(peer_id) DO NOTHING
    `).run(peer.peerId);
  }

  private updateObjectMaintenance(
    report: RepairReport<StoredObjectManifest>,
    existing: TrackedObjectRecord | undefined,
    startedAt: Date,
    completedAt: Date,
    policy: SchedulerMaintenancePolicy
  ): void {
    const hasRepairFailures = report.failed.length > 0;
    const repaired = report.repaired.length > 0;
    const consecutiveDegradedRuns = hasRepairFailures
      ? (existing?.consecutiveDegradedRuns ?? 0) + 1
      : 0;
    const cooldownMs = hasRepairFailures
      ? Math.min(
          policy.degradedBackoffMaxMs,
          policy.degradedBackoffBaseMs * 2 ** Math.max(0, consecutiveDegradedRuns - 1)
        )
      : repaired
        ? policy.objectCooldownMs
        : 0;
    const nextEligibleAt =
      cooldownMs > 0 ? new Date(completedAt.getTime() + cooldownMs).toISOString() : null;
    const lastRepairAt = repaired ? completedAt.toISOString() : existing?.lastRepairAt ?? null;

    this.db.prepare(`
      UPDATE manifests
      SET consecutive_degraded_runs = ?,
        next_eligible_at = ?,
        last_scheduler_run_at = ?,
        last_repair_at = ?
      WHERE content_id = ?
    `).run(
      consecutiveDegradedRuns,
      nextEligibleAt,
      startedAt.toISOString(),
      lastRepairAt,
      report.updatedManifest.contentId
    );
  }

  private insertShardPlacement(contentId: string, descriptor: ShardDescriptor): void {
    const domain = descriptor.failureDomain;
    this.db.prepare(`
      INSERT INTO shard_placements (
        content_id, shard_index, peer_id, peer_public_key, failure_bucket,
        device_group, host, subnet, size, checksum, merkle_root,
        merkle_leaf_size, merkle_leaf_count
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      contentId,
      descriptor.index,
      descriptor.peerId,
      descriptor.peerPublicKey ?? null,
      domain?.bucket ?? null,
      domain?.deviceGroup ?? null,
      domain?.host ?? null,
      domain?.subnet ?? null,
      descriptor.size,
      descriptor.checksum,
      descriptor.merkleRoot ?? null,
      descriptor.merkleLeafSize ?? null,
      descriptor.merkleLeafCount ?? null
    );
  }

  private insertSchedulerRun(run: SchedulerRunRecord): void {
    this.db.prepare(`
      INSERT INTO scheduler_runs (
        run_id, started_at, completed_at, objects_audited, shards_audited,
        audits_passed, audits_failed, repairs_succeeded, repairs_failed, objects_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      run.runId,
      run.startedAt,
      run.completedAt,
      run.objectsAudited,
      run.shardsAudited,
      run.auditsPassed,
      run.auditsFailed,
      run.repairsSucceeded,
      run.repairsFailed,
      JSON.stringify(run.objects)
    );
  }

  private insertRepairEvents(run: SchedulerRunRecord): void {
    for (const object of run.objects) {
      for (const repair of object.repaired) {
        this.db.prepare(`
          INSERT INTO repair_events (
            event_id, run_id, content_id, shard_index, event_type, old_peer_id, new_peer_id, reason
          ) VALUES (?, ?, ?, ?, 'repaired', ?, ?, ?)
        `).run(
          randomUUID(),
          run.runId,
          object.contentId,
          repair.shardIndex,
          repair.oldPeerId,
          repair.newPeerId,
          repair.reason
        );
      }

      for (const failure of object.failedRepairs) {
        this.db.prepare(`
          INSERT INTO repair_events (
            event_id, run_id, content_id, shard_index, event_type, old_peer_id, new_peer_id, reason
          ) VALUES (?, ?, ?, ?, 'failed', ?, NULL, ?)
        `).run(
          randomUUID(),
          run.runId,
          object.contentId,
          failure.shardIndex,
          failure.peerId,
          failure.reason
        );
      }
    }
  }

  private updatePeerHealth(
    audits: readonly StorageAuditResult[],
    repairs: readonly ShardRepair[],
    timestamp: Date
  ): void {
    const auditedAt = timestamp.toISOString();

    for (const audit of audits) {
      this.ensurePeer(audit.peerId, auditedAt);
      if (audit.ok) {
        this.db.prepare(`
          UPDATE peer_health
          SET audits_passed = audits_passed + 1,
            consecutive_failures = 0,
            last_audit_at = ?,
            last_ok_at = ?,
            last_error = NULL
          WHERE peer_id = ?
        `).run(auditedAt, auditedAt, audit.peerId);
      } else {
        this.db.prepare(`
          UPDATE peer_health
          SET audits_failed = audits_failed + 1,
            consecutive_failures = consecutive_failures + 1,
            last_audit_at = ?,
            last_failure_at = ?,
            last_error = ?
          WHERE peer_id = ?
        `).run(auditedAt, auditedAt, audit.error ?? "Audit failed", audit.peerId);
      }
    }

    for (const repair of repairs) {
      this.ensurePeer(repair.newPeerId, auditedAt);
      this.db.prepare(`
        UPDATE peer_health
        SET repaired_shards = repaired_shards + 1
        WHERE peer_id = ?
      `).run(repair.newPeerId);
    }
  }

  private ensurePeer(peerId: string, seenAt: string): void {
    this.db.prepare(`
      INSERT INTO peers (peer_id, first_seen_at, last_seen_at)
      VALUES (?, ?, ?)
      ON CONFLICT(peer_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
    `).run(peerId, seenAt, seenAt);

    this.db.prepare(`
      INSERT INTO peer_health (peer_id)
      VALUES (?)
      ON CONFLICT(peer_id) DO NOTHING
    `).run(peerId);
  }

  private trimRunHistory(): void {
    this.db.prepare(`
      UPDATE state_transitions
      SET committed_run_id = NULL
      WHERE committed_run_id IN (
        SELECT run_id FROM scheduler_runs
        ORDER BY completed_at DESC, run_id DESC
        LIMIT -1 OFFSET ?
      )
    `).run(this.maxRunHistory);

    this.db.prepare(`
      DELETE FROM scheduler_runs
      WHERE run_id IN (
        SELECT run_id FROM scheduler_runs
        ORDER BY completed_at DESC, run_id DESC
        LIMIT -1 OFFSET ?
      )
    `).run(this.maxRunHistory);
  }

  private transaction<T>(fn: () => T): T {
    if (this.transactionDepth > 0) {
      return fn();
    }

    this.db.exec("BEGIN IMMEDIATE");
    this.transactionDepth += 1;
    try {
      const value = fn();
      this.db.exec("COMMIT");
      return value;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    } finally {
      this.transactionDepth -= 1;
    }
  }

  private throwInjectedCommitFault(): void {
    if (!this.commitFaultMessage) {
      return;
    }

    const message = this.commitFaultMessage;
    this.commitFaultMessage = undefined;
    throw new Error(message);
  }
}

export function emptyState(): KrydenStateSnapshot {
  return {
    version: 1,
    objects: {},
    peers: {},
    runs: [],
    transitions: {}
  };
}

export const DEFAULT_MAINTENANCE_POLICY: SchedulerMaintenancePolicy = {
  objectCooldownMs: 30_000,
  degradedBackoffBaseMs: 60_000,
  degradedBackoffMaxMs: 15 * 60_000
};

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

function trackedObjectFromRow(row: ManifestRow): TrackedObjectRecord {
  return removeUndefined({
    contentId: row.content_id,
    manifest: cloneManifest(JSON.parse(row.manifest_json) as StoredObjectManifest),
    registeredAt: row.registered_at,
    updatedAt: row.updated_at,
    consecutiveDegradedRuns: row.consecutive_degraded_runs,
    nextEligibleAt: row.next_eligible_at ?? undefined,
    lastSchedulerRunAt: row.last_scheduler_run_at ?? undefined,
    lastRepairAt: row.last_repair_at ?? undefined
  });
}

function peerHealthFromRow(row: PeerHealthRow): PeerHealthRecord {
  return removeUndefined({
    peerId: row.peer_id,
    auditsPassed: row.audits_passed,
    auditsFailed: row.audits_failed,
    consecutiveFailures: row.consecutive_failures,
    repairedShards: row.repaired_shards,
    lastAuditAt: row.last_audit_at ?? undefined,
    lastOkAt: row.last_ok_at ?? undefined,
    lastFailureAt: row.last_failure_at ?? undefined,
    lastError: row.last_error ?? undefined
  });
}

function schedulerRunFromRow(row: SchedulerRunRow): SchedulerRunRecord {
  return {
    runId: row.run_id,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    objectsAudited: row.objects_audited,
    shardsAudited: row.shards_audited,
    auditsPassed: row.audits_passed,
    auditsFailed: row.audits_failed,
    repairsSucceeded: row.repairs_succeeded,
    repairsFailed: row.repairs_failed,
    objects: JSON.parse(row.objects_json) as SchedulerObjectRunRecord[]
  };
}

function transitionFromRow(row: StateTransitionRow): StateTransitionRecord {
  return removeUndefined({
    transitionId: row.transition_id,
    status: row.status,
    startedAt: row.started_at,
    completedAt: row.completed_at ?? undefined,
    objectIds: JSON.parse(row.object_ids_json) as string[],
    committedRunId: row.committed_run_id ?? undefined,
    error: row.error ?? undefined
  });
}

function cloneManifest(manifest: StoredObjectManifest): StoredObjectManifest {
  return {
    ...manifest,
    encryption: { ...manifest.encryption },
    erasure: { ...manifest.erasure },
    shards: manifest.shards.map((descriptor) => ({
      ...descriptor,
      failureDomain: descriptor.failureDomain ? cloneFailureDomain(descriptor.failureDomain) : undefined
    }))
  };
}

function cloneFailureDomain(domain: FailureDomain): FailureDomain {
  return {
    ...domain
  };
}

function removeUndefined<T extends object>(value: T): T {
  return Object.fromEntries(
    Object.entries(value).filter(([, entryValue]) => entryValue !== undefined)
  ) as T;
}
