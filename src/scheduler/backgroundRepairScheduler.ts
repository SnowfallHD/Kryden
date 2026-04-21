import type { StoredObjectManifest } from "../storage/manifest.js";
import type { LocalSwarm } from "../swarm/localSwarm.js";
import type { RepairReport } from "../swarm/repair.js";
import {
  DEFAULT_MAINTENANCE_POLICY,
  SQLiteStateStore,
  type SchedulerMaintenancePolicy,
  type SchedulerRunRecord
} from "../state/store.js";

export interface BackgroundRepairSchedulerOptions {
  intervalMs?: number;
  sampleCount?: number;
  recoverInterruptedTransitions?: boolean;
  maxRepairsPerRun?: number;
  objectCooldownMs?: number;
  degradedBackoffBaseMs?: number;
  degradedBackoffMaxMs?: number;
}

export interface SchedulerRunSummary {
  run: SchedulerRunRecord;
  reports: RepairReport<StoredObjectManifest>[];
}

export class BackgroundRepairScheduler {
  private readonly swarm: LocalSwarm;
  private readonly store: SQLiteStateStore;
  private readonly intervalMs: number;
  private readonly sampleCount: number;
  private readonly recoverInterruptedTransitions: boolean;
  private readonly maxRepairsPerRun: number;
  private readonly maintenancePolicy: SchedulerMaintenancePolicy;
  private timer: NodeJS.Timeout | undefined;
  private running = false;

  constructor(
    swarm: LocalSwarm,
    store: SQLiteStateStore,
    options: BackgroundRepairSchedulerOptions = {}
  ) {
    this.swarm = swarm;
    this.store = store;
    this.intervalMs = options.intervalMs ?? 60_000;
    this.sampleCount = options.sampleCount ?? 3;
    this.recoverInterruptedTransitions = options.recoverInterruptedTransitions ?? true;
    this.maxRepairsPerRun = options.maxRepairsPerRun ?? Number.POSITIVE_INFINITY;
    this.maintenancePolicy = {
      objectCooldownMs: options.objectCooldownMs ?? DEFAULT_MAINTENANCE_POLICY.objectCooldownMs,
      degradedBackoffBaseMs:
        options.degradedBackoffBaseMs ?? DEFAULT_MAINTENANCE_POLICY.degradedBackoffBaseMs,
      degradedBackoffMaxMs:
        options.degradedBackoffMaxMs ?? DEFAULT_MAINTENANCE_POLICY.degradedBackoffMaxMs
    };

    if (!Number.isInteger(this.intervalMs) || this.intervalMs <= 0) {
      throw new Error("Scheduler intervalMs must be a positive integer");
    }

    if (!Number.isInteger(this.sampleCount) || this.sampleCount <= 0) {
      throw new Error("Scheduler sampleCount must be a positive integer");
    }

    if (
      this.maxRepairsPerRun !== Number.POSITIVE_INFINITY &&
      (!Number.isInteger(this.maxRepairsPerRun) || this.maxRepairsPerRun < 0)
    ) {
      throw new Error("Scheduler maxRepairsPerRun must be a non-negative integer");
    }

    for (const [name, value] of Object.entries(this.maintenancePolicy)) {
      if (!Number.isInteger(value) || value < 0) {
        throw new Error(`Scheduler ${name} must be a non-negative integer`);
      }
    }
  }

  async trackObject(manifest: StoredObjectManifest): Promise<void> {
    await this.store.trackObject(manifest);
  }

  async runOnce(now = new Date()): Promise<SchedulerRunSummary> {
    if (this.running) {
      throw new Error("Scheduler run already in progress");
    }

    this.running = true;
    const startedAt = now;
    let transitionId: string | undefined;
    try {
      if (this.recoverInterruptedTransitions) {
        await this.store.recoverInterruptedTransitions(
          "Recovered interrupted scheduler transition",
          startedAt
        );
      }
      const state = await this.store.load();
      this.swarm.setPeerPlacementHealth(state.peers);
      const trackedObjects = await this.store.getEligibleTrackedObjects(startedAt);
      const transition = await this.store.beginSchedulerRun(
        trackedObjects.map((object) => object.contentId),
        startedAt
      );
      transitionId = transition.transitionId;
      const reports: RepairReport<StoredObjectManifest>[] = [];
      let remainingRepairs = this.maxRepairsPerRun;
      for (const object of trackedObjects) {
        const maxRepairs =
          remainingRepairs === Number.POSITIVE_INFINITY
            ? Number.POSITIVE_INFINITY
            : Math.max(0, remainingRepairs);
        const report = this.swarm.repairObject(object.manifest, this.sampleCount, { maxRepairs });
        reports.push(report);
        if (remainingRepairs !== Number.POSITIVE_INFINITY) {
          remainingRepairs = Math.max(0, remainingRepairs - report.repaired.length);
        }
      }
      const completedAt = new Date(startedAt);
      const run = await this.store.commitSchedulerRun(
        transition.transitionId,
        reports,
        startedAt,
        completedAt,
        this.swarm.toPeerRecords(),
        this.maintenancePolicy
      );
      return {
        run,
        reports
      };
    } catch (error) {
      if (transitionId) {
        await this.store.abandonTransition(
          transitionId,
          error instanceof Error ? error.message : "Unknown scheduler error"
        );
      }
      throw error;
    } finally {
      this.running = false;
    }
  }

  start(): void {
    if (this.timer) {
      return;
    }

    this.timer = setInterval(() => {
      this.runOnce().catch(() => {
        // The next interval can try again; callers can inspect state for successful runs.
      });
    }, this.intervalMs);
  }

  stop(): void {
    if (!this.timer) {
      return;
    }

    clearInterval(this.timer);
    this.timer = undefined;
  }
}
