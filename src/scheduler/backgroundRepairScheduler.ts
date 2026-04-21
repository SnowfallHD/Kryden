import type { StoredObjectManifest } from "../storage/manifest.js";
import type { LocalSwarm } from "../swarm/localSwarm.js";
import type { RepairReport } from "../swarm/repair.js";
import { JsonStateStore, type SchedulerRunRecord } from "../state/store.js";

export interface BackgroundRepairSchedulerOptions {
  intervalMs?: number;
  sampleCount?: number;
}

export interface SchedulerRunSummary {
  run: SchedulerRunRecord;
  reports: RepairReport<StoredObjectManifest>[];
}

export class BackgroundRepairScheduler {
  private readonly swarm: LocalSwarm;
  private readonly store: JsonStateStore;
  private readonly intervalMs: number;
  private readonly sampleCount: number;
  private timer: NodeJS.Timeout | undefined;
  private running = false;

  constructor(
    swarm: LocalSwarm,
    store: JsonStateStore,
    options: BackgroundRepairSchedulerOptions = {}
  ) {
    this.swarm = swarm;
    this.store = store;
    this.intervalMs = options.intervalMs ?? 60_000;
    this.sampleCount = options.sampleCount ?? 3;

    if (!Number.isInteger(this.intervalMs) || this.intervalMs <= 0) {
      throw new Error("Scheduler intervalMs must be a positive integer");
    }

    if (!Number.isInteger(this.sampleCount) || this.sampleCount <= 0) {
      throw new Error("Scheduler sampleCount must be a positive integer");
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
      const trackedObjects = await this.store.getTrackedObjects();
      const transition = await this.store.beginSchedulerRun(
        trackedObjects.map((object) => object.contentId),
        startedAt
      );
      transitionId = transition.transitionId;
      const reports = trackedObjects.map((object) =>
        this.swarm.repairObject(object.manifest, this.sampleCount)
      );
      const completedAt = new Date();
      const run = await this.store.commitSchedulerRun(
        transition.transitionId,
        reports,
        startedAt,
        completedAt
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
