import type { StorageAuditResult } from "./audit.js";

export interface ShardRepair {
  shardIndex: number;
  oldPeerId: string;
  newPeerId: string;
  reason: string;
}

export interface ShardRepairFailure {
  shardIndex: number;
  peerId: string;
  reason: string;
}

export interface RepairReport<TManifest> {
  updatedManifest: TManifest;
  audits: StorageAuditResult[];
  repaired: ShardRepair[];
  failed: ShardRepairFailure[];
  healthyShards: number;
  requiredShards: number;
}

