import { createHash } from "node:crypto";

import type { PeerStore, StorePurpose } from "./peer.js";

export interface PeerPlacementStats {
  auditsPassed: number;
  auditsFailed: number;
  consecutiveFailures: number;
  repairedShards: number;
}

export interface PeerAdmissionPolicy {
  maxConsecutiveFailures?: number;
  maxFailureRate?: number;
  minAuditsForFailureRate?: number;
}

export interface PeerScoreWeights {
  hash: number;
  failureDomain: number;
  capacityPressure: number;
  repairHeadroomPressure: number;
  auditFailureRate: number;
  consecutiveFailures: number;
  repairSuccessBonus: number;
}

export interface PlacementOptions {
  excludedPeerIds?: ReadonlySet<string>;
  avoidedFailureDomains?: ReadonlySet<string>;
  purpose?: StorePurpose;
  peerHealth?: ReadonlyMap<string, PeerPlacementStats>;
  admission?: PeerAdmissionPolicy;
  weights?: Partial<PeerScoreWeights>;
}

export const DEFAULT_ADMISSION_POLICY: Required<PeerAdmissionPolicy> = {
  maxConsecutiveFailures: 5,
  maxFailureRate: 0.85,
  minAuditsForFailureRate: 4
};

export const DEFAULT_SCORE_WEIGHTS: PeerScoreWeights = {
  hash: 0.05,
  failureDomain: 2,
  capacityPressure: 0.45,
  repairHeadroomPressure: 0.2,
  auditFailureRate: 1.5,
  consecutiveFailures: 0.35,
  repairSuccessBonus: 0.04
};

export function rankPeersForShard(
  objectId: string,
  shardIndex: number,
  peers: readonly PeerStore[],
  shardSize: number,
  options: PlacementOptions = {}
): PeerStore[] {
  const excludedPeerIds = options.excludedPeerIds ?? new Set<string>();
  const avoidedFailureDomains = options.avoidedFailureDomains ?? new Set<string>();
  const purpose = options.purpose ?? "regular";
  const admission = { ...DEFAULT_ADMISSION_POLICY, ...options.admission };
  const weights = { ...DEFAULT_SCORE_WEIGHTS, ...options.weights };

  return peers
    .filter(
      (peer) =>
        !excludedPeerIds.has(peer.id) &&
        admitsPeer(peer, shardSize, purpose, options.peerHealth?.get(peer.id), admission)
    )
    .map((peer) => ({
      peer,
      score: placementScore(
        objectId,
        shardIndex,
        peer,
        avoidedFailureDomains,
        purpose,
        options.peerHealth?.get(peer.id),
        weights
      )
    }))
    .sort((left, right) => left.score - right.score)
    .map((entry) => entry.peer);
}

function placementScore(
  objectId: string,
  shardIndex: number,
  peer: PeerStore,
  avoidedFailureDomains: ReadonlySet<string>,
  purpose: StorePurpose,
  health: PeerPlacementStats | undefined,
  weights: PeerScoreWeights
): number {
  const digest = createHash("sha256")
    .update(objectId)
    .update(":")
    .update(String(shardIndex))
    .update(":")
    .update(peer.id)
    .digest();
  const hashScore = digest.readUIntBE(0, 6) / 0xffffffffffff;
  const domainPenalty = avoidedFailureDomains.has(peer.failureDomain.bucket) ? 1 : 0;
  const capacityPressure = capacityPressureScore(peer, purpose);
  const repairHeadroomPressure = 1 - safeRatio(peer.repairFreeBytes, peer.capacityBytes - peer.reservedBytes);
  const failureRate = failureRateScore(health);
  const consecutiveFailures = Math.min(1, (health?.consecutiveFailures ?? 0) / 5);
  const repairSuccessBonus = Math.min(1, (health?.repairedShards ?? 0) / 10);

  return (
    hashScore * weights.hash +
    domainPenalty * weights.failureDomain +
    capacityPressure * weights.capacityPressure +
    repairHeadroomPressure * weights.repairHeadroomPressure +
    failureRate * weights.auditFailureRate +
    consecutiveFailures * weights.consecutiveFailures -
    repairSuccessBonus * weights.repairSuccessBonus
  );
}

function admitsPeer(
  peer: PeerStore,
  shardSize: number,
  purpose: StorePurpose,
  health: PeerPlacementStats | undefined,
  admission: Required<PeerAdmissionPolicy>
): boolean {
  if (!peer.canStore(shardSize, purpose)) {
    return false;
  }

  if (!health) {
    return true;
  }

  if (health.consecutiveFailures > admission.maxConsecutiveFailures) {
    return false;
  }

  const auditCount = health.auditsPassed + health.auditsFailed;
  if (
    auditCount >= admission.minAuditsForFailureRate &&
    health.auditsFailed / auditCount > admission.maxFailureRate
  ) {
    return false;
  }

  return true;
}

function capacityPressureScore(peer: PeerStore, purpose: StorePurpose): number {
  const denominator =
    purpose === "repair" ? peer.capacityBytes - peer.reservedBytes : peer.allocatableBytes;
  const free = purpose === "repair" ? peer.repairFreeBytes : peer.regularFreeBytes;
  return 1 - safeRatio(free, denominator);
}

function failureRateScore(health: PeerPlacementStats | undefined): number {
  if (!health) {
    return 0;
  }

  const auditCount = health.auditsPassed + health.auditsFailed;
  if (auditCount === 0) {
    return 0;
  }

  return health.auditsFailed / auditCount;
}

function safeRatio(numerator: number, denominator: number): number {
  if (denominator <= 0) {
    return 1;
  }

  return Math.max(0, Math.min(1, numerator / denominator));
}
