import { createHash } from "node:crypto";

import type { PeerStore, StorePurpose } from "./peer.js";

export interface PlacementOptions {
  excludedPeerIds?: ReadonlySet<string>;
  avoidedFailureDomains?: ReadonlySet<string>;
  purpose?: StorePurpose;
}

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

  return peers
    .filter((peer) => !excludedPeerIds.has(peer.id) && peer.canStore(shardSize, purpose))
    .map((peer) => ({
      peer,
      score: placementScore(objectId, shardIndex, peer, avoidedFailureDomains)
    }))
    .sort((left, right) => left.score - right.score)
    .map((entry) => entry.peer);
}

function placementScore(
  objectId: string,
  shardIndex: number,
  peer: PeerStore,
  avoidedFailureDomains: ReadonlySet<string>
): number {
  const digest = createHash("sha256")
    .update(objectId)
    .update(":")
    .update(String(shardIndex))
    .update(":")
    .update(peer.id)
    .digest();
  const hashScore = digest.readUIntBE(0, 6) / 0xffffffffffff;
  const capacityBias = peer.loadRatio * 0.15;
  const domainPenalty = avoidedFailureDomains.has(peer.failureDomain.bucket) ? 1 : 0;
  return hashScore + capacityBias + domainPenalty;
}
