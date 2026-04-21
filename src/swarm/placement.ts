import { createHash } from "node:crypto";

import type { PeerStore } from "./peer.js";

export function rankPeersForShard(
  objectId: string,
  shardIndex: number,
  peers: readonly PeerStore[],
  shardSize: number,
  excludedPeerIds: ReadonlySet<string> = new Set()
): PeerStore[] {
  return peers
    .filter((peer) => !excludedPeerIds.has(peer.id) && peer.canStore(shardSize))
    .map((peer) => ({
      peer,
      score: placementScore(objectId, shardIndex, peer)
    }))
    .sort((left, right) => left.score - right.score)
    .map((entry) => entry.peer);
}

function placementScore(objectId: string, shardIndex: number, peer: PeerStore): number {
  const digest = createHash("sha256")
    .update(objectId)
    .update(":")
    .update(String(shardIndex))
    .update(":")
    .update(peer.id)
    .digest();
  const hashScore = digest.readUIntBE(0, 6) / 0xffffffffffff;
  const capacityBias = peer.loadRatio * 0.15;
  return hashScore + capacityBias;
}

