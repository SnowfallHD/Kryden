import type { EncodedShard } from "../erasure/reedSolomon.js";
import { decodeErasure, encodeErasure } from "../erasure/reedSolomon.js";
import type { ShardDescriptor, StoredObjectManifest } from "../storage/manifest.js";
import { buildMerkleTree, DEFAULT_MERKLE_LEAF_SIZE } from "../storage/merkle.js";
import {
  createStorageAuditChallenge,
  verifyStorageAuditProof,
  type StorageAuditResult
} from "./audit.js";
import { PeerStore } from "./peer.js";
import type { LocalPeerRecord } from "./peer.js";
import { rankPeersForShard, type PeerPlacementStats } from "./placement.js";
import type { RepairOptions, RepairReport, ShardRepair, ShardRepairFailure } from "./repair.js";

export interface LocalSwarmOptions {
  failureDomainCount?: number;
  reservedBytes?: number;
  repairHeadroomBytes?: number;
}

export class LocalSwarm {
  readonly peers: PeerStore[];
  private readonly peerPlacementHealth = new Map<string, PeerPlacementStats>();

  constructor(peers: readonly PeerStore[]) {
    if (peers.length === 0) {
      throw new Error("LocalSwarm requires at least one peer");
    }

    const ids = new Set(peers.map((peer) => peer.id));
    if (ids.size !== peers.length) {
      throw new Error("Peer ids must be unique");
    }

    this.peers = [...peers];
  }

  setPeerPlacementHealth(
    health: ReadonlyMap<string, PeerPlacementStats> | Record<string, PeerPlacementStats>
  ): void {
    this.peerPlacementHealth.clear();
    const entries = health instanceof Map ? health.entries() : Object.entries(health);
    for (const [peerId, stats] of entries) {
      this.peerPlacementHealth.set(peerId, {
        auditsPassed: stats.auditsPassed,
        auditsFailed: stats.auditsFailed,
        consecutiveFailures: stats.consecutiveFailures,
        repairedShards: stats.repairedShards
      });
    }
  }

  storeObjectShards(objectId: string, shards: readonly EncodedShard[]): ShardDescriptor[] {
    const descriptors: ShardDescriptor[] = [];
    const usedPeerIds = new Set<string>();
    const usedFailureDomains = new Set<string>();

    for (const shard of shards) {
      let candidates = rankPeersForShard(
        objectId,
        shard.index,
        this.peers,
        shard.data.length,
        {
          excludedPeerIds: usedPeerIds,
          avoidedFailureDomains: usedFailureDomains,
          purpose: "regular",
          peerHealth: this.peerPlacementHealth
        }
      );

      if (candidates.length === 0) {
        candidates = rankPeersForShard(objectId, shard.index, this.peers, shard.data.length, {
          avoidedFailureDomains: usedFailureDomains,
          purpose: "regular",
          peerHealth: this.peerPlacementHealth
        });
      }

      const peer = candidates[0];
      if (!peer) {
        throw new Error(`No peer can store shard ${shard.index}`);
      }

      const descriptor = this.storeShardOnPeer(objectId, shard, peer, "regular");
      usedPeerIds.add(peer.id);
      usedFailureDomains.add(peer.failureDomain.bucket);
      descriptors.push(descriptor);
    }

    return descriptors;
  }

  fetchObjectShards(manifest: StoredObjectManifest): EncodedShard[] {
    const out: EncodedShard[] = [];
    for (const descriptor of manifest.shards) {
      const peer = this.peers.find((candidate) => candidate.id === descriptor.peerId);
      const shard = peer?.retrieve(manifest.contentId, descriptor.index);
      if (!shard) {
        continue;
      }

      out.push({
        index: descriptor.index,
        data: shard.data,
        checksum: descriptor.checksum
      });
    }

    return out;
  }

  setPeerOnline(peerId: string, online: boolean): void {
    const peer = this.peers.find((candidate) => candidate.id === peerId);
    if (!peer) {
      throw new Error(`Unknown peer ${peerId}`);
    }

    peer.online = online;
  }

  corruptShard(peerId: string, objectId: string, shardIndex: number): void {
    const peer = this.peers.find((candidate) => candidate.id === peerId);
    if (!peer) {
      throw new Error(`Unknown peer ${peerId}`);
    }

    peer.corruptShard(objectId, shardIndex);
  }

  offlinePeerIds(): string[] {
    return this.peers.filter((peer) => !peer.online).map((peer) => peer.id);
  }

  auditObject(manifest: StoredObjectManifest, sampleCount = 3): StorageAuditResult[] {
    return manifest.shards.map((descriptor) => {
      const peer = this.peers.find((candidate) => candidate.id === descriptor.peerId);
      if (!peer) {
        return {
          shardIndex: descriptor.index,
          peerId: descriptor.peerId,
          ok: false,
          error: "Peer not found"
        };
      }

      try {
        const challenge = createStorageAuditChallenge(descriptor, manifest.contentId, sampleCount);
        const proof = peer.respondToAudit(challenge, descriptor);
        if (!proof) {
          return {
            shardIndex: descriptor.index,
            peerId: descriptor.peerId,
            ok: false,
            error: "Peer did not return a proof",
            challenge
          };
        }

        const ok = verifyStorageAuditProof(manifest, descriptor, challenge, proof);
        return {
          shardIndex: descriptor.index,
          peerId: descriptor.peerId,
          ok,
          error: ok ? undefined : "Invalid storage proof",
          challenge,
          proof
        };
      } catch (error) {
        return {
          shardIndex: descriptor.index,
          peerId: descriptor.peerId,
          ok: false,
          error: error instanceof Error ? error.message : "Unknown audit error"
        };
      }
    });
  }

  repairObject(
    manifest: StoredObjectManifest,
    sampleCount = 3,
    options: RepairOptions = {}
  ): RepairReport<StoredObjectManifest> {
    const maxRepairs = options.maxRepairs ?? Number.POSITIVE_INFINITY;
    if (
      maxRepairs !== Number.POSITIVE_INFINITY &&
      (!Number.isInteger(maxRepairs) || maxRepairs < 0)
    ) {
      throw new Error("maxRepairs must be a non-negative integer");
    }

    const audits = this.auditObject(manifest, sampleCount);
    const failedAudits = audits.filter((audit) => !audit.ok);
    const healthyShards = audits.length - failedAudits.length;

    if (failedAudits.length === 0) {
      return {
        updatedManifest: cloneManifest(manifest),
        audits,
        repaired: [],
        failed: [],
        healthyShards,
        requiredShards: manifest.erasure.dataShards
      };
    }

    const failedShardIndexes = new Set(failedAudits.map((audit) => audit.shardIndex));
    const availableShards = this.fetchObjectShards(manifest).filter(
      (shard) => !failedShardIndexes.has(shard.index)
    );
    if (availableShards.length < manifest.erasure.dataShards) {
      return {
        updatedManifest: cloneManifest(manifest),
        audits,
        repaired: [],
        failed: failedAudits.map((audit) => ({
          shardIndex: audit.shardIndex,
          peerId: audit.peerId,
          reason: `Only ${availableShards.length} shards available; need ${manifest.erasure.dataShards}`
        })),
        healthyShards,
        requiredShards: manifest.erasure.dataShards
      };
    }

    const ciphertext = decodeErasure(availableShards, manifest.erasure);
    const encoded = encodeErasure(ciphertext, {
      dataShards: manifest.erasure.dataShards,
      parityShards: manifest.erasure.parityShards
    });
    const updatedManifest = cloneManifest(manifest);
    const repaired: ShardRepair[] = [];
    const failed: ShardRepairFailure[] = [];

    for (const audit of failedAudits) {
      if (repaired.length >= maxRepairs) {
        failed.push({
          shardIndex: audit.shardIndex,
          peerId: audit.peerId,
          reason: "Repair deferred by per-run cap"
        });
        continue;
      }

      const shard = encoded.shards.find((candidate) => candidate.index === audit.shardIndex);
      const descriptorIndex = updatedManifest.shards.findIndex(
        (candidate) => candidate.index === audit.shardIndex
      );

      if (!shard || descriptorIndex === -1) {
        failed.push({
          shardIndex: audit.shardIndex,
          peerId: audit.peerId,
          reason: "Shard is missing from reconstructed object"
        });
        continue;
      }

      const oldDescriptor = updatedManifest.shards[descriptorIndex];
      const oldPeerId = oldDescriptor.peerId;
      const remainingDescriptors = updatedManifest.shards.filter((_, index) => index !== descriptorIndex);
      const avoidPeers = new Set(remainingDescriptors.map((descriptor) => descriptor.peerId));
      const avoidFailureDomains = new Set(
        remainingDescriptors
          .map((descriptor) => descriptor.failureDomain?.bucket)
          .filter((bucket): bucket is string => typeof bucket === "string")
      );
      avoidPeers.add(oldPeerId);
      const replacementPeer = this.chooseReplacementPeer(
        updatedManifest.contentId,
        shard,
        avoidPeers,
        avoidFailureDomains,
        oldPeerId
      );

      if (!replacementPeer) {
        failed.push({
          shardIndex: audit.shardIndex,
          peerId: oldPeerId,
          reason: "No online peer had enough free capacity for repair"
        });
        continue;
      }

      const newDescriptor = this.storeShardOnPeer(
        updatedManifest.contentId,
        shard,
        replacementPeer,
        "repair"
      );
      updatedManifest.shards[descriptorIndex] = newDescriptor;
      repaired.push({
        shardIndex: audit.shardIndex,
        oldPeerId,
        newPeerId: replacementPeer.id,
        reason: audit.error ?? "Audit failed"
      });
    }

    updatedManifest.shards.sort((left, right) => left.index - right.index);

    return {
      updatedManifest,
      audits,
      repaired,
      failed,
      healthyShards,
      requiredShards: manifest.erasure.dataShards
    };
  }

  toPeerRecords(): LocalPeerRecord[] {
    return this.peers.map((peer) => peer.toRecord());
  }

  private chooseReplacementPeer(
    objectId: string,
    shard: EncodedShard,
    preferredAvoidPeerIds: ReadonlySet<string>,
    preferredAvoidFailureDomains: ReadonlySet<string>,
    oldPeerId: string
  ): PeerStore | undefined {
    const preferred = rankPeersForShard(
      objectId,
      shard.index,
      this.peers,
      shard.data.length,
      {
        excludedPeerIds: preferredAvoidPeerIds,
        avoidedFailureDomains: preferredAvoidFailureDomains,
        purpose: "repair",
        peerHealth: this.peerPlacementHealth
      }
    )[0];

    if (preferred) {
      return preferred;
    }

    return rankPeersForShard(
      objectId,
      shard.index,
      this.peers,
      shard.data.length,
      {
        excludedPeerIds: new Set([oldPeerId]),
        avoidedFailureDomains: preferredAvoidFailureDomains,
        purpose: "repair",
        peerHealth: this.peerPlacementHealth
      }
    )[0];
  }

  private storeShardOnPeer(
    objectId: string,
    shard: EncodedShard,
    peer: PeerStore,
    purpose: "regular" | "repair"
  ): ShardDescriptor {
    peer.store(objectId, shard, purpose);
    const tree = buildMerkleTree(shard.data, DEFAULT_MERKLE_LEAF_SIZE);
    return {
      index: shard.index,
      peerId: peer.id,
      peerPublicKey: peer.publicKeyPem,
      failureDomain: { ...peer.failureDomain },
      size: shard.data.length,
      checksum: shard.checksum,
      merkleRoot: tree.root,
      merkleLeafSize: tree.leafSize,
      merkleLeafCount: tree.leafCount
    };
  }
}

export function createLocalSwarm(
  peerCount: number,
  capacityBytes: number,
  options: LocalSwarmOptions = {}
): LocalSwarm {
  if (!Number.isInteger(peerCount) || peerCount <= 0) {
    throw new Error("peerCount must be a positive integer");
  }

  const failureDomainCount = options.failureDomainCount ?? peerCount;
  if (!Number.isInteger(failureDomainCount) || failureDomainCount <= 0) {
    throw new Error("failureDomainCount must be a positive integer");
  }

  return new LocalSwarm(
    Array.from(
      { length: peerCount },
      (_, index) =>
        new PeerStore(`peer-${index + 1}`, capacityBytes, undefined, {
          reservedBytes: options.reservedBytes,
          repairHeadroomBytes: options.repairHeadroomBytes,
          failureDomain: {
            bucket: `bucket-${(index % failureDomainCount) + 1}`,
            host: `host-${index + 1}`
          }
        })
    )
  );
}

export function createLocalSwarmFromRecords(records: readonly LocalPeerRecord[]): LocalSwarm {
  if (records.length === 0) {
    throw new Error("At least one peer record is required");
  }

  return new LocalSwarm(records.map((record) => PeerStore.fromRecord(record)));
}

function cloneManifest(manifest: StoredObjectManifest): StoredObjectManifest {
  return {
    ...manifest,
    encryption: { ...manifest.encryption },
    erasure: { ...manifest.erasure },
    shards: manifest.shards.map((descriptor) => ({ ...descriptor }))
  };
}
