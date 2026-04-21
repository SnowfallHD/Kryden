import type { EncodedShard } from "../erasure/reedSolomon.js";
import { decodeErasure, encodeErasure, type ErasureConfig } from "../erasure/reedSolomon.js";
import { decryptPayload, encryptPayload } from "../crypto/envelope.js";
import {
  MANIFEST_VERSION,
  assertSupportedManifest,
  type ClientSecret,
  type ShardDescriptor,
  type StoredObjectManifest
} from "../storage/manifest.js";
import { buildMerkleTree, DEFAULT_MERKLE_LEAF_SIZE } from "../storage/merkle.js";
import {
  createStorageAuditChallenge,
  type StorageAuditProof,
  verifyStorageAuditProof,
  type StorageAuditChallenge,
  type StorageAuditResult
} from "./audit.js";
import { assertSupportedRemotePeerRecord, type RemotePeerRecord } from "./peerRuntime.js";
import type { PeerHeartbeat } from "./membership.js";
import { rankPeersForShard, type PeerPlacementStats, type PlacementPeer } from "./placement.js";
import type { RepairOptions, RepairReport, ShardRepair, ShardRepairFailure } from "./repair.js";
import type { StorePurpose } from "./peer.js";
import {
  DEFAULT_PEER_REQUEST_TTL_MS,
  createSignedPeerRequest,
  type PeerRequestAuthorityRole,
  type PeerRequestOperation
} from "./requestAuth.js";
import type { PeerIdentity } from "./identity.js";

export interface RemoteKrydenPutResult {
  manifest: StoredObjectManifest;
  secret: ClientSecret;
}

interface WireShard {
  index: number;
  checksum: string;
  data: string;
}

export interface RemotePeerClientOptions {
  authority?: PeerIdentity;
  authorityRole?: PeerRequestAuthorityRole;
  requestTtlMs?: number;
}

export class RemotePeerClient {
  readonly url: string;
  private readonly authority?: PeerIdentity;
  private readonly authorityRole: PeerRequestAuthorityRole;
  private readonly requestTtlMs: number;

  constructor(url: string, options: RemotePeerClientOptions = {}) {
    if (!url) {
      throw new Error("Remote peer URL is required");
    }

    this.url = url.replace(/\/$/, "");
    this.authority = options.authority;
    this.authorityRole = options.authorityRole ?? "coordinator";
    this.requestTtlMs = options.requestTtlMs ?? DEFAULT_PEER_REQUEST_TTL_MS;
  }

  async getRecord(): Promise<RemotePeerRecord> {
    const response = await this.request<{ peer: RemotePeerRecord }>("/record", "GET");
    assertSupportedRemotePeerRecord(response.peer);
    return response.peer;
  }

  async getHeartbeat(): Promise<PeerHeartbeat> {
    const response = await this.request<{ heartbeat: PeerHeartbeat }>("/heartbeat", "GET");
    return response.heartbeat;
  }

  async store(objectId: string, shard: EncodedShard, purpose: StorePurpose = "regular"): Promise<RemotePeerRecord> {
    const response = await this.protectedRequest<{ peer: RemotePeerRecord }>("/store", purpose === "repair" ? "repair" : "store", {
      objectId,
      shard: shardToWire(shard),
      purpose
    });
    return response.peer;
  }

  async retrieve(objectId: string, shardIndex: number): Promise<EncodedShard | undefined> {
    const response = await this.protectedRequest<{ shard: WireShard | null }>("/retrieve", "retrieve", {
      objectId,
      shardIndex
    });
    return response.shard ? shardFromWire(response.shard) : undefined;
  }

  async respondToAudit(
    challenge: StorageAuditChallenge,
    descriptor: ShardDescriptor
  ): Promise<StorageAuditProof | undefined> {
    const response = await this.protectedRequest<{ proof: StorageAuditProof | null }>("/audit", "audit", {
      challenge,
      descriptor
    });
    return response.proof ?? undefined;
  }

  async setOnline(online: boolean): Promise<RemotePeerRecord> {
    const response = await this.protectedRequest<{ peer: RemotePeerRecord }>("/online", "admin", { online });
    return response.peer;
  }

  async corruptShard(objectId: string, shardIndex: number): Promise<RemotePeerRecord> {
    const response = await this.protectedRequest<{ peer: RemotePeerRecord }>("/corrupt", "admin", {
      objectId,
      shardIndex
    });
    return response.peer;
  }

  private async protectedRequest<T>(path: string, operation: PeerRequestOperation, body: unknown): Promise<T> {
    if (!this.authority) {
      throw new Error("Remote peer protected request requires a signing authority");
    }

    return this.request<T>(path, "POST", createSignedPeerRequest({
      authority: this.authority,
      authorityRole: this.authorityRole,
      operation,
      path,
      body,
      ttlMs: this.requestTtlMs
    }));
  }

  private async request<T>(path: string, method: "GET" | "POST", body?: unknown): Promise<T> {
    const response = await fetch(`${this.url}${path}`, {
      method,
      headers: body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body)
    });

    const text = await response.text();
    const parsed = text ? JSON.parse(text) as T & { error?: string } : {} as T & { error?: string };
    if (!response.ok) {
      throw new Error(parsed.error ?? `Remote peer request failed with HTTP ${response.status}`);
    }

    return parsed as T;
  }
}

export class RemoteSwarm {
  readonly peers: RemotePeerClient[];
  private readonly peerPlacementHealth = new Map<string, PeerPlacementStats>();
  private readonly peerClientsById = new Map<string, RemotePeerClient>();

  constructor(peers: readonly RemotePeerClient[]) {
    if (peers.length === 0) {
      throw new Error("RemoteSwarm requires at least one peer");
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

  async storeObjectShards(objectId: string, shards: readonly EncodedShard[]): Promise<ShardDescriptor[]> {
    const descriptors: ShardDescriptor[] = [];
    const usedPeerIds = new Set<string>();
    const usedFailureDomains = new Set<string>();
    const snapshots = await this.refreshSnapshots();

    for (const shard of shards) {
      let candidates = rankPeersForShard(
        objectId,
        shard.index,
        [...snapshots.values()],
        shard.data.length,
        {
          excludedPeerIds: usedPeerIds,
          avoidedFailureDomains: usedFailureDomains,
          purpose: "regular",
          peerHealth: this.peerPlacementHealth
        }
      );

      if (candidates.length === 0) {
        candidates = rankPeersForShard(objectId, shard.index, [...snapshots.values()], shard.data.length, {
          avoidedFailureDomains: usedFailureDomains,
          purpose: "regular",
          peerHealth: this.peerPlacementHealth
        });
      }

      const snapshot = candidates[0];
      if (!snapshot) {
        throw new Error(`No remote peer can store shard ${shard.index}`);
      }

      const client = this.peerClient(snapshot.id);
      const updatedRecord = await client.store(objectId, shard, "regular");
      const updatedSnapshot = new RemotePeerSnapshot(updatedRecord);
      snapshots.set(updatedSnapshot.id, updatedSnapshot);
      const descriptor = descriptorForRemoteShard(shard, updatedSnapshot);
      usedPeerIds.add(updatedSnapshot.id);
      usedFailureDomains.add(updatedSnapshot.failureDomain.bucket);
      descriptors.push(descriptor);
    }

    return descriptors;
  }

  async fetchObjectShards(manifest: StoredObjectManifest): Promise<EncodedShard[]> {
    assertSupportedManifest(manifest);
    const out: EncodedShard[] = [];
    for (const descriptor of manifest.shards) {
      const peer = await this.tryPeerClient(descriptor.peerId);
      if (!peer) {
        continue;
      }

      const shard = await peer.retrieve(manifest.contentId, descriptor.index).catch(() => undefined);
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

  async auditObject(manifest: StoredObjectManifest, sampleCount = 3): Promise<StorageAuditResult[]> {
    assertSupportedManifest(manifest);
    const results: StorageAuditResult[] = [];
    for (const descriptor of manifest.shards) {
      const peer = await this.tryPeerClient(descriptor.peerId);
      if (!peer) {
        results.push({
          shardIndex: descriptor.index,
          peerId: descriptor.peerId,
          ok: false,
          error: "Peer not found"
        });
        continue;
      }

      try {
        const challenge = createStorageAuditChallenge(descriptor, manifest.contentId, sampleCount);
        const proof = await peer.respondToAudit(challenge, descriptor);
        if (!proof) {
          results.push({
            shardIndex: descriptor.index,
            peerId: descriptor.peerId,
            ok: false,
            error: "Peer did not return a proof",
            challenge
          });
          continue;
        }

        const ok = verifyStorageAuditProof(manifest, descriptor, challenge, proof);
        results.push({
          shardIndex: descriptor.index,
          peerId: descriptor.peerId,
          ok,
          error: ok ? undefined : "Invalid storage proof",
          challenge,
          proof
        });
      } catch (error) {
        results.push({
          shardIndex: descriptor.index,
          peerId: descriptor.peerId,
          ok: false,
          error: error instanceof Error ? error.message : "Unknown audit error"
        });
      }
    }

    return results;
  }

  async repairObject(
    manifest: StoredObjectManifest,
    sampleCount = 3,
    options: RepairOptions = {}
  ): Promise<RepairReport<StoredObjectManifest>> {
    assertSupportedManifest(manifest);
    const maxRepairs = options.maxRepairs ?? Number.POSITIVE_INFINITY;
    if (
      maxRepairs !== Number.POSITIVE_INFINITY &&
      (!Number.isInteger(maxRepairs) || maxRepairs < 0)
    ) {
      throw new Error("maxRepairs must be a non-negative integer");
    }

    const audits = await this.auditObject(manifest, sampleCount);
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
    const availableShards = (await this.fetchObjectShards(manifest)).filter(
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
    const snapshots = await this.refreshSnapshots();

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
        snapshots,
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

      let updatedRecord: RemotePeerRecord;
      try {
        updatedRecord = await this.peerClient(replacementPeer.id).store(
          updatedManifest.contentId,
          shard,
          "repair"
        );
      } catch (error) {
        failed.push({
          shardIndex: audit.shardIndex,
          peerId: replacementPeer.id,
          reason: error instanceof Error ? error.message : "Repair write failed"
        });
        continue;
      }

      const updatedSnapshot = new RemotePeerSnapshot(updatedRecord);
      snapshots.set(updatedSnapshot.id, updatedSnapshot);
      updatedManifest.shards[descriptorIndex] = descriptorForRemoteShard(
        shard,
        updatedSnapshot
      );
      repaired.push({
        shardIndex: audit.shardIndex,
        oldPeerId,
        newPeerId: updatedSnapshot.id,
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

  private chooseReplacementPeer(
    objectId: string,
    shard: EncodedShard,
    snapshots: ReadonlyMap<string, RemotePeerSnapshot>,
    preferredAvoidPeerIds: ReadonlySet<string>,
    preferredAvoidFailureDomains: ReadonlySet<string>,
    oldPeerId: string
  ): RemotePeerSnapshot | undefined {
    const candidates = [...snapshots.values()];
    const preferred = rankPeersForShard(objectId, shard.index, candidates, shard.data.length, {
      excludedPeerIds: preferredAvoidPeerIds,
      avoidedFailureDomains: preferredAvoidFailureDomains,
      purpose: "repair",
      peerHealth: this.peerPlacementHealth
    })[0];

    if (preferred) {
      return preferred;
    }

    return rankPeersForShard(objectId, shard.index, candidates, shard.data.length, {
      excludedPeerIds: new Set([oldPeerId]),
      avoidedFailureDomains: preferredAvoidFailureDomains,
      purpose: "repair",
      peerHealth: this.peerPlacementHealth
    })[0];
  }

  private async refreshSnapshots(): Promise<Map<string, RemotePeerSnapshot>> {
    const entries = await Promise.all(
      this.peers.map(async (client) => {
        try {
          const snapshot = new RemotePeerSnapshot(await client.getRecord());
          this.peerClientsById.set(snapshot.id, client);
          return [snapshot.id, snapshot] as const;
        } catch {
          return undefined;
        }
      })
    );

    return new Map(entries.filter((entry): entry is readonly [string, RemotePeerSnapshot] => Boolean(entry)));
  }

  private peerClient(peerId: string): RemotePeerClient {
    const client = this.peerClientsById.get(peerId);
    if (client) {
      return client;
    }

    throw new Error(`Remote peer ${peerId} is not registered`);
  }

  private async tryPeerClient(peerId: string): Promise<RemotePeerClient | undefined> {
    for (const client of this.peers) {
      try {
        const record = await client.getRecord();
        this.peerClientsById.set(record.peerId, client);
        if (record.peerId === peerId) {
          return client;
        }
      } catch {
        continue;
      }
    }

    return undefined;
  }
}

export class RemoteKrydenClient {
  private readonly swarm: RemoteSwarm;

  constructor(swarm: RemoteSwarm) {
    this.swarm = swarm;
  }

  async put(
    plaintext: Buffer,
    config: ErasureConfig = { dataShards: 6, parityShards: 3 }
  ): Promise<RemoteKrydenPutResult> {
    const encrypted = encryptPayload(plaintext);
    const encoded = encodeErasure(encrypted.ciphertext, config);
    const contentId = `kd1_${encrypted.envelope.ciphertextSha256}`;
    const descriptors = await this.swarm.storeObjectShards(contentId, encoded.shards);

    return {
      manifest: {
        version: MANIFEST_VERSION,
        contentId,
        createdAt: new Date().toISOString(),
        encryption: encrypted.envelope,
        erasure: encoded.metadata,
        shards: descriptors
      },
      secret: encrypted.secret
    };
  }

  async get(manifest: StoredObjectManifest, secret: ClientSecret): Promise<Buffer> {
    assertSupportedManifest(manifest);
    const shards = await this.swarm.fetchObjectShards(manifest);
    const ciphertext = decodeErasure(shards, manifest.erasure);
    return decryptPayload(ciphertext, manifest.encryption, secret);
  }

  audit(manifest: StoredObjectManifest, sampleCount = 3): Promise<StorageAuditResult[]> {
    assertSupportedManifest(manifest);
    return this.swarm.auditObject(manifest, sampleCount);
  }

  repair(
    manifest: StoredObjectManifest,
    sampleCount = 3,
    options: RepairOptions = {}
  ): Promise<RepairReport<StoredObjectManifest>> {
    assertSupportedManifest(manifest);
    return this.swarm.repairObject(manifest, sampleCount, options);
  }
}

class RemotePeerSnapshot implements PlacementPeer {
  readonly id: string;
  readonly capacityBytes: number;
  readonly reservedBytes: number;
  readonly repairHeadroomBytes: number;
  readonly allocatableBytes: number;
  readonly regularFreeBytes: number;
  readonly repairFreeBytes: number;
  readonly failureDomain: RemotePeerRecord["failureDomain"];
  readonly publicKeyPem: string;
  private readonly usedBytes: number;
  private readonly online: boolean;

  constructor(record: RemotePeerRecord) {
    assertSupportedRemotePeerRecord(record);
    this.id = record.peerId;
    this.capacityBytes = record.capacityBytes;
    this.reservedBytes = record.reservedBytes;
    this.repairHeadroomBytes = record.repairHeadroomBytes;
    this.allocatableBytes = record.allocatableBytes;
    this.regularFreeBytes = record.regularFreeBytes;
    this.repairFreeBytes = record.repairFreeBytes;
    this.failureDomain = { ...record.failureDomain };
    this.publicKeyPem = record.publicKeyPem;
    this.usedBytes = record.usedBytes;
    this.online = record.online;
  }

  canStore(size: number, purpose: StorePurpose = "regular"): boolean {
    if (!Number.isInteger(size) || size < 0) {
      throw new Error("Store size must be a non-negative integer");
    }

    const limit = purpose === "repair"
      ? this.capacityBytes - this.reservedBytes
      : this.capacityBytes - this.reservedBytes - this.repairHeadroomBytes;
    return this.online && this.usedBytes + size <= limit;
  }
}

function descriptorForRemoteShard(
  shard: EncodedShard,
  peer: RemotePeerSnapshot
): ShardDescriptor {
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

function shardFromWire(shard: WireShard): EncodedShard {
  return {
    index: shard.index,
    checksum: shard.checksum,
    data: Buffer.from(shard.data, "base64")
  };
}

function shardToWire(shard: EncodedShard): WireShard {
  return {
    index: shard.index,
    checksum: shard.checksum,
    data: shard.data.toString("base64")
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
