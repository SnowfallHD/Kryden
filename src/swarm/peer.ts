import type { EncodedShard } from "../erasure/reedSolomon.js";
import type { FailureDomain, ShardDescriptor } from "../storage/manifest.js";
import { createStorageAuditProof, type StorageAuditChallenge, type StorageAuditProof } from "./audit.js";
import { createPeerIdentity, type PeerIdentity } from "./identity.js";

export const PEER_RECORD_VERSION = 1;

export interface StoredShard {
  objectId: string;
  shard: EncodedShard;
}

export interface LocalPeerRecord {
  version: typeof PEER_RECORD_VERSION;
  peerId: string;
  capacityBytes: number;
  reservedBytes?: number;
  repairHeadroomBytes?: number;
  usedBytes?: number;
  allocatableBytes?: number;
  regularFreeBytes?: number;
  repairFreeBytes?: number;
  failureDomain?: FailureDomain;
  publicKeyPem: string;
  privateKeyPem: string;
  online: boolean;
}

export interface PeerStoreOptions {
  reservedBytes?: number;
  repairHeadroomBytes?: number;
  failureDomain?: FailureDomain;
}

export type StorePurpose = "regular" | "repair";

export class PeerStore {
  readonly id: string;
  readonly capacityBytes: number;
  readonly reservedBytes: number;
  readonly repairHeadroomBytes: number;
  readonly failureDomain: FailureDomain;
  readonly identity: PeerIdentity;
  online = true;

  private readonly shards = new Map<string, StoredShard>();

  constructor(
    id: string,
    capacityBytes: number,
    identity: PeerIdentity = createPeerIdentity(id),
    options: PeerStoreOptions = {}
  ) {
    if (!id) {
      throw new Error("Peer id is required");
    }

    if (!Number.isInteger(capacityBytes) || capacityBytes <= 0) {
      throw new Error("Peer capacity must be a positive integer");
    }

    const reservedBytes = options.reservedBytes ?? 0;
    const repairHeadroomBytes = options.repairHeadroomBytes ?? Math.floor(capacityBytes * 0.1);
    validateCapacityPartition(capacityBytes, reservedBytes, repairHeadroomBytes);

    this.id = id;
    this.capacityBytes = capacityBytes;
    this.reservedBytes = reservedBytes;
    this.repairHeadroomBytes = repairHeadroomBytes;
    this.failureDomain = options.failureDomain ?? { bucket: id };
    this.identity = identity;
  }

  static fromRecord(record: LocalPeerRecord): PeerStore {
    assertSupportedPeerRecord(record);
    const peer = new PeerStore(record.peerId, record.capacityBytes, {
      peerId: record.peerId,
      publicKeyPem: record.publicKeyPem,
      privateKeyPem: record.privateKeyPem
    }, {
      reservedBytes: record.reservedBytes,
      repairHeadroomBytes: record.repairHeadroomBytes,
      failureDomain: record.failureDomain
    });
    peer.online = record.online;
    return peer;
  }

  get publicKeyPem(): string {
    return this.identity.publicKeyPem;
  }

  get usedBytes(): number {
    let total = 0;
    for (const stored of this.shards.values()) {
      total += stored.shard.data.length;
    }
    return total;
  }

  get freeBytes(): number {
    return Math.max(0, this.capacityBytes - this.reservedBytes - this.usedBytes);
  }

  get allocatableBytes(): number {
    return Math.max(0, this.capacityBytes - this.reservedBytes - this.repairHeadroomBytes);
  }

  get regularFreeBytes(): number {
    return Math.max(0, this.allocatableBytes - this.usedBytes);
  }

  get repairFreeBytes(): number {
    return this.freeBytes;
  }

  get loadRatio(): number {
    return this.usedBytes / Math.max(1, this.capacityBytes - this.reservedBytes);
  }

  canStore(size: number, purpose: StorePurpose = "regular"): boolean {
    if (!Number.isInteger(size) || size < 0) {
      throw new Error("Store size must be a non-negative integer");
    }

    const limit = purpose === "repair" ? this.capacityBytes - this.reservedBytes : this.allocatableBytes;
    return this.online && this.usedBytes + size <= limit;
  }

  store(objectId: string, shard: EncodedShard, purpose: StorePurpose = "regular"): void {
    if (!this.canStore(shard.data.length, purpose)) {
      throw new Error(`Peer ${this.id} does not have enough free capacity`);
    }

    this.shards.set(shardKey(objectId, shard.index), {
      objectId,
      shard: {
        index: shard.index,
        checksum: shard.checksum,
        data: Buffer.from(shard.data)
      }
    });
  }

  retrieve(objectId: string, shardIndex: number): EncodedShard | undefined {
    if (!this.online) {
      return undefined;
    }

    const stored = this.shards.get(shardKey(objectId, shardIndex));
    if (!stored) {
      return undefined;
    }

    return {
      index: stored.shard.index,
      checksum: stored.shard.checksum,
      data: Buffer.from(stored.shard.data)
    };
  }

  corruptShard(objectId: string, shardIndex: number): void {
    const stored = this.shards.get(shardKey(objectId, shardIndex));
    if (!stored) {
      throw new Error(`Peer ${this.id} does not store shard ${shardIndex} for ${objectId}`);
    }

    if (stored.shard.data.length === 0) {
      throw new Error(`Shard ${shardIndex} for ${objectId} is empty`);
    }

    for (let index = 0; index < stored.shard.data.length; index += 1) {
      stored.shard.data[index] = stored.shard.data[index] ^ 0xff;
    }
  }

  respondToAudit(challenge: StorageAuditChallenge, descriptor: ShardDescriptor): StorageAuditProof | undefined {
    if (!this.online) {
      return undefined;
    }

    const stored = this.shards.get(shardKey(challenge.objectId, challenge.shardIndex));
    if (!stored) {
      return undefined;
    }

    return createStorageAuditProof(challenge, descriptor, stored.shard.data, this.identity);
  }

  toRecord(): LocalPeerRecord {
    return {
      version: PEER_RECORD_VERSION,
      peerId: this.id,
      capacityBytes: this.capacityBytes,
      reservedBytes: this.reservedBytes,
      repairHeadroomBytes: this.repairHeadroomBytes,
      usedBytes: this.usedBytes,
      allocatableBytes: this.allocatableBytes,
      regularFreeBytes: this.regularFreeBytes,
      repairFreeBytes: this.repairFreeBytes,
      failureDomain: { ...this.failureDomain },
      publicKeyPem: this.identity.publicKeyPem,
      privateKeyPem: this.identity.privateKeyPem,
      online: this.online
    };
  }
}

export function assertSupportedPeerRecord(record: LocalPeerRecord): void {
  if (record.version !== PEER_RECORD_VERSION) {
    throw new Error(`Unsupported peer record version ${String(record.version)}`);
  }
}

function shardKey(objectId: string, shardIndex: number): string {
  return `${objectId}:${shardIndex}`;
}

function validateCapacityPartition(
  capacityBytes: number,
  reservedBytes: number,
  repairHeadroomBytes: number
): void {
  if (!Number.isInteger(reservedBytes) || reservedBytes < 0) {
    throw new Error("reservedBytes must be a non-negative integer");
  }

  if (!Number.isInteger(repairHeadroomBytes) || repairHeadroomBytes < 0) {
    throw new Error("repairHeadroomBytes must be a non-negative integer");
  }

  if (reservedBytes + repairHeadroomBytes >= capacityBytes) {
    throw new Error("reservedBytes plus repairHeadroomBytes must be smaller than capacityBytes");
  }
}
