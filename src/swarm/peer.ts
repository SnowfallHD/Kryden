import type { EncodedShard } from "../erasure/reedSolomon.js";
import type { ShardDescriptor } from "../storage/manifest.js";
import { createStorageAuditProof, type StorageAuditChallenge, type StorageAuditProof } from "./audit.js";
import { createPeerIdentity, type PeerIdentity } from "./identity.js";

export interface StoredShard {
  objectId: string;
  shard: EncodedShard;
}

export interface LocalPeerRecord {
  peerId: string;
  capacityBytes: number;
  publicKeyPem: string;
  privateKeyPem: string;
  online: boolean;
}

export class PeerStore {
  readonly id: string;
  readonly capacityBytes: number;
  readonly identity: PeerIdentity;
  online = true;

  private readonly shards = new Map<string, StoredShard>();

  constructor(id: string, capacityBytes: number, identity: PeerIdentity = createPeerIdentity(id)) {
    if (!id) {
      throw new Error("Peer id is required");
    }

    if (!Number.isInteger(capacityBytes) || capacityBytes <= 0) {
      throw new Error("Peer capacity must be a positive integer");
    }

    this.id = id;
    this.capacityBytes = capacityBytes;
    this.identity = identity;
  }

  static fromRecord(record: LocalPeerRecord): PeerStore {
    const peer = new PeerStore(record.peerId, record.capacityBytes, {
      peerId: record.peerId,
      publicKeyPem: record.publicKeyPem,
      privateKeyPem: record.privateKeyPem
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
    return this.capacityBytes - this.usedBytes;
  }

  get loadRatio(): number {
    return this.usedBytes / this.capacityBytes;
  }

  canStore(size: number): boolean {
    return this.online && this.freeBytes >= size;
  }

  store(objectId: string, shard: EncodedShard): void {
    if (!this.canStore(shard.data.length)) {
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
      peerId: this.id,
      capacityBytes: this.capacityBytes,
      publicKeyPem: this.identity.publicKeyPem,
      privateKeyPem: this.identity.privateKeyPem,
      online: this.online
    };
  }
}

function shardKey(objectId: string, shardIndex: number): string {
  return `${objectId}:${shardIndex}`;
}
