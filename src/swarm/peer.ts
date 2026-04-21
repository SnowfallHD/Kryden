import { randomBytes } from "node:crypto";
import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  rmSync,
  fsyncSync,
  statSync,
  unlinkSync,
  writeFileSync
} from "node:fs";
import { dirname, join } from "node:path";

import type { EncodedShard } from "../erasure/reedSolomon.js";
import type { FailureDomain, ShardDescriptor } from "../storage/manifest.js";
import { createStorageAuditProof, type StorageAuditChallenge, type StorageAuditProof } from "./audit.js";
import { createPeerIdentity, type PeerIdentity } from "./identity.js";

export const PEER_RECORD_VERSION = 1;
export const DURABLE_SHARD_RECORD_VERSION = 1;

export interface StoredShard {
  objectId: string;
  shard: EncodedShard;
  generation?: string;
  storedAt?: string;
  path?: string;
}

export interface DurableShardRecord {
  version: typeof DURABLE_SHARD_RECORD_VERSION;
  objectId: string;
  storedAt: string;
  generation: string;
  shard: {
    index: number;
    checksum: string;
    data: string;
  };
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
  storageDir?: string;
}

export type StorePurpose = "regular" | "repair";

export class PeerStore {
  readonly id: string;
  readonly capacityBytes: number;
  readonly reservedBytes: number;
  readonly repairHeadroomBytes: number;
  readonly failureDomain: FailureDomain;
  readonly identity: PeerIdentity;
  readonly storageDir?: string;
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
    this.storageDir = options.storageDir;
    if (this.storageDir) {
      this.rebuildDurableShardIndex();
    }
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

    return this.canStoreReplacing(size, purpose, 0);
  }

  store(objectId: string, shard: EncodedShard, purpose: StorePurpose = "regular"): void {
    const key = shardKey(objectId, shard.index);
    const previous = this.shards.get(key);
    if (!this.canStoreReplacing(shard.data.length, purpose, previous?.shard.data.length ?? 0)) {
      throw new Error(`Peer ${this.id} does not have enough free capacity`);
    }

    const stored: StoredShard = {
      objectId,
      shard: {
        index: shard.index,
        checksum: shard.checksum,
        data: Buffer.from(shard.data)
      }
    };

    if (this.storageDir) {
      const durable = this.writeDurableShard(stored);
      this.shards.set(key, durable);
      this.garbageCollectShardGenerations(objectId, shard.index, durable.path);
      return;
    }

    this.shards.set(key, stored);
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

    if (this.storageDir) {
      const durable = this.writeDurableShard(stored);
      this.shards.set(shardKey(objectId, shardIndex), durable);
      this.garbageCollectShardGenerations(objectId, shardIndex, durable.path);
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

  rebuildDurableShardIndex(): void {
    if (!this.storageDir) {
      return;
    }

    const root = durableShardsRoot(this.storageDir);
    mkdirSync(root, { recursive: true });
    const discovered = new Map<string, Array<StoredShard & { path: string; discoveredAtMs: number }>>();

    for (const filePath of walkFiles(root)) {
      if (isTemporaryShardFile(filePath)) {
        unlinkIfExists(filePath);
        continue;
      }

      if (!filePath.endsWith(".json")) {
        continue;
      }

      try {
        const record = parseDurableShardRecord(readFileSync(filePath, "utf8"));
        const stored = durableRecordToStoredShard(record, filePath);
        const key = shardKey(record.objectId, record.shard.index);
        const entries = discovered.get(key) ?? [];
        entries.push({
          ...stored,
          path: filePath,
          discoveredAtMs: durableRecordTime(record, filePath)
        });
        discovered.set(key, entries);
      } catch {
        unlinkIfExists(filePath);
      }
    }

    this.shards.clear();
    for (const [key, entries] of discovered) {
      entries.sort((left, right) =>
        right.discoveredAtMs - left.discoveredAtMs ||
        String(right.generation).localeCompare(String(left.generation))
      );
      const [latest, ...obsolete] = entries;
      this.shards.set(key, {
        objectId: latest.objectId,
        shard: latest.shard,
        generation: latest.generation,
        storedAt: latest.storedAt,
        path: latest.path
      });
      for (const stale of obsolete) {
        unlinkIfExists(stale.path);
      }
    }

    pruneEmptyDirectories(root);
  }

  private canStoreReplacing(size: number, purpose: StorePurpose, replacedBytes: number): boolean {
    if (!Number.isInteger(size) || size < 0) {
      throw new Error("Store size must be a non-negative integer");
    }

    const limit = purpose === "repair" ? this.capacityBytes - this.reservedBytes : this.allocatableBytes;
    return this.online && this.usedBytes - replacedBytes + size <= limit;
  }

  private writeDurableShard(stored: StoredShard): StoredShard {
    if (!this.storageDir) {
      return stored;
    }

    const generation = createShardGeneration();
    const storedAt = new Date().toISOString();
    const finalPath = durableShardPath(this.storageDir, stored.objectId, stored.shard.index, generation);
    const tempPath = `${finalPath}.${process.pid}-${randomBytes(4).toString("hex")}.tmp`;
    const record: DurableShardRecord = {
      version: DURABLE_SHARD_RECORD_VERSION,
      objectId: stored.objectId,
      storedAt,
      generation,
      shard: {
        index: stored.shard.index,
        checksum: stored.shard.checksum,
        data: stored.shard.data.toString("base64")
      }
    };

    mkdirSync(dirname(finalPath), { recursive: true });
    writeFileAtomic(tempPath, finalPath, `${JSON.stringify(record)}\n`);
    return {
      objectId: stored.objectId,
      shard: {
        index: stored.shard.index,
        checksum: stored.shard.checksum,
        data: Buffer.from(stored.shard.data)
      },
      generation,
      storedAt,
      path: finalPath
    };
  }

  private garbageCollectShardGenerations(
    objectId: string,
    shardIndex: number,
    keepPath?: string
  ): void {
    if (!this.storageDir || !keepPath) {
      return;
    }

    const dir = durableShardDirectory(this.storageDir, objectId, shardIndex);
    if (!existsSync(dir)) {
      return;
    }

    for (const entry of readdirSync(dir)) {
      const filePath = join(dir, entry);
      if (filePath !== keepPath && entry.endsWith(".json")) {
        unlinkIfExists(filePath);
      }
    }
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

function durableShardsRoot(storageDir: string): string {
  return join(storageDir, "shards");
}

function durableShardDirectory(storageDir: string, objectId: string, shardIndex: number): string {
  return join(durableShardsRoot(storageDir), encodeShardPathPart(objectId), String(shardIndex));
}

function durableShardPath(
  storageDir: string,
  objectId: string,
  shardIndex: number,
  generation: string
): string {
  return join(durableShardDirectory(storageDir, objectId, shardIndex), `${generation}.json`);
}

function encodeShardPathPart(value: string): string {
  return Buffer.from(value).toString("base64url");
}

function createShardGeneration(): string {
  return `${Date.now().toString(36)}-${randomBytes(8).toString("hex")}`;
}

function writeFileAtomic(tempPath: string, finalPath: string, content: string): void {
  let fileDescriptor: number | undefined;
  try {
    fileDescriptor = openSync(tempPath, "wx");
    writeFileSync(fileDescriptor, content);
  } finally {
    if (fileDescriptor !== undefined) {
      try {
        fsyncSync(fileDescriptor);
      } finally {
        closeSync(fileDescriptor);
      }
    }
  }

  renameSync(tempPath, finalPath);
  fsyncDirectory(dirname(finalPath));
}

function parseDurableShardRecord(value: string): DurableShardRecord {
  const parsed = JSON.parse(value) as DurableShardRecord;
  if (
    parsed.version !== DURABLE_SHARD_RECORD_VERSION ||
    !parsed.objectId ||
    !parsed.generation ||
    !parsed.storedAt ||
    !parsed.shard ||
    !Number.isInteger(parsed.shard.index) ||
    parsed.shard.index < 0 ||
    typeof parsed.shard.checksum !== "string" ||
    typeof parsed.shard.data !== "string"
  ) {
    throw new Error("Invalid durable shard record");
  }

  return parsed;
}

function durableRecordToStoredShard(record: DurableShardRecord, path: string): StoredShard {
  return {
    objectId: record.objectId,
    shard: {
      index: record.shard.index,
      checksum: record.shard.checksum,
      data: Buffer.from(record.shard.data, "base64")
    },
    generation: record.generation,
    storedAt: record.storedAt,
    path
  };
}

function durableRecordTime(record: DurableShardRecord, path: string): number {
  const parsed = Date.parse(record.storedAt);
  if (Number.isFinite(parsed)) {
    return parsed;
  }

  return statSync(path).mtimeMs;
}

function walkFiles(root: string): string[] {
  const out: string[] = [];
  if (!existsSync(root)) {
    return out;
  }

  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      out.push(...walkFiles(path));
    } else if (entry.isFile()) {
      out.push(path);
    }
  }

  return out;
}

function isTemporaryShardFile(path: string): boolean {
  return path.endsWith(".tmp") || path.includes(".tmp-");
}

function pruneEmptyDirectories(root: string): void {
  if (!existsSync(root)) {
    return;
  }

  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const path = join(root, entry.name);
    pruneEmptyDirectories(path);
    if (readdirSync(path).length === 0) {
      rmSync(path, { recursive: true, force: true });
    }
  }
}

function unlinkIfExists(path: string): void {
  try {
    unlinkSync(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      throw error;
    }
  }
}

function fsyncDirectory(path: string): void {
  let directoryDescriptor: number | undefined;
  try {
    directoryDescriptor = openSync(path, "r");
    fsyncSync(directoryDescriptor);
  } catch {
    // Some filesystems do not allow directory fsync. The temp-file rename still
    // preserves atomic visibility for readers.
  } finally {
    if (directoryDescriptor !== undefined) {
      closeSync(directoryDescriptor);
    }
  }
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
