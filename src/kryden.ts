import { decryptPayload, encryptPayload } from "./crypto/envelope.js";
import { decodeErasure, encodeErasure, type ErasureConfig } from "./erasure/reedSolomon.js";
import type { ClientSecret, StoredObjectManifest } from "./storage/manifest.js";
import type { StorageAuditResult } from "./swarm/audit.js";
import type { LocalSwarm } from "./swarm/localSwarm.js";
import type { RepairReport } from "./swarm/repair.js";

export interface KrydenPutResult {
  manifest: StoredObjectManifest;
  secret: ClientSecret;
}

export class KrydenClient {
  private readonly swarm: LocalSwarm;

  constructor(swarm: LocalSwarm) {
    this.swarm = swarm;
  }

  put(plaintext: Buffer, config: ErasureConfig = { dataShards: 6, parityShards: 3 }): KrydenPutResult {
    const encrypted = encryptPayload(plaintext);
    const encoded = encodeErasure(encrypted.ciphertext, config);
    const contentId = `kd1_${encrypted.envelope.ciphertextSha256}`;
    const descriptors = this.swarm.storeObjectShards(contentId, encoded.shards);

    return {
      manifest: {
        version: 1,
        contentId,
        createdAt: new Date().toISOString(),
        encryption: encrypted.envelope,
        erasure: encoded.metadata,
        shards: descriptors
      },
      secret: encrypted.secret
    };
  }

  get(manifest: StoredObjectManifest, secret: ClientSecret): Buffer {
    const shards = this.swarm.fetchObjectShards(manifest);
    const ciphertext = decodeErasure(shards, manifest.erasure);
    return decryptPayload(ciphertext, manifest.encryption, secret);
  }

  audit(manifest: StoredObjectManifest, sampleCount = 3): StorageAuditResult[] {
    return this.swarm.auditObject(manifest, sampleCount);
  }

  repair(manifest: StoredObjectManifest, sampleCount = 3): RepairReport<StoredObjectManifest> {
    return this.swarm.repairObject(manifest, sampleCount);
  }
}

export type { ClientSecret, StoredObjectManifest } from "./storage/manifest.js";
export { createLocalSwarm, createLocalSwarmFromRecords, LocalSwarm } from "./swarm/localSwarm.js";
export type { LocalSwarmOptions } from "./swarm/localSwarm.js";
export { createPeerIdentity } from "./swarm/identity.js";
export { createStorageAuditChallenge, verifyStorageAuditProof } from "./swarm/audit.js";
export type { LocalPeerRecord } from "./swarm/peer.js";
export type { RepairReport, ShardRepair, ShardRepairFailure } from "./swarm/repair.js";
export { decodeErasure, encodeErasure } from "./erasure/reedSolomon.js";
export { decryptPayload, encryptPayload } from "./crypto/envelope.js";
