import { decryptPayload, encryptPayload } from "./crypto/envelope.js";
import { decodeErasure, encodeErasure, type ErasureConfig } from "./erasure/reedSolomon.js";
import { MANIFEST_VERSION, assertSupportedManifest, type ClientSecret, type StoredObjectManifest } from "./storage/manifest.js";
import type { StorageAuditResult } from "./swarm/audit.js";
import type { LocalSwarm } from "./swarm/localSwarm.js";
import type { RepairOptions, RepairReport } from "./swarm/repair.js";

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

  get(manifest: StoredObjectManifest, secret: ClientSecret): Buffer {
    assertSupportedManifest(manifest);
    const shards = this.swarm.fetchObjectShards(manifest);
    const ciphertext = decodeErasure(shards, manifest.erasure);
    return decryptPayload(ciphertext, manifest.encryption, secret);
  }

  audit(manifest: StoredObjectManifest, sampleCount = 3): StorageAuditResult[] {
    assertSupportedManifest(manifest);
    return this.swarm.auditObject(manifest, sampleCount);
  }

  repair(
    manifest: StoredObjectManifest,
    sampleCount = 3,
    options: RepairOptions = {}
  ): RepairReport<StoredObjectManifest> {
    assertSupportedManifest(manifest);
    return this.swarm.repairObject(manifest, sampleCount, options);
  }
}

export { MANIFEST_VERSION } from "./storage/manifest.js";
export type { ClientSecret, StoredObjectManifest } from "./storage/manifest.js";
export { createLocalSwarm, createLocalSwarmFromRecords, LocalSwarm } from "./swarm/localSwarm.js";
export type { LocalSwarmOptions } from "./swarm/localSwarm.js";
export { RemoteKrydenClient, RemotePeerClient, RemoteSwarm } from "./swarm/remoteSwarm.js";
export type { RemotePeerClientOptions } from "./swarm/remoteSwarm.js";
export { REMOTE_PEER_RECORD_VERSION, startPeerRuntimeServer } from "./swarm/peerRuntime.js";
export type {
  PeerRuntimeOptions,
  PeerRuntimeServer,
  PeerRuntimeTlsOptions,
  RemotePeerRecord
} from "./swarm/peerRuntime.js";
export {
  DEFAULT_HEARTBEAT_TTL_MS,
  HEARTBEAT_PROTOCOL,
  PeerMembershipRegistry,
  createSignedPeerHeartbeat,
  verifyPeerHeartbeat
} from "./swarm/membership.js";
export type { MembershipBootstrapResult, MembershipEntry, PeerHeartbeat } from "./swarm/membership.js";
export { createPeerIdentity } from "./swarm/identity.js";
export type { PeerIdentity } from "./swarm/identity.js";
export {
  DEFAULT_PEER_REQUEST_TTL_MS,
  PEER_REQUEST_PROTOCOL,
  PeerRequestReplayProtector,
  createSignedPeerRequest,
  verifySignedPeerRequest
} from "./swarm/requestAuth.js";
export type {
  PeerRequestAuthority,
  PeerRequestAuthorityRole,
  PeerRequestOperation,
  SignedPeerRequestEnvelope
} from "./swarm/requestAuth.js";
export { createStorageAuditChallenge, verifyStorageAuditProof } from "./swarm/audit.js";
export { DURABLE_SHARD_RECORD_VERSION, PEER_RECORD_VERSION } from "./swarm/peer.js";
export type { LocalPeerRecord } from "./swarm/peer.js";
export type { RepairOptions, RepairReport, ShardRepair, ShardRepairFailure } from "./swarm/repair.js";
export { decodeErasure, encodeErasure } from "./erasure/reedSolomon.js";
export { decryptPayload, encryptPayload } from "./crypto/envelope.js";
export { SQLITE_STATE_SCHEMA_VERSION } from "./state/schema.js";
