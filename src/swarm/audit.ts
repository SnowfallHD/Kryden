import { createHash, randomBytes } from "node:crypto";

import { assertSupportedManifest, type ShardDescriptor, type StoredObjectManifest } from "../storage/manifest.js";
import { createMerkleProof, verifyMerkleProof, type MerkleBranchNode } from "../storage/merkle.js";
import { fromHex, toHex } from "../util/bytes.js";
import { signPeerMessage, verifyPeerSignature, type PeerIdentity } from "./identity.js";

export const AUDIT_PROTOCOL = "kryden-storage-audit-v1";

export interface StorageAuditChallenge {
  protocol: typeof AUDIT_PROTOCOL;
  objectId: string;
  shardIndex: number;
  nonce: string;
  leafIndices: number[];
}

export interface StorageAuditLeaf {
  leafIndex: number;
  data: string;
  branch: MerkleBranchNode[];
}

export interface StorageAuditProof {
  protocol: typeof AUDIT_PROTOCOL;
  objectId: string;
  shardIndex: number;
  peerId: string;
  peerPublicKey: string;
  nonce: string;
  leafSize: number;
  leafCount: number;
  shardSize: number;
  shardChecksum: string;
  merkleRoot: string;
  leaves: StorageAuditLeaf[];
  signature: string;
}

export interface StorageAuditResult {
  shardIndex: number;
  peerId: string;
  ok: boolean;
  error?: string;
  challenge?: StorageAuditChallenge;
  proof?: StorageAuditProof;
}

export function createStorageAuditChallenge(
  descriptor: ShardDescriptor,
  objectId: string,
  sampleCount = 3,
  nonce: Buffer = randomBytes(32)
): StorageAuditChallenge {
  requireAuditableDescriptor(descriptor);

  return {
    protocol: AUDIT_PROTOCOL,
    objectId,
    shardIndex: descriptor.index,
    nonce: toHex(nonce),
    leafIndices: deriveLeafIndices(objectId, descriptor.index, toHex(nonce), descriptor.merkleLeafCount, sampleCount)
  };
}

export function createStorageAuditProof(
  challenge: StorageAuditChallenge,
  descriptor: ShardDescriptor,
  shardData: Buffer,
  identity: PeerIdentity
): StorageAuditProof {
  requireAuditableDescriptor(descriptor);

  if (challenge.protocol !== AUDIT_PROTOCOL) {
    throw new Error("Unsupported audit challenge protocol");
  }

  if (challenge.shardIndex !== descriptor.index) {
    throw new Error("Audit challenge shard index does not match descriptor");
  }

  if (identity.peerId !== descriptor.peerId) {
    throw new Error("Peer identity does not match descriptor");
  }

  if (identity.publicKeyPem !== descriptor.peerPublicKey) {
    throw new Error("Peer public key does not match descriptor");
  }

  const leaves = challenge.leafIndices.map((leafIndex) => {
    const proof = createMerkleProof(shardData, leafIndex, descriptor.merkleLeafSize);
    return {
      leafIndex,
      data: proof.leafData.toString("hex"),
      branch: proof.branch
    };
  });

  const unsigned: Omit<StorageAuditProof, "signature"> = {
    protocol: AUDIT_PROTOCOL,
    objectId: challenge.objectId,
    shardIndex: descriptor.index,
    peerId: descriptor.peerId,
    peerPublicKey: descriptor.peerPublicKey,
    nonce: challenge.nonce,
    leafSize: descriptor.merkleLeafSize,
    leafCount: descriptor.merkleLeafCount,
    shardSize: descriptor.size,
    shardChecksum: descriptor.checksum,
    merkleRoot: descriptor.merkleRoot,
    leaves
  };

  return {
    ...unsigned,
    signature: signPeerMessage(identity, auditTranscript(unsigned))
  };
}

export function verifyStorageAuditProof(
  manifest: StoredObjectManifest,
  descriptor: ShardDescriptor,
  challenge: StorageAuditChallenge,
  proof: StorageAuditProof
): boolean {
  assertSupportedManifest(manifest);
  requireAuditableDescriptor(descriptor);

  if (challenge.protocol !== AUDIT_PROTOCOL || proof.protocol !== AUDIT_PROTOCOL) {
    return false;
  }

  if (
    challenge.objectId !== manifest.contentId ||
    challenge.objectId !== proof.objectId ||
    challenge.shardIndex !== descriptor.index ||
    challenge.shardIndex !== proof.shardIndex ||
    proof.peerId !== descriptor.peerId ||
    proof.peerPublicKey !== descriptor.peerPublicKey ||
    proof.nonce !== challenge.nonce ||
    proof.leafSize !== descriptor.merkleLeafSize ||
    proof.leafCount !== descriptor.merkleLeafCount ||
    proof.shardSize !== descriptor.size ||
    proof.shardChecksum !== descriptor.checksum ||
    proof.merkleRoot !== descriptor.merkleRoot
  ) {
    return false;
  }

  const expectedIndices = new Set(challenge.leafIndices);
  if (proof.leaves.length !== expectedIndices.size) {
    return false;
  }

  for (const leaf of proof.leaves) {
    if (!expectedIndices.has(leaf.leafIndex)) {
      return false;
    }

    if (
      !verifyMerkleProof(
        {
          leafIndex: leaf.leafIndex,
          leafData: fromHex(leaf.data, "audit leaf data"),
          branch: leaf.branch
        },
        descriptor.merkleRoot,
        descriptor.merkleLeafCount,
        descriptor.merkleLeafSize
      )
    ) {
      return false;
    }

    expectedIndices.delete(leaf.leafIndex);
  }

  if (expectedIndices.size !== 0) {
    return false;
  }

  const { signature, ...unsigned } = proof;
  return verifyPeerSignature(proof.peerPublicKey, auditTranscript(unsigned), signature);
}

export function auditTranscript(proof: Omit<StorageAuditProof, "signature">): Buffer {
  return Buffer.from(JSON.stringify(proof));
}

function deriveLeafIndices(
  objectId: string,
  shardIndex: number,
  nonce: string,
  leafCount: number,
  sampleCount: number
): number[] {
  if (!Number.isInteger(sampleCount) || sampleCount <= 0) {
    throw new Error("Audit sample count must be a positive integer");
  }

  const target = Math.min(sampleCount, leafCount);
  const indices = new Set<number>();
  let round = 0;

  while (indices.size < target) {
    const digest = createHash("sha256")
      .update("kryden-audit-leaf-v1")
      .update(objectId)
      .update(":")
      .update(String(shardIndex))
      .update(":")
      .update(nonce)
      .update(":")
      .update(String(round))
      .digest();
    indices.add(digest.readUInt32BE(0) % leafCount);
    round += 1;
  }

  return [...indices].sort((a, b) => a - b);
}

function requireAuditableDescriptor(descriptor: ShardDescriptor): asserts descriptor is ShardDescriptor & {
  peerPublicKey: string;
  merkleRoot: string;
  merkleLeafSize: number;
  merkleLeafCount: number;
} {
  if (!descriptor.peerPublicKey) {
    throw new Error(`Shard ${descriptor.index} is missing peer public key`);
  }

  if (!descriptor.merkleRoot || !descriptor.merkleLeafSize || !descriptor.merkleLeafCount) {
    throw new Error(`Shard ${descriptor.index} is missing Merkle audit metadata`);
  }
}
