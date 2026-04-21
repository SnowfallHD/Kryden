export interface EncryptionEnvelope {
  algorithm: "AES-256-GCM";
  nonce: string;
  tag: string;
  plaintextSize: number;
  ciphertextSize: number;
  ciphertextSha256: string;
}

export interface ClientSecret {
  algorithm: "AES-256-GCM";
  key: string;
}

export interface ErasureMetadata {
  codec: "reed-solomon-gf256-v1";
  dataShards: number;
  parityShards: number;
  totalShards: number;
  originalSize: number;
  shardSize: number;
}

export interface ShardDescriptor {
  index: number;
  peerId: string;
  peerPublicKey?: string;
  size: number;
  checksum: string;
  merkleRoot?: string;
  merkleLeafSize?: number;
  merkleLeafCount?: number;
}

export interface StoredObjectManifest {
  version: 1;
  contentId: string;
  createdAt: string;
  encryption: EncryptionEnvelope;
  erasure: ErasureMetadata;
  shards: ShardDescriptor[];
}
