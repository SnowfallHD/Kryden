import { createHash } from "node:crypto";

import { mul, pow } from "./galois.js";
import { invertMatrix, multiplyMatrices, type Matrix } from "./matrix.js";
import type { ErasureMetadata } from "../storage/manifest.js";

export interface ErasureConfig {
  dataShards: number;
  parityShards: number;
}

export interface EncodedShard {
  index: number;
  data: Buffer;
  checksum: string;
}

export interface EncodedObject {
  metadata: ErasureMetadata;
  shards: EncodedShard[];
}

export function shardChecksum(data: Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

export function encodeErasure(data: Buffer, config: ErasureConfig): EncodedObject {
  validateConfig(config);
  const shardSize = Math.ceil(data.length / config.dataShards);
  const paddedSize = shardSize * config.dataShards;
  const padded = Buffer.alloc(paddedSize);
  data.copy(padded);

  const dataShards = Array.from({ length: config.dataShards }, (_, index) => {
    const start = index * shardSize;
    return padded.subarray(start, start + shardSize);
  });

  const generator = buildGeneratorMatrix(config);
  const shards = generator.map((row, index) => {
    const shard = Buffer.alloc(shardSize);
    for (let shardByte = 0; shardByte < shardSize; shardByte += 1) {
      let value = 0;
      for (let dataIndex = 0; dataIndex < config.dataShards; dataIndex += 1) {
        const coefficient = row[dataIndex];
        if (coefficient === 1) {
          value ^= dataShards[dataIndex][shardByte];
        } else if (coefficient !== 0) {
          value ^= mul(coefficient, dataShards[dataIndex][shardByte]);
        }
      }
      shard[shardByte] = value;
    }

    return {
      index,
      data: shard,
      checksum: shardChecksum(shard)
    };
  });

  return {
    metadata: {
      codec: "reed-solomon-gf256-v1",
      dataShards: config.dataShards,
      parityShards: config.parityShards,
      totalShards: config.dataShards + config.parityShards,
      originalSize: data.length,
      shardSize
    },
    shards
  };
}

export function decodeErasure(shards: readonly EncodedShard[], metadata: ErasureMetadata): Buffer {
  validateMetadata(metadata);

  const unique = new Map<number, EncodedShard>();
  for (const shard of shards) {
    if (shard.index < 0 || shard.index >= metadata.totalShards) {
      throw new Error(`Shard index ${shard.index} is outside manifest range`);
    }

    if (shard.data.length !== metadata.shardSize) {
      throw new Error(`Shard ${shard.index} has invalid size`);
    }

    const actualChecksum = shardChecksum(shard.data);
    if (actualChecksum !== shard.checksum) {
      throw new Error(`Shard ${shard.index} checksum mismatch`);
    }

    unique.set(shard.index, shard);
  }

  if (unique.size < metadata.dataShards) {
    throw new Error(
      `Need ${metadata.dataShards} shards to decode, only found ${unique.size}`
    );
  }

  const selected = [...unique.values()]
    .sort((a, b) => a.index - b.index)
    .slice(0, metadata.dataShards);
  const generator = buildGeneratorMatrix(metadata);
  const selectedRows = selected.map((shard) => generator[shard.index]);
  const decodeMatrix = invertMatrix(selectedRows);
  const recoveredDataShards = decodeMatrix.map((row) => {
    const shard = Buffer.alloc(metadata.shardSize);
    for (let shardByte = 0; shardByte < metadata.shardSize; shardByte += 1) {
      let value = 0;
      for (let selectedIndex = 0; selectedIndex < selected.length; selectedIndex += 1) {
        const coefficient = row[selectedIndex];
        if (coefficient === 1) {
          value ^= selected[selectedIndex].data[shardByte];
        } else if (coefficient !== 0) {
          value ^= mul(coefficient, selected[selectedIndex].data[shardByte]);
        }
      }
      shard[shardByte] = value;
    }
    return shard;
  });

  return Buffer.concat(recoveredDataShards).subarray(0, metadata.originalSize);
}

export function buildGeneratorMatrix(config: ErasureConfig): Matrix {
  validateConfig(config);
  const totalShards = config.dataShards + config.parityShards;
  const vandermonde = Array.from({ length: totalShards }, (_, rowIndex) => {
    const row = new Uint8Array(config.dataShards);
    const x = rowIndex + 1;
    for (let column = 0; column < config.dataShards; column += 1) {
      row[column] = pow(x, column);
    }
    return row;
  });

  const top = vandermonde.slice(0, config.dataShards);
  const topInverse = invertMatrix(top);
  return multiplyMatrices(vandermonde, topInverse);
}

function validateMetadata(metadata: ErasureMetadata): void {
  if (metadata.codec !== "reed-solomon-gf256-v1") {
    throw new Error(`Unsupported erasure codec: ${metadata.codec}`);
  }

  validateConfig(metadata);
  if (metadata.totalShards !== metadata.dataShards + metadata.parityShards) {
    throw new Error("Invalid erasure metadata total shard count");
  }

  if (!Number.isInteger(metadata.originalSize) || metadata.originalSize < 0) {
    throw new Error("Invalid original size");
  }

  if (!Number.isInteger(metadata.shardSize) || metadata.shardSize < 0) {
    throw new Error("Invalid shard size");
  }
}

function validateConfig(config: ErasureConfig): void {
  if (!Number.isInteger(config.dataShards) || config.dataShards <= 0) {
    throw new Error("dataShards must be a positive integer");
  }

  if (!Number.isInteger(config.parityShards) || config.parityShards <= 0) {
    throw new Error("parityShards must be a positive integer");
  }

  if (config.dataShards + config.parityShards > 255) {
    throw new Error("Reed-Solomon GF(256) supports at most 255 total shards");
  }
}

