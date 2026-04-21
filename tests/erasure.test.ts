import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { decodeErasure, encodeErasure } from "../src/erasure/reedSolomon.js";

describe("Reed-Solomon erasure coding", () => {
  it("recovers data after losing parity-budget shards", () => {
    const original = randomBytes(8192);
    const encoded = encodeErasure(original, { dataShards: 4, parityShards: 2 });
    const remaining = encoded.shards.filter((shard) => shard.index !== 0 && shard.index !== 3);

    const recovered = decodeErasure(remaining, encoded.metadata);

    expect(recovered.equals(original)).toBe(true);
  });

  it("recovers from every valid shard combination", () => {
    const original = randomBytes(257);
    const encoded = encodeErasure(original, { dataShards: 3, parityShards: 2 });

    for (const indices of combinations([0, 1, 2, 3, 4], 3)) {
      const shards = encoded.shards.filter((shard) => indices.includes(shard.index));
      const recovered = decodeErasure(shards, encoded.metadata);
      expect(recovered.equals(original)).toBe(true);
    }
  });

  it("rejects tampered shards", () => {
    const original = randomBytes(1024);
    const encoded = encodeErasure(original, { dataShards: 4, parityShards: 2 });
    encoded.shards[1].data[0] ^= 0xff;

    expect(() => decodeErasure(encoded.shards, encoded.metadata)).toThrow(/checksum/i);
  });
});

function combinations(values: number[], size: number): number[][] {
  if (size === 0) {
    return [[]];
  }

  if (values.length < size) {
    return [];
  }

  const [first, ...rest] = values;
  return [
    ...combinations(rest, size - 1).map((tail) => [first, ...tail]),
    ...combinations(rest, size)
  ];
}

