import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { buildMerkleTree, createMerkleProof, verifyMerkleProof } from "../src/storage/merkle.js";

describe("Merkle shard commitments", () => {
  it("verifies sampled leaves against a shard root", () => {
    const data = randomBytes(8192);
    const tree = buildMerkleTree(data, 512);
    const proof = createMerkleProof(data, 7, 512);

    expect(verifyMerkleProof(proof, tree.root, tree.leafCount, tree.leafSize)).toBe(true);
  });

  it("rejects tampered sampled leaves", () => {
    const data = randomBytes(4096);
    const tree = buildMerkleTree(data, 512);
    const proof = createMerkleProof(data, 2, 512);
    proof.leafData[0] ^= 0xff;

    expect(verifyMerkleProof(proof, tree.root, tree.leafCount, tree.leafSize)).toBe(false);
  });
});

