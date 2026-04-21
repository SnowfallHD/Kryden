import { createHash } from "node:crypto";

import { fromHex, toHex } from "../util/bytes.js";

export const DEFAULT_MERKLE_LEAF_SIZE = 1024;

export interface MerkleBranchNode {
  position: "left" | "right";
  hash: string;
}

export interface MerkleLeafProof {
  leafIndex: number;
  leafData: Buffer;
  branch: MerkleBranchNode[];
}

export interface MerkleTree {
  leafSize: number;
  leafCount: number;
  root: string;
  levels: Buffer[][];
}

export function buildMerkleTree(data: Buffer, leafSize = DEFAULT_MERKLE_LEAF_SIZE): MerkleTree {
  if (!Number.isInteger(leafSize) || leafSize <= 0) {
    throw new Error("Merkle leaf size must be a positive integer");
  }

  const leaves = splitLeaves(data, leafSize).map((leaf, index) => hashLeaf(index, leaf));
  const levels: Buffer[][] = [leaves];

  while (levels[levels.length - 1].length > 1) {
    const previous = levels[levels.length - 1];
    const next: Buffer[] = [];

    for (let index = 0; index < previous.length; index += 2) {
      const left = previous[index];
      const right = previous[index + 1] ?? left;
      next.push(hashNode(left, right));
    }

    levels.push(next);
  }

  return {
    leafSize,
    leafCount: leaves.length,
    root: toHex(levels[levels.length - 1][0]),
    levels
  };
}

export function createMerkleProof(data: Buffer, leafIndex: number, leafSize = DEFAULT_MERKLE_LEAF_SIZE): MerkleLeafProof {
  const tree = buildMerkleTree(data, leafSize);
  if (!Number.isInteger(leafIndex) || leafIndex < 0 || leafIndex >= tree.leafCount) {
    throw new Error("Merkle leaf index is out of range");
  }

  const leaves = splitLeaves(data, leafSize);
  const branch: MerkleBranchNode[] = [];
  let cursor = leafIndex;

  for (let levelIndex = 0; levelIndex < tree.levels.length - 1; levelIndex += 1) {
    const level = tree.levels[levelIndex];
    const isRight = cursor % 2 === 1;
    const siblingIndex = isRight ? cursor - 1 : cursor + 1;
    const sibling = level[siblingIndex] ?? level[cursor];
    branch.push({
      position: isRight ? "left" : "right",
      hash: toHex(sibling)
    });
    cursor = Math.floor(cursor / 2);
  }

  return {
    leafIndex,
    leafData: leaves[leafIndex],
    branch
  };
}

export function verifyMerkleProof(
  proof: MerkleLeafProof,
  root: string,
  expectedLeafCount: number,
  leafSize = DEFAULT_MERKLE_LEAF_SIZE
): boolean {
  if (!Number.isInteger(expectedLeafCount) || expectedLeafCount <= 0) {
    return false;
  }

  if (!Number.isInteger(proof.leafIndex) || proof.leafIndex < 0 || proof.leafIndex >= expectedLeafCount) {
    return false;
  }

  if (proof.leafData.length > leafSize) {
    return false;
  }

  let current = hashLeaf(proof.leafIndex, proof.leafData);
  for (const node of proof.branch) {
    const sibling = fromHex(node.hash, "Merkle branch hash");
    current = node.position === "left" ? hashNode(sibling, current) : hashNode(current, sibling);
  }

  return toHex(current) === root;
}

export function leafCountForSize(size: number, leafSize = DEFAULT_MERKLE_LEAF_SIZE): number {
  if (!Number.isInteger(size) || size < 0) {
    throw new Error("Merkle data size must be a non-negative integer");
  }

  if (!Number.isInteger(leafSize) || leafSize <= 0) {
    throw new Error("Merkle leaf size must be a positive integer");
  }

  return Math.max(1, Math.ceil(size / leafSize));
}

function splitLeaves(data: Buffer, leafSize: number): Buffer[] {
  const leafCount = leafCountForSize(data.length, leafSize);
  return Array.from({ length: leafCount }, (_, index) => {
    const start = index * leafSize;
    return Buffer.from(data.subarray(start, Math.min(start + leafSize, data.length)));
  });
}

function hashLeaf(index: number, data: Buffer): Buffer {
  return createHash("sha256")
    .update("kryden-merkle-leaf-v1")
    .update(Buffer.from(String(index)))
    .update(Buffer.from([0]))
    .update(data)
    .digest();
}

function hashNode(left: Buffer, right: Buffer): Buffer {
  return createHash("sha256")
    .update("kryden-merkle-node-v1")
    .update(left)
    .update(right)
    .digest();
}

