import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { MANIFEST_VERSION, KrydenClient, createLocalSwarm } from "../src/kryden.js";

describe("Kryden local swarm", () => {
  it("stores and restores through peer loss", () => {
    const plaintext = randomBytes(64 * 1024);
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(plaintext, { dataShards: 4, parityShards: 2 });

    for (const descriptor of stored.manifest.shards.slice(0, 2)) {
      swarm.setPeerOnline(descriptor.peerId, false);
    }

    const recovered = client.get(stored.manifest, stored.secret);

    expect(recovered.equals(plaintext)).toBe(true);
    expect(stored.manifest.version).toBe(MANIFEST_VERSION);
  });

  it("rejects unsupported manifest versions", () => {
    const swarm = createLocalSwarm(8, 1024 * 1024);
    const client = new KrydenClient(swarm);
    const stored = client.put(randomBytes(4096), { dataShards: 4, parityShards: 2 });

    expect(() =>
      client.get({
        ...stored.manifest,
        version: 999 as typeof MANIFEST_VERSION
      }, stored.secret)
    ).toThrow(/Unsupported manifest version/i);
  });
});
