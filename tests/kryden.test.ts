import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { KrydenClient, createLocalSwarm } from "../src/kryden.js";

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
  });
});

