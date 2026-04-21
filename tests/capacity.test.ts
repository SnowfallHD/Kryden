import { describe, expect, it } from "vitest";

import { PeerStore } from "../src/swarm/peer.js";

describe("peer capacity accounting", () => {
  it("separates allocatable bytes from repair headroom and reserved bytes", () => {
    const peer = new PeerStore("peer-a", 1000, undefined, {
      reservedBytes: 100,
      repairHeadroomBytes: 200,
      failureDomain: { bucket: "bucket-a" }
    });

    expect(peer.allocatableBytes).toBe(700);
    expect(peer.freeBytes).toBe(900);
    expect(peer.regularFreeBytes).toBe(700);
    expect(peer.repairFreeBytes).toBe(900);
    expect(peer.canStore(701, "regular")).toBe(false);
    expect(peer.canStore(701, "repair")).toBe(true);
  });

  it("prevents regular placement from consuming repair headroom", () => {
    const peer = new PeerStore("peer-a", 1000, undefined, {
      reservedBytes: 0,
      repairHeadroomBytes: 250
    });
    const shard = {
      index: 0,
      checksum: "unused",
      data: Buffer.alloc(760)
    };

    expect(() => peer.store("object-a", shard, "regular")).toThrow(/capacity/i);
    peer.store("object-a", shard, "repair");
    expect(peer.usedBytes).toBe(760);
  });
});

