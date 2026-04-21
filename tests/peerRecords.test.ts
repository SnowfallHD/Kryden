import { describe, expect, it } from "vitest";

import { createLocalSwarm, createLocalSwarmFromRecords } from "../src/kryden.js";

describe("persistent peer records", () => {
  it("restores peer identity and online state from records", () => {
    const swarm = createLocalSwarm(3, 1024 * 1024);
    swarm.setPeerOnline("peer-2", false);

    const records = swarm.toPeerRecords();
    const restored = createLocalSwarmFromRecords(records);

    expect(restored.toPeerRecords()).toEqual(records);
    expect(restored.offlinePeerIds()).toEqual(["peer-2"]);
  });
});

