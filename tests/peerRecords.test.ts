import { describe, expect, it } from "vitest";

import { PEER_RECORD_VERSION, createLocalSwarm, createLocalSwarmFromRecords } from "../src/kryden.js";

describe("persistent peer records", () => {
  it("restores peer identity and online state from records", () => {
    const swarm = createLocalSwarm(3, 1024 * 1024);
    swarm.setPeerOnline("peer-2", false);

    const records = swarm.toPeerRecords();
    const restored = createLocalSwarmFromRecords(records);

    expect(records.every((record) => record.version === PEER_RECORD_VERSION)).toBe(true);
    expect(restored.toPeerRecords()).toEqual(records);
    expect(restored.offlinePeerIds()).toEqual(["peer-2"]);
  });

  it("rejects unsupported peer record versions", () => {
    const records = createLocalSwarm(1, 1024 * 1024).toPeerRecords();

    expect(() =>
      createLocalSwarmFromRecords([{ ...records[0], version: 999 as typeof PEER_RECORD_VERSION }])
    ).toThrow(/Unsupported peer record version/i);
  });
});
