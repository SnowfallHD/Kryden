#!/usr/bin/env node
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { randomBytes } from "node:crypto";
import { parseArgs } from "node:util";

import { decryptPayload, encryptPayload } from "./crypto/envelope.js";
import { decodeErasure, encodeErasure, type EncodedShard } from "./erasure/reedSolomon.js";
import {
  KrydenClient,
  createLocalSwarm,
  type ClientSecret,
  type StoredObjectManifest
} from "./kryden.js";

const command = process.argv[2];
const commandArgs = process.argv.slice(3);

async function main(): Promise<void> {
  if (command === "encode") {
    await encodeCommand(commandArgs);
    return;
  }

  if (command === "decode") {
    await decodeCommand(commandArgs);
    return;
  }

  if (command === "simulate") {
    await simulateCommand(commandArgs);
    return;
  }

  if (command === "simulate-scheduler") {
    await simulateSchedulerCommand(commandArgs);
    return;
  }

  printHelp();
  process.exitCode = command ? 1 : 0;
}

async function encodeCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    allowPositionals: true,
    options: {
      out: { type: "string", short: "o" },
      "data-shards": { type: "string", default: "6" },
      "parity-shards": { type: "string", default: "3" }
    }
  });

  const inputPath = parsed.positionals[0];
  const outDir = parsed.values.out;
  if (!inputPath || !outDir) {
    throw new Error("Usage: kryden encode <input> --out <directory>");
  }

  const plaintext = await readFile(inputPath);
  const encrypted = encryptPayload(plaintext);
  const encoded = encodeErasure(encrypted.ciphertext, {
    dataShards: parsePositiveInteger(parsed.values["data-shards"], "data-shards"),
    parityShards: parsePositiveInteger(parsed.values["parity-shards"], "parity-shards")
  });
  const contentId = `kd1_${encrypted.envelope.ciphertextSha256}`;
  const shardsDir = join(outDir, "shards");
  await mkdir(shardsDir, { recursive: true });

  const manifest: StoredObjectManifest = {
    version: 1,
    contentId,
    createdAt: new Date().toISOString(),
    encryption: encrypted.envelope,
    erasure: encoded.metadata,
    shards: encoded.shards.map((shard) => ({
      index: shard.index,
      peerId: `local-file-${shard.index}`,
      size: shard.data.length,
      checksum: shard.checksum
    }))
  };

  await Promise.all(
    encoded.shards.map((shard) => writeFile(join(shardsDir, `${shard.index}.shard`), shard.data))
  );
  await writeJson(join(outDir, "manifest.json"), manifest);
  await writeJson(join(outDir, "secret.kryden-secret.json"), encrypted.secret);

  console.log(`Encoded ${basename(inputPath)} as ${encoded.shards.length} encrypted shards.`);
  console.log(`Manifest: ${join(outDir, "manifest.json")}`);
  console.log(`Secret: ${join(outDir, "secret.kryden-secret.json")}`);
}

async function decodeCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    allowPositionals: true,
    options: {
      out: { type: "string", short: "o" }
    }
  });

  const objectDir = parsed.positionals[0];
  const outPath = parsed.values.out;
  if (!objectDir || !outPath) {
    throw new Error("Usage: kryden decode <object-directory> --out <file>");
  }

  const manifest = JSON.parse(await readFile(join(objectDir, "manifest.json"), "utf8")) as StoredObjectManifest;
  const secret = JSON.parse(await readFile(join(objectDir, "secret.kryden-secret.json"), "utf8")) as ClientSecret;
  const shardFiles = await readdir(join(objectDir, "shards"));
  const shards: EncodedShard[] = [];

  for (const file of shardFiles) {
    if (!file.endsWith(".shard")) {
      continue;
    }

    const index = Number(file.replace(/\.shard$/, ""));
    const descriptor = manifest.shards.find((candidate) => candidate.index === index);
    if (!descriptor) {
      continue;
    }

    shards.push({
      index,
      checksum: descriptor.checksum,
      data: await readFile(join(objectDir, "shards", file))
    });
  }

  const ciphertext = decodeErasure(shards, manifest.erasure);
  const plaintext = decryptPayload(ciphertext, manifest.encryption, secret);
  await writeFile(outPath, plaintext);
  console.log(`Decoded ${plaintext.length} bytes to ${outPath}.`);
}

async function simulateCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    options: {
      size: { type: "string", default: "1048576" },
      peers: { type: "string", default: "12" },
      "data-shards": { type: "string", default: "6" },
      "parity-shards": { type: "string", default: "3" },
      "fail-peers": { type: "string", default: "3" },
      "failure-domains": { type: "string" },
      "reserved-bytes": { type: "string", default: "0" },
      "repair-headroom-bytes": { type: "string" },
      "skip-repair": { type: "boolean", default: false }
    }
  });

  const size = parsePositiveInteger(parsed.values.size, "size");
  const peers = parsePositiveInteger(parsed.values.peers, "peers");
  const dataShards = parsePositiveInteger(parsed.values["data-shards"], "data-shards");
  const parityShards = parsePositiveInteger(parsed.values["parity-shards"], "parity-shards");
  const failPeers = parsePositiveInteger(parsed.values["fail-peers"], "fail-peers");
  const failureDomainCount = parseOptionalPositiveInteger(parsed.values["failure-domains"], "failure-domains");
  const reservedBytes = parseNonNegativeInteger(parsed.values["reserved-bytes"], "reserved-bytes");
  const repairHeadroomBytes = parseOptionalNonNegativeInteger(
    parsed.values["repair-headroom-bytes"],
    "repair-headroom-bytes"
  );
  const plaintext = randomBytes(size);
  const swarm = createLocalSwarm(peers, size * 2, {
    failureDomainCount,
    reservedBytes,
    repairHeadroomBytes
  });
  const client = new KrydenClient(swarm);
  const stored = client.put(plaintext, { dataShards, parityShards });

  for (const descriptor of stored.manifest.shards.slice(0, failPeers)) {
    swarm.setPeerOnline(descriptor.peerId, false);
  }

  const recovered = client.get(stored.manifest, stored.secret);
  const ok = recovered.equals(plaintext);
  const audits = client.audit(stored.manifest);
  const passingAudits = audits.filter((audit) => audit.ok).length;
  const repair = parsed.values["skip-repair"] ? undefined : client.repair(stored.manifest);
  const repairedRecovery = repair ? client.get(repair.updatedManifest, stored.secret).equals(plaintext) : undefined;
  const postRepairAudits = repair ? client.audit(repair.updatedManifest) : undefined;

  console.log(
    JSON.stringify(
      {
        ok,
        size,
        peers,
        offlinePeers: swarm.offlinePeerIds(),
        failureDomains: new Set(swarm.peers.map((peer) => peer.failureDomain.bucket)).size,
        audits: {
          passing: passingAudits,
          total: audits.length,
          failures: audits
            .filter((audit) => !audit.ok)
            .map((audit) => ({
              shardIndex: audit.shardIndex,
              peerId: audit.peerId,
              error: audit.error
            }))
        },
        repair: repair
          ? {
              repairedRecovery,
              repaired: repair.repaired,
              failed: repair.failed,
              postRepairAudits: {
                passing: postRepairAudits?.filter((audit) => audit.ok).length ?? 0,
                total: postRepairAudits?.length ?? 0
              }
            }
          : undefined,
        erasure: stored.manifest.erasure,
        contentId: stored.manifest.contentId
      },
      null,
      2
    )
  );

  if (!ok) {
    process.exitCode = 1;
  }
}

async function simulateSchedulerCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    options: {
      size: { type: "string", default: "1048576" },
      peers: { type: "string", default: "12" },
      "data-shards": { type: "string", default: "6" },
      "parity-shards": { type: "string", default: "3" },
      "fail-peers": { type: "string", default: "3" },
      "failure-domains": { type: "string" },
      "reserved-bytes": { type: "string", default: "0" },
      "repair-headroom-bytes": { type: "string" },
      state: { type: "string", default: "tmp/kryden-scheduler-state.sqlite" },
      "sample-count": { type: "string", default: "3" }
    }
  });

  const size = parsePositiveInteger(parsed.values.size, "size");
  const peers = parsePositiveInteger(parsed.values.peers, "peers");
  const dataShards = parsePositiveInteger(parsed.values["data-shards"], "data-shards");
  const parityShards = parsePositiveInteger(parsed.values["parity-shards"], "parity-shards");
  const failPeers = parsePositiveInteger(parsed.values["fail-peers"], "fail-peers");
  const failureDomainCount = parseOptionalPositiveInteger(parsed.values["failure-domains"], "failure-domains");
  const reservedBytes = parseNonNegativeInteger(parsed.values["reserved-bytes"], "reserved-bytes");
  const repairHeadroomBytes = parseOptionalNonNegativeInteger(
    parsed.values["repair-headroom-bytes"],
    "repair-headroom-bytes"
  );
  const sampleCount = parsePositiveInteger(parsed.values["sample-count"], "sample-count");
  const statePath = requireString(parsed.values.state, "state");

  const plaintext = randomBytes(size);
  const swarm = createLocalSwarm(peers, size * 2, {
    failureDomainCount,
    reservedBytes,
    repairHeadroomBytes
  });
  const client = new KrydenClient(swarm);
  const [{ BackgroundRepairScheduler }, { SQLiteStateStore }] = await Promise.all([
    import("./scheduler/backgroundRepairScheduler.js"),
    import("./state/store.js")
  ]);
  const store = new SQLiteStateStore(statePath);
  const scheduler = new BackgroundRepairScheduler(swarm, store, { sampleCount });
  const stored = client.put(plaintext, { dataShards, parityShards });

  await scheduler.trackObject(stored.manifest);

  for (const descriptor of stored.manifest.shards.slice(0, failPeers)) {
    swarm.setPeerOnline(descriptor.peerId, false);
  }

  const summary = await scheduler.runOnce();
  const state = await store.load();
  const persistedObject = state.objects[stored.manifest.contentId];
  const recovered = client.get(persistedObject.manifest, stored.secret);
  const postRepairAudits = client.audit(persistedObject.manifest, sampleCount);

  console.log(
    JSON.stringify(
      {
        ok: recovered.equals(plaintext),
        statePath,
        offlinePeers: swarm.offlinePeerIds(),
        failureDomains: new Set(swarm.peers.map((peer) => peer.failureDomain.bucket)).size,
        run: summary.run,
        trackedObjects: Object.keys(state.objects).length,
        peerHealth: state.peers,
        postRepairAudits: {
          passing: postRepairAudits.filter((audit) => audit.ok).length,
          total: postRepairAudits.length
        }
      },
      null,
      2
    )
  );
}

function parsePositiveInteger(value: string | boolean | undefined, name: string): number {
  if (typeof value !== "string") {
    throw new Error(`${name} must be provided`);
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
}

function parseOptionalPositiveInteger(value: string | boolean | undefined, name: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }

  return parsePositiveInteger(value, name);
}

function parseNonNegativeInteger(value: string | boolean | undefined, name: string): number {
  if (typeof value !== "string") {
    throw new Error(`${name} must be provided`);
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }

  return parsed;
}

function parseOptionalNonNegativeInteger(value: string | boolean | undefined, name: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }

  return parseNonNegativeInteger(value, name);
}

function requireString(value: string | boolean | undefined, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be provided`);
  }

  return value;
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
}

function printHelp(): void {
  console.log(`Kryden prototype CLI

Usage:
  kryden encode <input> --out <directory> [--data-shards 6] [--parity-shards 3]
  kryden decode <object-directory> --out <file>
  kryden simulate [--size 1048576] [--peers 12] [--data-shards 6] [--parity-shards 3] [--fail-peers 3] [--failure-domains 12] [--reserved-bytes 0] [--repair-headroom-bytes n] [--skip-repair]
  kryden simulate-scheduler [--state tmp/kryden-scheduler-state.sqlite] [--sample-count 3] [--failure-domains 12] [--reserved-bytes 0] [--repair-headroom-bytes n]
`);
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
