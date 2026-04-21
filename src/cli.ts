#!/usr/bin/env node
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { randomBytes } from "node:crypto";
import { parseArgs } from "node:util";

import { decryptPayload, encryptPayload } from "./crypto/envelope.js";
import { decodeErasure, encodeErasure, type EncodedShard } from "./erasure/reedSolomon.js";
import { assertSupportedManifest } from "./storage/manifest.js";
import type { TrackedObjectRecord } from "./state/store.js";
import {
  KrydenClient,
  MANIFEST_VERSION,
  createLocalSwarm,
  createPeerIdentity,
  type ClientSecret,
  type PeerIdentity,
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

  if (command === "inspect") {
    await inspectCommand(commandArgs);
    return;
  }

  if (command === "identity") {
    await identityCommand(commandArgs);
    return;
  }

  if (command === "cluster-put") {
    await clusterPutCommand(commandArgs);
    return;
  }

  if (command === "cluster-scheduler") {
    await clusterSchedulerCommand(commandArgs);
    return;
  }

  if (command === "peer-runtime") {
    await peerRuntimeCommand(commandArgs);
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
    version: MANIFEST_VERSION,
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
  assertSupportedManifest(manifest);
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
      "sample-count": { type: "string", default: "3" },
      "max-repairs-per-run": { type: "string" },
      "object-cooldown-ms": { type: "string" },
      "degraded-backoff-base-ms": { type: "string" },
      "degraded-backoff-max-ms": { type: "string" }
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
  const maxRepairsPerRun = parseOptionalNonNegativeInteger(
    parsed.values["max-repairs-per-run"],
    "max-repairs-per-run"
  );
  const objectCooldownMs = parseOptionalNonNegativeInteger(
    parsed.values["object-cooldown-ms"],
    "object-cooldown-ms"
  );
  const degradedBackoffBaseMs = parseOptionalNonNegativeInteger(
    parsed.values["degraded-backoff-base-ms"],
    "degraded-backoff-base-ms"
  );
  const degradedBackoffMaxMs = parseOptionalNonNegativeInteger(
    parsed.values["degraded-backoff-max-ms"],
    "degraded-backoff-max-ms"
  );
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
  const scheduler = new BackgroundRepairScheduler(swarm, store, {
    sampleCount,
    maxRepairsPerRun,
    objectCooldownMs,
    degradedBackoffBaseMs,
    degradedBackoffMaxMs
  });
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

async function peerRuntimeCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    options: {
      id: { type: "string" },
      "capacity-bytes": { type: "string" },
      host: { type: "string", default: "127.0.0.1" },
      port: { type: "string", default: "0" },
      "reserved-bytes": { type: "string", default: "0" },
      "repair-headroom-bytes": { type: "string" },
      "heartbeat-ttl-ms": { type: "string" },
      "failure-bucket": { type: "string" },
      "failure-host": { type: "string" },
      "storage-dir": { type: "string" },
      "tls-cert": { type: "string" },
      "tls-key": { type: "string" },
      "trusted-authority-id": { type: "string" },
      "trusted-authority-public-key-base64": { type: "string" }
    }
  });

  const id = requireString(parsed.values.id, "id");
  const capacityBytes = parsePositiveInteger(parsed.values["capacity-bytes"], "capacity-bytes");
  const port = parseNonNegativeInteger(parsed.values.port, "port");
  const reservedBytes = parseNonNegativeInteger(parsed.values["reserved-bytes"], "reserved-bytes");
  const repairHeadroomBytes = parseOptionalNonNegativeInteger(
    parsed.values["repair-headroom-bytes"],
    "repair-headroom-bytes"
  );
  const heartbeatTtlMs = parseOptionalPositiveInteger(
    parsed.values["heartbeat-ttl-ms"],
    "heartbeat-ttl-ms"
  );
  const host = requireString(parsed.values.host, "host");
  const trustedAuthorityId = parsed.values["trusted-authority-id"];
  const trustedAuthorityPublicKeyBase64 = parsed.values["trusted-authority-public-key-base64"];
  if (
    (typeof trustedAuthorityId === "string") !==
    (typeof trustedAuthorityPublicKeyBase64 === "string")
  ) {
    throw new Error("trusted-authority-id and trusted-authority-public-key-base64 must be provided together");
  }
  const tlsCert = parsed.values["tls-cert"];
  const tlsKey = parsed.values["tls-key"];
  if ((typeof tlsCert === "string") !== (typeof tlsKey === "string")) {
    throw new Error("tls-cert and tls-key must be provided together");
  }

  const [{ startPeerRuntimeServer }] = await Promise.all([
    import("./swarm/peerRuntime.js")
  ]);
  const runtime = await startPeerRuntimeServer({
    id,
    capacityBytes,
    host,
    port,
    reservedBytes,
    repairHeadroomBytes,
    heartbeatTtlMs,
    storageDir: typeof parsed.values["storage-dir"] === "string"
      ? parsed.values["storage-dir"]
      : undefined,
    tls: typeof tlsCert === "string" && typeof tlsKey === "string"
      ? {
          certPem: await readFile(tlsCert, "utf8"),
          keyPem: await readFile(tlsKey, "utf8")
        }
      : undefined,
    trustedAuthorities: typeof trustedAuthorityId === "string" && typeof trustedAuthorityPublicKeyBase64 === "string"
      ? [{
          id: trustedAuthorityId,
          role: "coordinator",
          publicKeyPem: Buffer.from(trustedAuthorityPublicKeyBase64, "base64").toString("utf8")
        }]
      : [],
    failureDomain: {
      bucket: typeof parsed.values["failure-bucket"] === "string"
        ? parsed.values["failure-bucket"]
        : id,
      host: typeof parsed.values["failure-host"] === "string"
        ? parsed.values["failure-host"]
        : id
    }
  });

  console.log(JSON.stringify({
    ready: true,
    peerId: runtime.peer.id,
    url: runtime.url
  }));
  await new Promise<void>(() => {});
}

async function inspectCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    allowPositionals: true,
    options: {
      state: { type: "string", default: "tmp/kryden-scheduler-state.sqlite" }
    }
  });
  const target = parsed.positionals[0];
  const id = parsed.positionals[1];
  const statePath = requireString(parsed.values.state, "state");
  const { SQLiteStateStore } = await import("./state/store.js");
  const store = new SQLiteStateStore(statePath);

  try {
    if (target === "run") {
      const runId = id ?? "latest";
      const run = await store.getSchedulerRun(runId);
      if (!run) {
        throw new Error(`Scheduler run ${runId} was not found`);
      }

      printJson({
        type: "scheduler-run",
        statePath,
        run
      });
      return;
    }

    if (target === "object") {
      if (!id) {
        throw new Error("Usage: kryden inspect object <content-id> --state <sqlite-path>");
      }

      const object = await store.getTrackedObject(id);
      if (!object) {
        throw new Error(`Tracked object ${id} was not found`);
      }

      printJson({
        type: "tracked-object",
        statePath,
        object: objectSummary(object)
      });
      return;
    }

    if (target === "peer") {
      if (!id) {
        throw new Error("Usage: kryden inspect peer <peer-id> --state <sqlite-path>");
      }

      const peer = await store.getPeerInspection(id);
      if (!peer) {
        throw new Error(`Peer ${id} was not found`);
      }

      printJson({
        type: "peer",
        statePath,
        peer
      });
      return;
    }

    if (target === "degraded") {
      const degraded = await store.getDegradedObjects();
      printJson({
        type: "degraded-objects",
        statePath,
        count: degraded.length,
        objects: degraded.map(objectSummary)
      });
      return;
    }
  } finally {
    store.close();
  }

  throw new Error("Usage: kryden inspect <run|object|peer|degraded> [id] --state <sqlite-path>");
}

async function identityCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    options: {
      id: { type: "string" },
      out: { type: "string", short: "o" }
    }
  });
  const identity = createPeerIdentity(
    typeof parsed.values.id === "string" ? parsed.values.id : undefined
  );
  const payload = {
    ...identity,
    publicKeyBase64: Buffer.from(identity.publicKeyPem).toString("base64")
  };

  if (typeof parsed.values.out === "string") {
    await writeJson(parsed.values.out, payload);
    console.log(JSON.stringify({
      peerId: identity.peerId,
      identityPath: parsed.values.out,
      publicKeyBase64: payload.publicKeyBase64
    }, null, 2));
    return;
  }

  printJson(payload);
}

async function clusterPutCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    allowPositionals: true,
    options: {
      config: { type: "string" },
      state: { type: "string" },
      authority: { type: "string" },
      peer: { type: "string", multiple: true },
      peers: { type: "string" },
      out: { type: "string", short: "o" },
      "data-shards": { type: "string" },
      "parity-shards": { type: "string" },
      "tls-insecure": { type: "boolean", default: false }
    }
  });
  const config = await loadClusterConfig(parsed.values.config);
  const inputPath = parsed.positionals[0];
  if (!inputPath) {
    throw new Error("Usage: kryden cluster-put <input> --state <db> --authority <identity.json> --peer <url>");
  }

  applyTlsInsecureFlag(parsed.values["tls-insecure"] === true || config.tlsInsecure === true);
  const authority = await loadIdentity(requireConfigString(parsed.values.authority, config.authorityPath, "authority"));
  const peerEndpoints = parsePeerEndpoints(parsed.values.peer, parsed.values.peers, config.peerEndpoints);
  const statePath = requireConfigString(parsed.values.state, config.statePath, "state");
  const dataShards = parseOptionalPositiveInteger(parsed.values["data-shards"], "data-shards")
    ?? config.dataShards
    ?? 4;
  const parityShards = parseOptionalPositiveInteger(parsed.values["parity-shards"], "parity-shards")
    ?? config.parityShards
    ?? 2;
  const [{ RemoteKrydenClient }, { RemotePeerClient, RemoteSwarm }, { SQLiteStateStore }] = await Promise.all([
    import("./swarm/remoteSwarm.js"),
    import("./swarm/remoteSwarm.js"),
    import("./state/store.js")
  ]);
  const swarm = new RemoteSwarm(
    peerEndpoints.map((endpoint) => new RemotePeerClient(endpoint, { authority }))
  );
  const client = new RemoteKrydenClient(swarm);
  const plaintext = await readFile(inputPath);
  const stored = await client.put(plaintext, { dataShards, parityShards });
  const store = new SQLiteStateStore(statePath);
  try {
    await store.trackObject(stored.manifest);
  } finally {
    store.close();
  }

  const output = {
    statePath,
    inputPath,
    manifest: stored.manifest,
    secret: stored.secret,
    peerEndpoints
  };
  if (typeof parsed.values.out === "string") {
    await writeJson(parsed.values.out, output);
    console.log(JSON.stringify({
      contentId: stored.manifest.contentId,
      manifestPath: parsed.values.out,
      statePath
    }, null, 2));
    return;
  }

  printJson(output);
}

async function clusterSchedulerCommand(args: string[]): Promise<void> {
  const parsed = parseArgs({
    args,
    options: {
      config: { type: "string" },
      state: { type: "string" },
      authority: { type: "string" },
      peer: { type: "string", multiple: true },
      peers: { type: "string" },
      "interval-ms": { type: "string" },
      "sample-count": { type: "string" },
      "max-repairs-per-run": { type: "string" },
      "object-cooldown-ms": { type: "string" },
      "degraded-backoff-base-ms": { type: "string" },
      "degraded-backoff-max-ms": { type: "string" },
      "run-once": { type: "boolean", default: false },
      "tls-insecure": { type: "boolean", default: false }
    }
  });
  const config = await loadClusterConfig(parsed.values.config);
  applyTlsInsecureFlag(parsed.values["tls-insecure"] === true || config.tlsInsecure === true);
  const authority = await loadIdentity(requireConfigString(parsed.values.authority, config.authorityPath, "authority"));
  const peerEndpoints = parsePeerEndpoints(parsed.values.peer, parsed.values.peers, config.peerEndpoints);
  const statePath = requireConfigString(parsed.values.state, config.statePath, "state");
  const intervalMs = parseOptionalPositiveInteger(parsed.values["interval-ms"], "interval-ms")
    ?? config.scheduler?.intervalMs
    ?? 60_000;
  const sampleCount = parseOptionalPositiveInteger(parsed.values["sample-count"], "sample-count")
    ?? config.scheduler?.sampleCount
    ?? 3;
  const maxRepairsPerRun = parseOptionalNonNegativeInteger(
    parsed.values["max-repairs-per-run"],
    "max-repairs-per-run"
  ) ?? config.scheduler?.maxRepairsPerRun;
  const objectCooldownMs = parseOptionalNonNegativeInteger(
    parsed.values["object-cooldown-ms"],
    "object-cooldown-ms"
  ) ?? config.scheduler?.objectCooldownMs;
  const degradedBackoffBaseMs = parseOptionalNonNegativeInteger(
    parsed.values["degraded-backoff-base-ms"],
    "degraded-backoff-base-ms"
  ) ?? config.scheduler?.degradedBackoffBaseMs;
  const degradedBackoffMaxMs = parseOptionalNonNegativeInteger(
    parsed.values["degraded-backoff-max-ms"],
    "degraded-backoff-max-ms"
  ) ?? config.scheduler?.degradedBackoffMaxMs;
  const [{ BackgroundRepairScheduler }, { PeerMembershipRegistry }, { SQLiteStateStore }] = await Promise.all([
    import("./scheduler/backgroundRepairScheduler.js"),
    import("./swarm/membership.js"),
    import("./state/store.js")
  ]);
  const store = new SQLiteStateStore(statePath);

  const runClusterPass = async () => {
    const registry = new PeerMembershipRegistry();
    const bootstrap = await registry.bootstrap(peerEndpoints);
    const swarm = registry.createRemoteSwarm({ authority });
    const scheduler = new BackgroundRepairScheduler(swarm, store, {
      sampleCount,
      maxRepairsPerRun,
      objectCooldownMs,
      degradedBackoffBaseMs,
      degradedBackoffMaxMs
    });
    const summary = await scheduler.runOnce();
    console.log(JSON.stringify({
      type: "cluster-scheduler-run",
      statePath,
      activePeers: bootstrap.active.length,
      failedPeers: bootstrap.failed,
      run: summary.run
    }));
  };

  const shutdown = () => {
    store.close();
    process.exit(0);
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);

  await runClusterPass();
  if (parsed.values["run-once"] === true) {
    store.close();
    return;
  }

  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      runClusterPass().catch((error) => {
        console.error(JSON.stringify({
          type: "cluster-scheduler-error",
          message: error instanceof Error ? error.message : String(error)
        }));
      });
    }, intervalMs);
    process.once("exit", () => clearInterval(timer));
  });
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

function printJson(value: unknown): void {
  console.log(JSON.stringify(value, null, 2));
}

function objectSummary(object: TrackedObjectRecord): unknown {
  return {
    contentId: object.contentId,
    registeredAt: object.registeredAt,
    updatedAt: object.updatedAt,
    consecutiveDegradedRuns: object.consecutiveDegradedRuns,
    nextEligibleAt: object.nextEligibleAt,
    lastSchedulerRunAt: object.lastSchedulerRunAt,
    lastRepairAt: object.lastRepairAt,
    erasure: object.manifest.erasure,
    shardCount: object.manifest.shards.length,
    placements: object.manifest.shards.map((descriptor) => ({
      shardIndex: descriptor.index,
      peerId: descriptor.peerId,
      failureDomain: descriptor.failureDomain,
      size: descriptor.size,
      checksum: descriptor.checksum,
      merkleRoot: descriptor.merkleRoot
    }))
  };
}

interface ClusterConfig {
  statePath?: string;
  authorityPath?: string;
  peerEndpoints?: string[];
  tlsInsecure?: boolean;
  dataShards?: number;
  parityShards?: number;
  scheduler?: {
    intervalMs?: number;
    sampleCount?: number;
    maxRepairsPerRun?: number;
    objectCooldownMs?: number;
    degradedBackoffBaseMs?: number;
    degradedBackoffMaxMs?: number;
  };
}

async function loadClusterConfig(path: string | boolean | undefined): Promise<ClusterConfig> {
  if (typeof path !== "string") {
    return {};
  }

  return JSON.parse(await readFile(path, "utf8")) as ClusterConfig;
}

async function loadIdentity(path: string): Promise<PeerIdentity> {
  const parsed = JSON.parse(await readFile(path, "utf8")) as PeerIdentity;
  if (!parsed.peerId || !parsed.publicKeyPem || !parsed.privateKeyPem) {
    throw new Error(`Invalid coordinator identity file ${path}`);
  }

  return parsed;
}

function parsePeerEndpoints(
  repeated: string[] | string | boolean | undefined,
  commaSeparated: string | boolean | undefined,
  configured: string[] | undefined
): string[] {
  const endpoints = [
    ...(Array.isArray(repeated) ? repeated : typeof repeated === "string" ? [repeated] : []),
    ...(typeof commaSeparated === "string" ? commaSeparated.split(",") : []),
    ...(configured ?? [])
  ]
    .map((endpoint) => endpoint.trim())
    .filter((endpoint) => endpoint.length > 0);
  if (endpoints.length === 0) {
    throw new Error("At least one peer endpoint is required");
  }

  return [...new Set(endpoints)];
}

function requireConfigString(
  cliValue: string | boolean | undefined,
  configValue: string | undefined,
  name: string
): string {
  if (typeof cliValue === "string" && cliValue.length > 0) {
    return cliValue;
  }

  if (configValue && configValue.length > 0) {
    return configValue;
  }

  throw new Error(`${name} must be provided`);
}

function applyTlsInsecureFlag(enabled: boolean): void {
  if (enabled) {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
  }
}

function printHelp(): void {
  console.log(`Kryden prototype CLI

Usage:
  kryden encode <input> --out <directory> [--data-shards 6] [--parity-shards 3]
  kryden decode <object-directory> --out <file>
  kryden simulate [--size 1048576] [--peers 12] [--data-shards 6] [--parity-shards 3] [--fail-peers 3] [--failure-domains 12] [--reserved-bytes 0] [--repair-headroom-bytes n] [--skip-repair]
  kryden simulate-scheduler [--state tmp/kryden-scheduler-state.sqlite] [--sample-count 3] [--max-repairs-per-run n] [--object-cooldown-ms n] [--degraded-backoff-base-ms n] [--degraded-backoff-max-ms n] [--failure-domains 12] [--reserved-bytes 0] [--repair-headroom-bytes n]
  kryden inspect run [run-id|latest] [--state tmp/kryden-scheduler-state.sqlite]
  kryden inspect object <content-id> [--state tmp/kryden-scheduler-state.sqlite]
  kryden inspect peer <peer-id> [--state tmp/kryden-scheduler-state.sqlite]
  kryden inspect degraded [--state tmp/kryden-scheduler-state.sqlite]
  kryden identity [--id coordinator-1] [--out cluster/coordinator.identity.json]
  kryden cluster-put <input> --state cluster/coordinator.sqlite --authority cluster/coordinator.identity.json --peer https://peer-1:9443 [--peer ...] [--out cluster/object.json]
  kryden cluster-scheduler --state cluster/coordinator.sqlite --authority cluster/coordinator.identity.json --peer https://peer-1:9443 [--peer ...] [--interval-ms 60000]
  kryden peer-runtime --id peer-1 --capacity-bytes 1048576 [--host 127.0.0.1] [--port 0] [--failure-bucket bucket-1] [--heartbeat-ttl-ms 30000] [--storage-dir ./tmp/peer-1] [--tls-cert cert.pem --tls-key key.pem] [--trusted-authority-id coordinator-1 --trusted-authority-public-key-base64 ...]
`);
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
