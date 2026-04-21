import { randomBytes } from "node:crypto";
import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { createInterface } from "node:readline";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import type { EncodedShard } from "../src/erasure/reedSolomon.js";
import type { ShardDescriptor } from "../src/storage/manifest.js";
import type { StorageAuditChallenge, StorageAuditProof } from "../src/swarm/audit.js";
import type { StorePurpose } from "../src/swarm/peer.js";
import {
  createPeerIdentity,
  createSignedPeerRequest,
  PeerMembershipRegistry,
  startPeerRuntimeServer,
  REMOTE_PEER_RECORD_VERSION,
  RemoteKrydenClient,
  RemotePeerClient,
  RemoteSwarm,
  verifyPeerHeartbeat,
  type PeerIdentity,
  type RemotePeerClientOptions,
  type RemotePeerRecord
} from "../src/kryden.js";

interface RuntimeProcess {
  peerId: string;
  url: string;
  child: ChildProcessWithoutNullStreams;
}

const runtimeProcesses: RuntimeProcess[] = [];

describe("real peer runtime", () => {
  afterEach(async () => {
    await Promise.all(runtimeProcesses.splice(0).map(stopRuntimeProcess));
  });

  it("audits and repairs through separate peer processes", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const runtimes = await startRuntimeProcesses(8, 1024 * 1024, 8, coordinator);
    const registry = new PeerMembershipRegistry();
    const bootstrap = await registry.bootstrap(runtimes.map((runtime) => runtime.url));
    const remoteSwarm = registry.createRemoteSwarm({ authority: coordinator });
    const client = new RemoteKrydenClient(remoteSwarm);
    const plaintext = randomBytes(32 * 1024);
    const stored = await client.put(plaintext, { dataShards: 4, parityShards: 2 });
    const failedDescriptor = stored.manifest.shards[0];
    const failedRuntime = runtimes.find((runtime) => runtime.peerId === failedDescriptor.peerId);
    if (!failedRuntime) {
      throw new Error("test runtime missing failed peer");
    }

    await stopRuntimeProcess(failedRuntime);
    const audits = await client.audit(stored.manifest, 2);
    const report = await client.repair(stored.manifest, 2);
    const recovered = await client.get(report.updatedManifest, stored.secret);
    const postRepairAudits = await client.audit(report.updatedManifest, 2);

    expect(audits.filter((audit) => !audit.ok)).toHaveLength(1);
    expect(report.repaired).toHaveLength(1);
    expect(report.failed).toHaveLength(0);
    expect(report.repaired[0].oldPeerId).toBe(failedDescriptor.peerId);
    expect(report.repaired[0].newPeerId).not.toBe(failedDescriptor.peerId);
    expect(recovered.equals(plaintext)).toBe(true);
    expect(postRepairAudits.every((audit) => audit.ok)).toBe(true);
    expect(bootstrap.failed).toHaveLength(0);
    expect(bootstrap.active).toHaveLength(8);
  });

  it("bootstraps signed membership and expires stale peers", async () => {
    const runtimes = await startRuntimeProcesses(4, 1024 * 1024, 4);
    const registry = new PeerMembershipRegistry();
    const bootstrap = await registry.bootstrap(runtimes.map((runtime) => runtime.url));
    const active = registry.getActiveMembers();
    const heartbeat = active[0].heartbeat;
    const heartbeatExpiresAt = Date.parse(heartbeat.expiresAt);
    const expiredAt = new Date(
      Math.max(...active.map((entry) => Date.parse(entry.expiresAt))) + 1
    );

    expect(bootstrap.failed).toHaveLength(0);
    expect(active.map((entry) => entry.peerId)).toEqual(["peer-1", "peer-2", "peer-3", "peer-4"]);
    expect(active.every((entry) => entry.peer.version === REMOTE_PEER_RECORD_VERSION)).toBe(true);
    expect(verifyPeerHeartbeat(heartbeat)).toBe(true);
    expect(verifyPeerHeartbeat(heartbeat, new Date(heartbeatExpiresAt - 1))).toBe(true);
    expect(verifyPeerHeartbeat(heartbeat, new Date(heartbeatExpiresAt))).toBe(false);
    expect(() => registry.upsertHeartbeat(heartbeat)).toThrow(/Stale heartbeat/i);
    expect(verifyPeerHeartbeat({
      ...heartbeat,
      endpoint: `${heartbeat.endpoint}/tampered`
    })).toBe(false);
    expect(verifyPeerHeartbeat({
      ...heartbeat,
      peer: {
        ...heartbeat.peer,
        version: 999 as typeof REMOTE_PEER_RECORD_VERSION
      }
    })).toBe(false);
    expect(registry.pruneExpired(expiredAt)).toHaveLength(4);
    expect(registry.getActiveMembers(expiredAt)).toHaveLength(0);
    expect(() => registry.createRemoteSwarm(expiredAt)).toThrow(/No active peers/i);
  });

  it("rejects unsigned and replayed protected peer requests", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const auditOnlyOwner = createPeerIdentity("owner-audit-only");
    const runtime = await startPeerRuntimeServer({
      id: "peer-auth",
      capacityBytes: 1024 * 1024,
      trustedAuthorities: [{
        id: coordinator.peerId,
        role: "coordinator",
        publicKeyPem: coordinator.publicKeyPem
      }, {
        id: auditOnlyOwner.peerId,
        role: "owner",
        publicKeyPem: auditOnlyOwner.publicKeyPem,
        allowedOperations: ["audit"]
      }]
    });
    const body = {
      objectId: "object-auth",
      shard: {
        index: 0,
        checksum: "checksum",
        data: Buffer.from("signed shard").toString("base64")
      },
      purpose: "regular"
    };

    try {
      const unsigned = await fetch(`${runtime.url}/store`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
      });
      const signed = createSignedPeerRequest({
        authority: coordinator,
        operation: "store",
        path: "/store",
        body
      });
      const first = await fetch(`${runtime.url}/store`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(signed)
      });
      const replay = await fetch(`${runtime.url}/store`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(signed)
      });
      const unauthorized = await fetch(`${runtime.url}/store`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(createSignedPeerRequest({
          authority: auditOnlyOwner,
          authorityRole: "owner",
          operation: "store",
          path: "/store",
          body
        }))
      });

      expect(unsigned.status).toBe(401);
      expect(first.status).toBe(200);
      expect(replay.status).toBe(409);
      expect(unauthorized.status).toBe(403);
    } finally {
      await runtime.close();
    }
  });

  it("serves protected peer operations over HTTPS when TLS material is configured", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const tlsDir = await mkdtemp(join(tmpdir(), "kryden-peer-tls-"));
    const certPath = join(tlsDir, "cert.pem");
    const keyPath = join(tlsDir, "key.pem");
    execFileSync("openssl", [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "1",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1,DNS:localhost"
    ], { stdio: "ignore" });
    const previousTlsReject = process.env.NODE_TLS_REJECT_UNAUTHORIZED;
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    const runtime = await startPeerRuntimeServer({
      id: "peer-https",
      capacityBytes: 1024 * 1024,
      tls: {
        certPem: await readFile(certPath, "utf8"),
        keyPem: await readFile(keyPath, "utf8")
      },
      trustedAuthorities: [{
        id: coordinator.peerId,
        role: "coordinator",
        publicKeyPem: coordinator.publicKeyPem
      }]
    });

    try {
      const client = new RemotePeerClient(runtime.url, { authority: coordinator });
      await client.store("tls-object", {
        index: 0,
        checksum: "tls-checksum",
        data: Buffer.from("encrypted transport shard")
      });
      const restored = await client.retrieve("tls-object", 0);
      const heartbeat = await client.getHeartbeat();

      expect(runtime.url.startsWith("https://")).toBe(true);
      expect(heartbeat.endpoint.startsWith("https://")).toBe(true);
      expect(verifyPeerHeartbeat(heartbeat)).toBe(true);
      expect(restored?.data.toString()).toBe("encrypted transport shard");
    } finally {
      await runtime.close();
      if (previousTlsReject === undefined) {
        delete process.env.NODE_TLS_REJECT_UNAUTHORIZED;
      } else {
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = previousTlsReject;
      }
      await rm(tlsDir, { recursive: true, force: true });
    }
  });

  it("persists shard data across peer runtime restarts and cleans partial writes", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const storageDir = await mkdtemp(join(tmpdir(), "kryden-peer-store-"));
    const trustedAuthorities = [{
      id: coordinator.peerId,
      role: "coordinator" as const,
      publicKeyPem: coordinator.publicKeyPem
    }];
    let runtime = await startPeerRuntimeServer({
      id: "peer-durable",
      capacityBytes: 1024 * 1024,
      storageDir,
      trustedAuthorities
    });

    try {
      let client = new RemotePeerClient(runtime.url, { authority: coordinator });
      await client.store("durable-object", {
        index: 0,
        checksum: "checksum-v1",
        data: Buffer.from("durable shard v1")
      });
      await runtime.close();

      const partialDir = join(storageDir, "shards", "partial-object", "0");
      await mkdir(partialDir, { recursive: true });
      await writeFile(join(partialDir, "orphan.json.tmp"), "partial write");

      runtime = await startPeerRuntimeServer({
        id: "peer-durable",
        capacityBytes: 1024 * 1024,
        storageDir,
        trustedAuthorities
      });
      client = new RemotePeerClient(runtime.url, { authority: coordinator });
      const restored = await client.retrieve("durable-object", 0);
      expect(restored?.data.toString()).toBe("durable shard v1");
      expect(runtime.peer.usedBytes).toBe(Buffer.byteLength("durable shard v1"));

      await client.store("durable-object", {
        index: 0,
        checksum: "checksum-v2",
        data: Buffer.from("durable shard v2")
      });
      const replaced = await client.retrieve("durable-object", 0);
      const shardFiles = await listShardStoreFiles(storageDir);

      expect(replaced?.data.toString()).toBe("durable shard v2");
      expect(runtime.peer.usedBytes).toBe(Buffer.byteLength("durable shard v2"));
      expect(shardFiles.filter((file) => file.endsWith(".json"))).toHaveLength(1);
      expect(shardFiles.some((file) => file.endsWith(".tmp"))).toBe(false);
    } finally {
      await runtime.close().catch(() => undefined);
      await rm(storageDir, { recursive: true, force: true });
    }
  });

  it("repairs through delayed responses after a scheduler-to-peer partition", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const runtimes = await startRuntimeProcesses(8, 1024 * 1024, 8, coordinator);
    const delayedSwarm = new RemoteSwarm(
      runtimes.map((runtime) => new DelayedRemotePeerClient(runtime.url, { authority: coordinator }, 5))
    );
    const client = new RemoteKrydenClient(delayedSwarm);
    const plaintext = randomBytes(24 * 1024);
    const stored = await client.put(plaintext, { dataShards: 4, parityShards: 2 });
    const partitionedPeerIds = stored.manifest.shards.slice(0, 2).map((descriptor) => descriptor.peerId);

    await Promise.all(
      runtimes
        .filter((runtime) => partitionedPeerIds.includes(runtime.peerId))
        .map(stopRuntimeProcess)
    );

    const audits = await client.audit(stored.manifest, 2);
    const report = await client.repair(stored.manifest, 2);
    const recovered = await client.get(report.updatedManifest, stored.secret);

    expect(audits.filter((audit) => !audit.ok).map((audit) => audit.peerId).sort()).toEqual(
      [...partitionedPeerIds].sort()
    );
    expect(report.repaired).toHaveLength(2);
    expect(report.failed).toHaveLength(0);
    expect(recovered.equals(plaintext)).toBe(true);
  });

  it("ignores stale membership records during placement", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const runtimes = await startRuntimeProcesses(5, 1024 * 1024, 5, coordinator);
    const registry = new PeerMembershipRegistry();
    await registry.bootstrap(runtimes.map((runtime) => runtime.url));
    const staleRuntime = runtimes[0];
    await stopRuntimeProcess(staleRuntime);

    const remoteSwarm = registry.createRemoteSwarm({ authority: coordinator });
    const client = new RemoteKrydenClient(remoteSwarm);
    const stored = await client.put(randomBytes(16 * 1024), { dataShards: 3, parityShards: 1 });

    expect(stored.manifest.shards).toHaveLength(4);
    expect(stored.manifest.shards.some((descriptor) => descriptor.peerId === staleRuntime.peerId)).toBe(false);
  });

  it("reports repair write failure when a replacement peer restarts mid-write", async () => {
    const coordinator = createPeerIdentity("coordinator-1");
    const runtimes = await startRuntimeProcesses(8, 1024 * 1024, 8, coordinator);
    const normalSwarm = new RemoteSwarm(
      runtimes.map((runtime) => new RemotePeerClient(runtime.url, { authority: coordinator }))
    );
    const client = new RemoteKrydenClient(normalSwarm);
    const stored = await client.put(randomBytes(24 * 1024), { dataShards: 4, parityShards: 2 });
    const failedPeerId = stored.manifest.shards[0].peerId;
    const failedRuntime = runtimes.find((runtime) => runtime.peerId === failedPeerId);
    if (!failedRuntime) {
      throw new Error("test runtime missing failed peer");
    }

    await stopRuntimeProcess(failedRuntime);
    const faultSwarm = new RemoteSwarm(
      runtimes.map((runtime) => new FailRepairStoreOnceClient(runtime.url, { authority: coordinator }))
    );
    const faultClient = new RemoteKrydenClient(faultSwarm);
    const interrupted = await faultClient.repair(stored.manifest, 2);
    const repaired = await client.repair(stored.manifest, 2);

    expect(interrupted.repaired).toHaveLength(0);
    expect(interrupted.failed).toHaveLength(1);
    expect(interrupted.failed[0].reason).toMatch(/restart during repair write/i);
    expect(repaired.repaired).toHaveLength(1);
    expect(repaired.failed).toHaveLength(0);
  });
});

async function startRuntimeProcesses(
  count: number,
  capacityBytes: number,
  failureDomainCount: number,
  trustedAuthority?: PeerIdentity
): Promise<RuntimeProcess[]> {
  const runtimes = await Promise.all(
    Array.from({ length: count }, async (_, index) => {
      const authorityArgs = trustedAuthority
        ? [
            "--trusted-authority-id",
            trustedAuthority.peerId,
            "--trusted-authority-public-key-base64",
            Buffer.from(trustedAuthority.publicKeyPem).toString("base64")
          ]
        : [];
      const child = spawn(process.execPath, [
        "--import",
        "tsx",
        join(process.cwd(), "src/swarm/peerRuntime.ts"),
        "--id",
        `peer-${index + 1}`,
        "--capacity-bytes",
        String(capacityBytes),
        "--port",
        "0",
        "--failure-bucket",
        `bucket-${(index % failureDomainCount) + 1}`,
        "--failure-host",
        `host-${index + 1}`,
        ...authorityArgs
      ], {
        cwd: process.cwd(),
        stdio: ["pipe", "pipe", "pipe"]
      });

      const ready = await waitForReady(child);
      const runtime = {
        peerId: ready.peerId,
        url: ready.url,
        child
      };
      runtimeProcesses.push(runtime);
      return runtime;
    })
  );

  return runtimes;
}

async function waitForReady(child: ChildProcessWithoutNullStreams): Promise<{ peerId: string; url: string }> {
  const stdout = createInterface({ input: child.stdout });
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk);
  });

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Timed out waiting for peer runtime: ${stderr}`));
    }, 5000);

    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      reject(new Error(`Peer runtime exited before ready: code=${code} signal=${signal} stderr=${stderr}`));
    });

    stdout.once("line", (line) => {
      clearTimeout(timer);
      const parsed = JSON.parse(line) as { ready?: boolean; peerId?: string; url?: string };
      if (!parsed.ready || !parsed.peerId || !parsed.url) {
        reject(new Error(`Invalid peer runtime ready payload: ${line}`));
        return;
      }

      resolve({ peerId: parsed.peerId, url: parsed.url });
    });
  });
}

async function stopRuntimeProcess(runtime: RuntimeProcess): Promise<void> {
  const index = runtimeProcesses.indexOf(runtime);
  if (index !== -1) {
    runtimeProcesses.splice(index, 1);
  }

  if (runtime.child.exitCode !== null || runtime.child.signalCode !== null) {
    return;
  }

  runtime.child.kill("SIGTERM");
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, 1000);
    runtime.child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function listShardStoreFiles(root: string): Promise<string[]> {
  const out: string[] = [];

  async function walk(path: string): Promise<void> {
    const entries = await readdir(path, { withFileTypes: true }).catch(() => []);
    for (const entry of entries) {
      const childPath = join(path, entry.name);
      if (entry.isDirectory()) {
        await walk(childPath);
      } else if (entry.isFile()) {
        out.push(childPath);
      }
    }
  }

  await walk(join(root, "shards"));
  return out;
}

class DelayedRemotePeerClient extends RemotePeerClient {
  private readonly delayMs: number;

  constructor(url: string, options: RemotePeerClientOptions, delayMs: number) {
    super(url, options);
    this.delayMs = delayMs;
  }

  override async getRecord(): Promise<RemotePeerRecord> {
    await delay(this.delayMs);
    return super.getRecord();
  }

  override async store(
    objectId: string,
    shard: EncodedShard,
    purpose: StorePurpose = "regular"
  ): Promise<RemotePeerRecord> {
    await delay(this.delayMs);
    return super.store(objectId, shard, purpose);
  }

  override async retrieve(objectId: string, shardIndex: number): Promise<EncodedShard | undefined> {
    await delay(this.delayMs);
    return super.retrieve(objectId, shardIndex);
  }

  override async respondToAudit(
    challenge: StorageAuditChallenge,
    descriptor: ShardDescriptor
  ): Promise<StorageAuditProof | undefined> {
    await delay(this.delayMs);
    return super.respondToAudit(challenge, descriptor);
  }
}

class FailRepairStoreOnceClient extends RemotePeerClient {
  private hasFailedRepairStore = false;

  override async store(
    objectId: string,
    shard: EncodedShard,
    purpose: StorePurpose = "regular"
  ): Promise<RemotePeerRecord> {
    if (purpose === "repair" && !this.hasFailedRepairStore) {
      this.hasFailedRepairStore = true;
      throw new Error("Simulated peer restart during repair write");
    }

    return super.store(objectId, shard, purpose);
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
