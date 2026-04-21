import { randomBytes } from "node:crypto";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  PeerMembershipRegistry,
  REMOTE_PEER_RECORD_VERSION,
  RemoteKrydenClient,
  verifyPeerHeartbeat
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
    const runtimes = await startRuntimeProcesses(8, 1024 * 1024, 8);
    const registry = new PeerMembershipRegistry();
    const bootstrap = await registry.bootstrap(runtimes.map((runtime) => runtime.url));
    const remoteSwarm = registry.createRemoteSwarm();
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
    const expiredAt = new Date(
      Math.max(...active.map((entry) => Date.parse(entry.expiresAt))) + 1
    );

    expect(bootstrap.failed).toHaveLength(0);
    expect(active.map((entry) => entry.peerId)).toEqual(["peer-1", "peer-2", "peer-3", "peer-4"]);
    expect(active.every((entry) => entry.peer.version === REMOTE_PEER_RECORD_VERSION)).toBe(true);
    expect(verifyPeerHeartbeat(heartbeat)).toBe(true);
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
});

async function startRuntimeProcesses(
  count: number,
  capacityBytes: number,
  failureDomainCount: number
): Promise<RuntimeProcess[]> {
  const runtimes = await Promise.all(
    Array.from({ length: count }, async (_, index) => {
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
        `host-${index + 1}`
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
