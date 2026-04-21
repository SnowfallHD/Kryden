#!/usr/bin/env node
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { pathToFileURL } from "node:url";

import type { EncodedShard } from "../erasure/reedSolomon.js";
import type { ShardDescriptor } from "../storage/manifest.js";
import type { StorageAuditChallenge } from "./audit.js";
import { DEFAULT_HEARTBEAT_TTL_MS, createSignedPeerHeartbeat } from "./membership.js";
import { PeerStore, type PeerStoreOptions, type StorePurpose } from "./peer.js";

export const REMOTE_PEER_RECORD_VERSION = 1;

export interface PeerRuntimeOptions extends PeerStoreOptions {
  id: string;
  capacityBytes: number;
  host?: string;
  port?: number;
  heartbeatTtlMs?: number;
}

export interface PeerRuntimeServer {
  peer: PeerStore;
  server: Server;
  url: string;
  close(): Promise<void>;
}

interface StoreRequest {
  objectId: string;
  shard: WireShard;
  purpose?: StorePurpose;
}

interface RetrieveRequest {
  objectId: string;
  shardIndex: number;
}

interface AuditRequest {
  challenge: StorageAuditChallenge;
  descriptor: ShardDescriptor;
}

interface OnlineRequest {
  online: boolean;
}

interface CorruptRequest {
  objectId: string;
  shardIndex: number;
}

interface WireShard {
  index: number;
  checksum: string;
  data: string;
}

export async function startPeerRuntimeServer(options: PeerRuntimeOptions): Promise<PeerRuntimeServer> {
  const host = options.host ?? "127.0.0.1";
  const heartbeatTtlMs = options.heartbeatTtlMs ?? DEFAULT_HEARTBEAT_TTL_MS;
  if (!Number.isInteger(heartbeatTtlMs) || heartbeatTtlMs <= 0) {
    throw new Error("heartbeatTtlMs must be a positive integer");
  }

  const peer = new PeerStore(options.id, options.capacityBytes, undefined, {
    reservedBytes: options.reservedBytes,
    repairHeadroomBytes: options.repairHeadroomBytes,
    failureDomain: options.failureDomain
  });
  let runtimeUrl = "";
  let heartbeatSequence = 0;

  const server = createServer(async (request, response) => {
    try {
      await routePeerRequest({
        peer,
        heartbeatTtlMs,
        endpoint: () => runtimeUrl,
        nextHeartbeatSequence: () => {
          heartbeatSequence += 1;
          return heartbeatSequence;
        }
      }, request, response);
    } catch (error) {
      sendJson(response, 500, {
        error: error instanceof Error ? error.message : "Unknown peer runtime error"
      });
    }
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? 0, host, () => {
      server.off("error", reject);
      resolve();
    });
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Peer runtime did not bind to a TCP address");
  }
  runtimeUrl = `http://${host}:${address.port}`;

  return {
    peer,
    server,
    url: runtimeUrl,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      })
  };
}

interface PeerRuntimeContext {
  peer: PeerStore;
  heartbeatTtlMs: number;
  endpoint(): string;
  nextHeartbeatSequence(): number;
}

async function routePeerRequest(
  context: PeerRuntimeContext,
  request: IncomingMessage,
  response: ServerResponse
): Promise<void> {
  const { peer } = context;

  if (request.method === "GET" && (request.url === "/health" || request.url === "/record")) {
    sendJson(response, 200, { peer: publicPeerRecord(peer) });
    return;
  }

  if (request.method === "GET" && request.url === "/heartbeat") {
    sendJson(response, 200, {
      heartbeat: createSignedPeerHeartbeat(
        peer.identity,
        publicPeerRecord(peer),
        context.endpoint(),
        new Date(),
        context.heartbeatTtlMs,
        context.nextHeartbeatSequence()
      )
    });
    return;
  }

  if (request.method === "POST" && request.url === "/store") {
    const body = await readJson<StoreRequest>(request);
    peer.store(body.objectId, shardFromWire(body.shard), body.purpose ?? "regular");
    sendJson(response, 200, { peer: publicPeerRecord(peer) });
    return;
  }

  if (request.method === "POST" && request.url === "/retrieve") {
    const body = await readJson<RetrieveRequest>(request);
    const shard = peer.retrieve(body.objectId, body.shardIndex);
    sendJson(response, 200, { shard: shard ? shardToWire(shard) : null });
    return;
  }

  if (request.method === "POST" && request.url === "/audit") {
    const body = await readJson<AuditRequest>(request);
    const proof = peer.respondToAudit(body.challenge, body.descriptor);
    sendJson(response, 200, { proof: proof ?? null });
    return;
  }

  if (request.method === "POST" && request.url === "/online") {
    const body = await readJson<OnlineRequest>(request);
    peer.online = body.online;
    sendJson(response, 200, { peer: publicPeerRecord(peer) });
    return;
  }

  if (request.method === "POST" && request.url === "/corrupt") {
    const body = await readJson<CorruptRequest>(request);
    peer.corruptShard(body.objectId, body.shardIndex);
    sendJson(response, 200, { peer: publicPeerRecord(peer) });
    return;
  }

  sendJson(response, 404, { error: "Not found" });
}

function publicPeerRecord(peer: PeerStore): RemotePeerRecord {
  return {
    version: REMOTE_PEER_RECORD_VERSION,
    peerId: peer.id,
    capacityBytes: peer.capacityBytes,
    reservedBytes: peer.reservedBytes,
    repairHeadroomBytes: peer.repairHeadroomBytes,
    usedBytes: peer.usedBytes,
    allocatableBytes: peer.allocatableBytes,
    regularFreeBytes: peer.regularFreeBytes,
    repairFreeBytes: peer.repairFreeBytes,
    failureDomain: { ...peer.failureDomain },
    publicKeyPem: peer.publicKeyPem,
    online: peer.online
  };
}

export interface RemotePeerRecord {
  version: typeof REMOTE_PEER_RECORD_VERSION;
  peerId: string;
  capacityBytes: number;
  reservedBytes: number;
  repairHeadroomBytes: number;
  usedBytes: number;
  allocatableBytes: number;
  regularFreeBytes: number;
  repairFreeBytes: number;
  failureDomain: {
    bucket: string;
    deviceGroup?: string;
    host?: string;
    subnet?: string;
  };
  publicKeyPem: string;
  online: boolean;
}

export function assertSupportedRemotePeerRecord(record: RemotePeerRecord): void {
  if (record.version !== REMOTE_PEER_RECORD_VERSION) {
    throw new Error(`Unsupported remote peer record version ${String(record.version)}`);
  }
}

function shardFromWire(shard: WireShard): EncodedShard {
  return {
    index: shard.index,
    checksum: shard.checksum,
    data: Buffer.from(shard.data, "base64")
  };
}

function shardToWire(shard: EncodedShard): WireShard {
  return {
    index: shard.index,
    checksum: shard.checksum,
    data: shard.data.toString("base64")
  };
}

async function readJson<T>(request: IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }

  if (chunks.length === 0) {
    return {} as T;
  }

  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as T;
}

function sendJson(response: ServerResponse, statusCode: number, body: unknown): void {
  response.writeHead(statusCode, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

function parseCliOptions(argv: readonly string[]): PeerRuntimeOptions {
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      continue;
    }

    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }

    values.set(token.slice(2), next);
    index += 1;
  }

  const id = values.get("id");
  const capacityBytes = Number(values.get("capacity-bytes"));
  if (!id) {
    throw new Error("--id is required");
  }

  if (!Number.isInteger(capacityBytes) || capacityBytes <= 0) {
    throw new Error("--capacity-bytes must be a positive integer");
  }

  const port = values.has("port") ? Number(values.get("port")) : undefined;
  const reservedBytes = values.has("reserved-bytes") ? Number(values.get("reserved-bytes")) : undefined;
  const repairHeadroomBytes = values.has("repair-headroom-bytes")
    ? Number(values.get("repair-headroom-bytes"))
    : undefined;
  const heartbeatTtlMs = values.has("heartbeat-ttl-ms")
    ? Number(values.get("heartbeat-ttl-ms"))
    : undefined;
  const bucket = values.get("failure-bucket") ?? id;
  const host = values.get("host") ?? "127.0.0.1";

  return {
    id,
    capacityBytes,
    host,
    port,
    reservedBytes,
    repairHeadroomBytes,
    heartbeatTtlMs,
    failureDomain: {
      bucket,
      host: values.get("failure-host") ?? id
    }
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  startPeerRuntimeServer(parseCliOptions(process.argv.slice(2)))
    .then((runtime) => {
      process.stdout.write(JSON.stringify({
        ready: true,
        peerId: runtime.peer.id,
        url: runtime.url
      }) + "\n");
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    });
}
