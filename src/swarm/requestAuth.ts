import { createHash, randomBytes } from "node:crypto";

import { fromHex } from "../util/bytes.js";
import { signPeerMessage, verifyPeerSignature, type PeerIdentity } from "./identity.js";

export const PEER_REQUEST_PROTOCOL = "kryden-peer-request-v1";
export const DEFAULT_PEER_REQUEST_TTL_MS = 30_000;
export const DEFAULT_PEER_REQUEST_CLOCK_SKEW_MS = 5_000;

export type PeerRequestAuthorityRole = "coordinator" | "owner";
export type PeerRequestOperation = "store" | "repair" | "retrieve" | "audit" | "admin";

export interface PeerRequestAuthority {
  id: string;
  role: PeerRequestAuthorityRole;
  publicKeyPem: string;
  allowedOperations?: readonly PeerRequestOperation[];
}

export interface SignedPeerRequestEnvelope<T = unknown> {
  protocol: typeof PEER_REQUEST_PROTOCOL;
  authorityId: string;
  authorityRole: PeerRequestAuthorityRole;
  operation: PeerRequestOperation;
  method: "POST";
  path: string;
  issuedAt: string;
  expiresAt: string;
  nonce: string;
  bodySha256: string;
  body: T;
  signature: string;
}

export interface CreateSignedPeerRequestOptions<T> {
  authority: PeerIdentity;
  authorityRole?: PeerRequestAuthorityRole;
  operation: PeerRequestOperation;
  path: string;
  body: T;
  now?: Date;
  ttlMs?: number;
  nonce?: Buffer;
}

export interface VerifySignedPeerRequestOptions {
  trustedAuthorities: readonly PeerRequestAuthority[];
  expectedPath: string;
  expectedOperations: readonly PeerRequestOperation[];
  now?: Date;
  clockSkewMs?: number;
  maxTtlMs?: number;
  replayProtector: PeerRequestReplayProtector;
}

export class PeerRequestAuthError extends Error {
  readonly statusCode: number;

  constructor(message: string, statusCode: number) {
    super(message);
    this.name = "PeerRequestAuthError";
    this.statusCode = statusCode;
  }
}

export class PeerRequestReplayProtector {
  private readonly seenNonces = new Map<string, number>();

  accept(authorityId: string, nonce: string, expiresAtMs: number, nowMs = Date.now()): void {
    this.prune(nowMs);
    const key = `${authorityId}:${nonce}`;
    if (this.seenNonces.has(key)) {
      throw new PeerRequestAuthError("Signed request replay detected", 409);
    }

    this.seenNonces.set(key, expiresAtMs);
  }

  private prune(nowMs: number): void {
    for (const [key, expiresAtMs] of this.seenNonces) {
      if (expiresAtMs <= nowMs) {
        this.seenNonces.delete(key);
      }
    }
  }
}

export function createSignedPeerRequest<T>(
  options: CreateSignedPeerRequestOptions<T>
): SignedPeerRequestEnvelope<T> {
  const now = options.now ?? new Date();
  const ttlMs = options.ttlMs ?? DEFAULT_PEER_REQUEST_TTL_MS;
  if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
    throw new Error("Signed peer request ttlMs must be a positive integer");
  }

  const unsigned: Omit<SignedPeerRequestEnvelope<T>, "signature"> = {
    protocol: PEER_REQUEST_PROTOCOL,
    authorityId: options.authority.peerId,
    authorityRole: options.authorityRole ?? "coordinator",
    operation: options.operation,
    method: "POST",
    path: options.path,
    issuedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + ttlMs).toISOString(),
    nonce: (options.nonce ?? randomBytes(32)).toString("hex"),
    bodySha256: bodySha256(options.body),
    body: options.body
  };

  return {
    ...unsigned,
    signature: signPeerMessage(options.authority, peerRequestTranscript(unsigned))
  };
}

export function verifySignedPeerRequest<T>(
  envelope: SignedPeerRequestEnvelope<T>,
  options: VerifySignedPeerRequestOptions
): PeerRequestAuthority {
  if (envelope.protocol !== PEER_REQUEST_PROTOCOL) {
    throw new PeerRequestAuthError("Unsupported signed request protocol", 401);
  }

  if (envelope.method !== "POST" || envelope.path !== options.expectedPath) {
    throw new PeerRequestAuthError("Signed request target mismatch", 401);
  }

  if (!options.expectedOperations.includes(envelope.operation)) {
    throw new PeerRequestAuthError("Signed request operation mismatch", 403);
  }

  if (
    envelope.nonce.length % 2 !== 0 ||
    !/^[0-9a-f]+$/i.test(envelope.nonce) ||
    fromHex(envelope.nonce, "request nonce").length < 16
  ) {
    throw new PeerRequestAuthError("Invalid signed request nonce", 401);
  }

  if (bodySha256(envelope.body) !== envelope.bodySha256) {
    throw new PeerRequestAuthError("Signed request body digest mismatch", 401);
  }

  const authority = options.trustedAuthorities.find((candidate) => candidate.id === envelope.authorityId);
  if (!authority || authority.role !== envelope.authorityRole) {
    throw new PeerRequestAuthError("Unknown signed request authority", 403);
  }

  if (!authorityAllows(authority, envelope.operation)) {
    throw new PeerRequestAuthError("Signed request authority is not allowed for this operation", 403);
  }

  const now = options.now ?? new Date();
  const clockSkewMs = options.clockSkewMs ?? DEFAULT_PEER_REQUEST_CLOCK_SKEW_MS;
  const maxTtlMs = options.maxTtlMs ?? DEFAULT_PEER_REQUEST_TTL_MS;
  const issuedAtMs = Date.parse(envelope.issuedAt);
  const expiresAtMs = Date.parse(envelope.expiresAt);
  if (!Number.isFinite(issuedAtMs) || !Number.isFinite(expiresAtMs) || expiresAtMs <= issuedAtMs) {
    throw new PeerRequestAuthError("Invalid signed request time window", 401);
  }

  const nowMs = now.getTime();
  if (expiresAtMs - issuedAtMs > maxTtlMs) {
    throw new PeerRequestAuthError("Signed request time window is too long", 401);
  }

  if (issuedAtMs - clockSkewMs > nowMs || expiresAtMs + clockSkewMs <= nowMs) {
    throw new PeerRequestAuthError("Signed request timestamp is outside the accepted window", 401);
  }

  const { signature, ...unsigned } = envelope;
  if (!verifyPeerSignature(authority.publicKeyPem, peerRequestTranscript(unsigned), signature)) {
    throw new PeerRequestAuthError("Invalid signed request signature", 401);
  }

  options.replayProtector.accept(envelope.authorityId, envelope.nonce, expiresAtMs + clockSkewMs, nowMs);
  return authority;
}

export function peerRequestTranscript<T>(
  request: Omit<SignedPeerRequestEnvelope<T>, "signature">
): Buffer {
  return Buffer.from(stableStringify(request));
}

function authorityAllows(authority: PeerRequestAuthority, operation: PeerRequestOperation): boolean {
  if (authority.allowedOperations) {
    return authority.allowedOperations.includes(operation);
  }

  if (authority.role === "coordinator") {
    return true;
  }

  return operation !== "admin";
}

function bodySha256(body: unknown): string {
  return createHash("sha256").update(stableStringify(body)).digest("hex");
}

function stableStringify(value: unknown): string {
  return JSON.stringify(sortForJson(value)) ?? "null";
}

function sortForJson(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortForJson);
  }

  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortForJson((value as Record<string, unknown>)[key]);
    }
    return out;
  }

  return value;
}
