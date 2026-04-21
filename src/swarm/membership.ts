import type { RemotePeerRecord } from "./peerRuntime.js";
import { RemotePeerClient, RemoteSwarm } from "./remoteSwarm.js";
import { signPeerMessage, verifyPeerSignature, type PeerIdentity } from "./identity.js";

export const HEARTBEAT_PROTOCOL = "kryden-peer-heartbeat-v1";
export const DEFAULT_HEARTBEAT_TTL_MS = 30_000;
const SUPPORTED_REMOTE_PEER_RECORD_VERSION = 1;

export interface PeerHeartbeat {
  protocol: typeof HEARTBEAT_PROTOCOL;
  peerId: string;
  endpoint: string;
  peer: RemotePeerRecord;
  issuedAt: string;
  expiresAt: string;
  sequence: number;
  signature: string;
}

export interface MembershipEntry {
  peerId: string;
  endpoint: string;
  peer: RemotePeerRecord;
  firstSeenAt: string;
  lastHeartbeatAt: string;
  expiresAt: string;
  sequence: number;
  heartbeat: PeerHeartbeat;
}

export interface MembershipBootstrapResult {
  active: MembershipEntry[];
  failed: Array<{ endpoint: string; error: string }>;
}

export function createSignedPeerHeartbeat(
  identity: PeerIdentity,
  peer: RemotePeerRecord,
  endpoint: string,
  now = new Date(),
  ttlMs = DEFAULT_HEARTBEAT_TTL_MS,
  sequence = 0
): PeerHeartbeat {
  if (!endpoint) {
    throw new Error("Heartbeat endpoint is required");
  }

  if (!Number.isInteger(ttlMs) || ttlMs <= 0) {
    throw new Error("Heartbeat ttlMs must be a positive integer");
  }

  if (!Number.isInteger(sequence) || sequence < 0) {
    throw new Error("Heartbeat sequence must be a non-negative integer");
  }

  const unsigned: Omit<PeerHeartbeat, "signature"> = {
    protocol: HEARTBEAT_PROTOCOL,
    peerId: identity.peerId,
    endpoint,
    peer,
    issuedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + ttlMs).toISOString(),
    sequence
  };

  return {
    ...unsigned,
    signature: signPeerMessage(identity, heartbeatTranscript(unsigned))
  };
}

export function verifyPeerHeartbeat(heartbeat: PeerHeartbeat, now = new Date()): boolean {
  if (
    heartbeat.protocol !== HEARTBEAT_PROTOCOL ||
    heartbeat.peer.version !== SUPPORTED_REMOTE_PEER_RECORD_VERSION ||
    heartbeat.peerId !== heartbeat.peer.peerId ||
    heartbeat.peer.publicKeyPem.length === 0 ||
    heartbeat.endpoint.length === 0 ||
    !Number.isInteger(heartbeat.sequence) ||
    heartbeat.sequence < 0
  ) {
    return false;
  }

  const issuedAt = Date.parse(heartbeat.issuedAt);
  const expiresAt = Date.parse(heartbeat.expiresAt);
  if (!Number.isFinite(issuedAt) || !Number.isFinite(expiresAt) || expiresAt <= issuedAt) {
    return false;
  }

  if (expiresAt <= now.getTime()) {
    return false;
  }

  const { signature, ...unsigned } = heartbeat;
  return verifyPeerSignature(heartbeat.peer.publicKeyPem, heartbeatTranscript(unsigned), signature);
}

export function heartbeatTranscript(heartbeat: Omit<PeerHeartbeat, "signature">): Buffer {
  return Buffer.from(JSON.stringify(heartbeat));
}

export class PeerMembershipRegistry {
  private readonly members = new Map<string, MembershipEntry>();

  async bootstrap(
    endpoints: readonly string[],
    now = new Date()
  ): Promise<MembershipBootstrapResult> {
    const failed: Array<{ endpoint: string; error: string }> = [];

    for (const endpoint of endpoints) {
      try {
        await this.refreshPeer(endpoint, now);
      } catch (error) {
        failed.push({
          endpoint,
          error: error instanceof Error ? error.message : "Unknown membership bootstrap error"
        });
      }
    }

    return {
      active: this.getActiveMembers(now),
      failed
    };
  }

  async refreshPeer(endpoint: string, now = new Date()): Promise<MembershipEntry> {
    const client = new RemotePeerClient(endpoint);
    return this.upsertHeartbeat(await client.getHeartbeat(), now);
  }

  upsertHeartbeat(heartbeat: PeerHeartbeat, now = new Date()): MembershipEntry {
    if (!verifyPeerHeartbeat(heartbeat, now)) {
      throw new Error(`Invalid heartbeat from ${heartbeat.peerId || heartbeat.endpoint}`);
    }

    const existing = this.members.get(heartbeat.peerId);
    if (existing && heartbeat.sequence < existing.sequence) {
      throw new Error(`Stale heartbeat from ${heartbeat.peerId}`);
    }

    const entry: MembershipEntry = {
      peerId: heartbeat.peerId,
      endpoint: heartbeat.endpoint,
      peer: { ...heartbeat.peer, failureDomain: { ...heartbeat.peer.failureDomain } },
      firstSeenAt: existing?.firstSeenAt ?? now.toISOString(),
      lastHeartbeatAt: heartbeat.issuedAt,
      expiresAt: heartbeat.expiresAt,
      sequence: heartbeat.sequence,
      heartbeat
    };
    this.members.set(entry.peerId, entry);
    return cloneEntry(entry);
  }

  pruneExpired(now = new Date()): MembershipEntry[] {
    const expired: MembershipEntry[] = [];
    for (const [peerId, entry] of this.members) {
      if (Date.parse(entry.expiresAt) <= now.getTime()) {
        this.members.delete(peerId);
        expired.push(cloneEntry(entry));
      }
    }

    return expired;
  }

  getActiveMembers(now = new Date()): MembershipEntry[] {
    this.pruneExpired(now);
    return [...this.members.values()]
      .sort((left, right) => left.peerId.localeCompare(right.peerId))
      .map(cloneEntry);
  }

  createRemoteSwarm(now = new Date()): RemoteSwarm {
    const active = this.getActiveMembers(now);
    if (active.length === 0) {
      throw new Error("No active peers in membership registry");
    }

    return new RemoteSwarm(active.map((entry) => new RemotePeerClient(entry.endpoint)));
  }
}

function cloneEntry(entry: MembershipEntry): MembershipEntry {
  return {
    ...entry,
    peer: { ...entry.peer, failureDomain: { ...entry.peer.failureDomain } },
    heartbeat: {
      ...entry.heartbeat,
      peer: {
        ...entry.heartbeat.peer,
        failureDomain: { ...entry.heartbeat.peer.failureDomain }
      }
    }
  };
}
