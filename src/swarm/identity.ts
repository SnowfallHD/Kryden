import { createHash, generateKeyPairSync, sign, verify } from "node:crypto";

import { fromHex } from "../util/bytes.js";

export interface PeerIdentity {
  peerId: string;
  publicKeyPem: string;
  privateKeyPem: string;
}

export function createPeerIdentity(peerId?: string): PeerIdentity {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const publicKeyPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const privateKeyPem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();

  return {
    peerId: peerId ?? derivePeerId(publicKeyPem),
    publicKeyPem,
    privateKeyPem
  };
}

export function derivePeerId(publicKeyPem: string): string {
  const digest = createHash("sha256").update(publicKeyPem).digest("hex").slice(0, 32);
  return `peer_${digest}`;
}

export function signPeerMessage(identity: PeerIdentity, message: Buffer): string {
  return sign(null, message, identity.privateKeyPem).toString("hex");
}

export function verifyPeerSignature(publicKeyPem: string, message: Buffer, signatureHex: string): boolean {
  try {
    return verify(null, message, publicKeyPem, fromHex(signatureHex, "peer signature"));
  } catch {
    return false;
  }
}

