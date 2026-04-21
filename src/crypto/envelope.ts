import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";

import { constantTimeEquals, fromHex, toHex } from "../util/bytes.js";
import type { ClientSecret, EncryptionEnvelope } from "../storage/manifest.js";

const ALGORITHM = "aes-256-gcm";
const PUBLIC_ALGORITHM_NAME = "AES-256-GCM";
const KEY_BYTES = 32;
const NONCE_BYTES = 12;

export interface EncryptedPayload {
  ciphertext: Buffer;
  envelope: EncryptionEnvelope;
  secret: ClientSecret;
}

export function sha256Hex(data: Buffer): string {
  return createHash("sha256").update(data).digest("hex");
}

export function encryptPayload(plaintext: Buffer): EncryptedPayload {
  const key = randomBytes(KEY_BYTES);
  const nonce = randomBytes(NONCE_BYTES);
  const cipher = createCipheriv(ALGORITHM, key, nonce);
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();

  return {
    ciphertext,
    envelope: {
      algorithm: PUBLIC_ALGORITHM_NAME,
      nonce: toHex(nonce),
      tag: toHex(tag),
      plaintextSize: plaintext.length,
      ciphertextSize: ciphertext.length,
      ciphertextSha256: sha256Hex(ciphertext)
    },
    secret: {
      algorithm: PUBLIC_ALGORITHM_NAME,
      key: toHex(key)
    }
  };
}

export function decryptPayload(
  ciphertext: Buffer,
  envelope: EncryptionEnvelope,
  secret: ClientSecret
): Buffer {
  if (envelope.algorithm !== PUBLIC_ALGORITHM_NAME || secret.algorithm !== PUBLIC_ALGORITHM_NAME) {
    throw new Error("Unsupported encryption algorithm");
  }

  const expectedHash = fromHex(envelope.ciphertextSha256, "ciphertext hash");
  const actualHash = fromHex(sha256Hex(ciphertext), "ciphertext hash");
  if (!constantTimeEquals(expectedHash, actualHash)) {
    throw new Error("Ciphertext integrity check failed");
  }

  const key = fromHex(secret.key, "content key");
  if (key.length !== KEY_BYTES) {
    throw new Error("Invalid content key length");
  }

  const nonce = fromHex(envelope.nonce, "nonce");
  const tag = fromHex(envelope.tag, "authentication tag");
  const decipher = createDecipheriv(ALGORITHM, key, nonce);
  decipher.setAuthTag(tag);
  const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);

  if (plaintext.length !== envelope.plaintextSize) {
    throw new Error("Plaintext size mismatch");
  }

  return plaintext;
}

