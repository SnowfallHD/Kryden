import { randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";

import { decryptPayload, encryptPayload } from "../src/crypto/envelope.js";

describe("encryption envelope", () => {
  it("decrypts back to the original plaintext", () => {
    const plaintext = randomBytes(4096);
    const encrypted = encryptPayload(plaintext);

    const decrypted = decryptPayload(encrypted.ciphertext, encrypted.envelope, encrypted.secret);

    expect(decrypted.equals(plaintext)).toBe(true);
  });

  it("rejects ciphertext tampering", () => {
    const plaintext = randomBytes(4096);
    const encrypted = encryptPayload(plaintext);
    encrypted.ciphertext[10] ^= 0xff;

    expect(() =>
      decryptPayload(encrypted.ciphertext, encrypted.envelope, encrypted.secret)
    ).toThrow(/integrity/i);
  });
});

