export function concatBuffers(buffers: readonly Buffer[]): Buffer {
  return Buffer.concat(buffers);
}

export function toHex(buffer: Buffer): string {
  return buffer.toString("hex");
}

export function fromHex(value: string, fieldName = "hex value"): Buffer {
  if (!/^[0-9a-f]*$/i.test(value) || value.length % 2 !== 0) {
    throw new Error(`Invalid ${fieldName}`);
  }

  return Buffer.from(value, "hex");
}

export function constantTimeEquals(a: Buffer, b: Buffer): boolean {
  if (a.length !== b.length) {
    return false;
  }

  let diff = 0;
  for (let index = 0; index < a.length; index += 1) {
    diff |= a[index] ^ b[index];
  }

  return diff === 0;
}

