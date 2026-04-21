const FIELD_SIZE = 256;
const FIELD_ORDER = 255;
const PRIMITIVE_POLYNOMIAL = 0x11d;

const expTable = new Uint8Array(FIELD_ORDER * 2);
const logTable = new Uint8Array(FIELD_SIZE);

let value = 1;
for (let index = 0; index < FIELD_ORDER; index += 1) {
  expTable[index] = value;
  logTable[value] = index;
  value <<= 1;
  if ((value & FIELD_SIZE) !== 0) {
    value ^= PRIMITIVE_POLYNOMIAL;
  }
}

for (let index = FIELD_ORDER; index < expTable.length; index += 1) {
  expTable[index] = expTable[index - FIELD_ORDER];
}

export function add(a: number, b: number): number {
  return a ^ b;
}

export function sub(a: number, b: number): number {
  return a ^ b;
}

export function mul(a: number, b: number): number {
  if (a === 0 || b === 0) {
    return 0;
  }

  return expTable[logTable[a] + logTable[b]];
}

export function div(a: number, b: number): number {
  if (b === 0) {
    throw new Error("GF(256) division by zero");
  }

  if (a === 0) {
    return 0;
  }

  return expTable[(logTable[a] - logTable[b] + FIELD_ORDER) % FIELD_ORDER];
}

export function pow(a: number, exponent: number): number {
  if (exponent < 0) {
    throw new Error("GF(256) negative exponents are not supported");
  }

  if (exponent === 0) {
    return 1;
  }

  if (a === 0) {
    return 0;
  }

  return expTable[(logTable[a] * exponent) % FIELD_ORDER];
}

export function inverse(a: number): number {
  if (a === 0) {
    throw new Error("GF(256) inverse of zero");
  }

  return expTable[FIELD_ORDER - logTable[a]];
}

