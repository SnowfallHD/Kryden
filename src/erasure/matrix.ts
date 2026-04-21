import { inverse, mul } from "./galois.js";

export type Matrix = Uint8Array[];

export function identityMatrix(size: number): Matrix {
  return Array.from({ length: size }, (_, rowIndex) => {
    const row = new Uint8Array(size);
    row[rowIndex] = 1;
    return row;
  });
}

export function multiplyMatrices(left: Matrix, right: Matrix): Matrix {
  if (left.length === 0 || right.length === 0) {
    return [];
  }

  const shared = right.length;
  const columns = right[0].length;
  return left.map((leftRow) => {
    if (leftRow.length !== shared) {
      throw new Error("Matrix dimension mismatch");
    }

    const out = new Uint8Array(columns);
    for (let column = 0; column < columns; column += 1) {
      let value = 0;
      for (let index = 0; index < shared; index += 1) {
        value ^= mul(leftRow[index], right[index][column]);
      }
      out[column] = value;
    }
    return out;
  });
}

export function invertMatrix(matrix: Matrix): Matrix {
  const size = matrix.length;
  if (size === 0 || matrix.some((row) => row.length !== size)) {
    throw new Error("Only non-empty square matrices can be inverted");
  }

  const augmented = matrix.map((row, rowIndex) => {
    const out = new Uint8Array(size * 2);
    out.set(row, 0);
    out[size + rowIndex] = 1;
    return out;
  });

  for (let column = 0; column < size; column += 1) {
    let pivot = column;
    while (pivot < size && augmented[pivot][column] === 0) {
      pivot += 1;
    }

    if (pivot === size) {
      throw new Error("Matrix is singular");
    }

    if (pivot !== column) {
      const current = augmented[column];
      augmented[column] = augmented[pivot];
      augmented[pivot] = current;
    }

    const pivotValue = augmented[column][column];
    if (pivotValue !== 1) {
      const scale = inverse(pivotValue);
      for (let index = 0; index < size * 2; index += 1) {
        augmented[column][index] = mul(augmented[column][index], scale);
      }
    }

    for (let row = 0; row < size; row += 1) {
      if (row === column) {
        continue;
      }

      const factor = augmented[row][column];
      if (factor === 0) {
        continue;
      }

      for (let index = 0; index < size * 2; index += 1) {
        augmented[row][index] ^= mul(factor, augmented[column][index]);
      }
    }
  }

  return augmented.map((row) => row.slice(size));
}

