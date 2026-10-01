const Base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

/**
 * Encode a vector as base64 little-endian float32, the provider's compact wire
 * form for every vector element type, in writes and in ANN queries alike. It
 * is about a quarter the size of JSON numbers, which dominate request bytes.
 */
export const encodeTurbopufferVector = (vector: ReadonlyArray<number>): string => {
  const bytes = new Uint8Array(vector.length * 4)
  const view = new DataView(bytes.buffer)
  vector.forEach((component, index) => view.setFloat32(index * 4, component, true))
  let encoded = ""

  for (let index = 0; index < bytes.length; index += 3) {
    const first = bytes[index] ?? 0
    const second = bytes[index + 1]
    const third = bytes[index + 2]
    const triple = (first << 16) | ((second ?? 0) << 8) | (third ?? 0)
    encoded += Base64Alphabet.charAt((triple >> 18) & 63)
    encoded += Base64Alphabet.charAt((triple >> 12) & 63)
    encoded += second === undefined ? "=" : Base64Alphabet.charAt((triple >> 6) & 63)
    encoded += third === undefined ? "=" : Base64Alphabet.charAt(triple & 63)
  }

  return encoded
}
