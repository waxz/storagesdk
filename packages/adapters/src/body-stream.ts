import { type BodyInput, bodyToBytes } from '@storagesdk/core/adapter';

/**
 * Helpers for adapters that upload a `ReadableStream` body piece by piece
 * instead of collecting it into one buffer first.
 */

/** Passes a stream through and counts the bytes that went by. */
export function countBytes(source: ReadableStream<Uint8Array>): {
  stream: ReadableStream<Uint8Array>;
  bytes: () => number;
} {
  let n = 0;
  const stream = source.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        n += chunk.byteLength;
        controller.enqueue(chunk);
      },
    })
  );
  return { stream, bytes: () => n };
}

/**
 * A body ready to send. Streams pass through untouched (and are counted as
 * they go); every other body type is read into bytes. `size()` is final
 * once the data has been consumed.
 */
export async function prepareBody(body: BodyInput): Promise<{
  data: ReadableStream<Uint8Array> | Uint8Array;
  streamed: boolean;
  size: () => number;
}> {
  if (body instanceof ReadableStream) {
    const counted = countBytes(body);
    return { data: counted.stream, streamed: true, size: counted.bytes };
  }
  const bytes = await bodyToBytes(body);
  return { data: bytes, streamed: false, size: () => bytes.byteLength };
}

/**
 * Re-chunks a stream into blocks of exactly `size` bytes (the last one may
 * be shorter). Holds at most one block in memory. Yields nothing for an
 * empty stream.
 */
export async function* blocks(
  source: ReadableStream<Uint8Array>,
  size: number
): AsyncGenerator<Uint8Array<ArrayBuffer>> {
  const reader = source.getReader();
  let block = new Uint8Array(size);
  let fill = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      for (let off = 0; off < value.byteLength; ) {
        const take = Math.min(size - fill, value.byteLength - off);
        block.set(value.subarray(off, off + take), fill);
        fill += take;
        off += take;
        if (fill === size) {
          yield block;
          block = new Uint8Array(size);
          fill = 0;
        }
      }
    }
    if (fill > 0) yield block.slice(0, fill);
  } finally {
    // Stops the source if the consumer bailed out early (error, abort).
    await reader.cancel().catch(() => {});
  }
}
