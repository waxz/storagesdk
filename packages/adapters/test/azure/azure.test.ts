import { describe, expect, it } from 'vitest';
import { azure } from '../../src/azure/azure.js';
import { storageAdapterTestSuite } from '../../src/test-suite.js';

// All connection settings come from `AZURE_*` env vars. `AZURE_BUCKET`
// is required — the suite skips entirely when it isn't set. The other
// vars fall back to Azurite (the official Microsoft Blob emulator)
// defaults, so `pnpm test` works against the local
// `docker compose up azurite` stack out of the box.
//
// The well-known Azurite dev account is published by Microsoft and
// works on every Azurite install:
// https://learn.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key
const BUCKET = process.env.AZURE_BUCKET;
const ACCOUNT_NAME = process.env.AZURE_ACCOUNT_NAME ?? 'devstoreaccount1';
const ACCOUNT_KEY =
  process.env.AZURE_ACCOUNT_KEY ??
  'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==';
const ENDPOINT =
  process.env.AZURE_ENDPOINT ?? 'http://127.0.0.1:10000/devstoreaccount1';

const configured = Boolean(BUCKET);

const buildAdapter = () =>
  azure({
    bucket: BUCKET as string,
    accountName: ACCOUNT_NAME,
    accountKey: ACCOUNT_KEY,
    endpoint: ENDPOINT,
  });

storageAdapterTestSuite({
  name: 'azure adapter',
  skip: !configured,
  adapter: buildAdapter,
});

if (!configured) {
  describe('azure adapter (skipped)', () => {
    it('skipped: AZURE_BUCKET not set', () => {
      expect(true).toBe(true);
    });
  });
}

describe.skipIf(!configured)('azure adapter implementation', () => {
  it('stages blocks while the body stream is still open', async () => {
    const adapter = buildAdapter();
    const key = `stream-${Date.now()}-${Math.random().toString(36).slice(2)}.bin`;
    const blob = adapter.raw
      .getContainerClient(BUCKET as string)
      .getBlockBlobClient(key);
    const part = 1024 * 1024;
    const chunk = new Uint8Array(part).fill(7);
    let sent = 0;
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        if (sent === 2) {
          // Hold the stream open until Azure holds a staged block. An
          // upload that collects the whole body first never gets there.
          const deadline = Date.now() + 3_000;
          for (;;) {
            // 404 until the first block is staged.
            const list = await blob
              .getBlockList('uncommitted')
              .catch(() => undefined);
            if ((list?.uncommittedBlocks?.length ?? 0) > 0) break;
            if (Date.now() > deadline) {
              controller.error(
                new Error('no block staged before the stream ended')
              );
              return;
            }
            await new Promise((r) => setTimeout(r, 100));
          }
        }
        if (sent === 3) {
          controller.close();
          return;
        }
        controller.enqueue(chunk);
        sent += 1;
      },
    });
    try {
      const meta = await adapter.upload(key, body, { partSize: part });
      expect(meta.size).toBe(3 * part);
      const item = await adapter.download(key);
      expect(item.size).toBe(3 * part);
      expect(item.body.every((b) => b === 7)).toBe(true);
    } finally {
      await adapter.delete(key).catch(() => {});
    }
  });
});
