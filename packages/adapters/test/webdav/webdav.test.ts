import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { storageAdapterTestSuite } from '../../src/test-suite.js';
import { webdav } from '../../src/webdav/webdav.js';

const URL_ENV = process.env.WEBDAV_TEST_URL;
const USERNAME = process.env.WEBDAV_TEST_USERNAME;
const PASSWORD = process.env.WEBDAV_TEST_PASSWORD;
const ROOT = process.env.WEBDAV_TEST_ROOT;

// Live tests against a real WebDAV server. The user needs write access
// to `ROOT`. For local development, `docker compose up webdav` starts
// an Apache mod_dav container on port 8080 with `user:pass` and the
// repo root configured as `/`.
//
//   WEBDAV_TEST_URL=http://localhost:8080
//   WEBDAV_TEST_USERNAME=user
//   WEBDAV_TEST_PASSWORD=pass
//   WEBDAV_TEST_ROOT=/storagesdk
const configured = Boolean(URL_ENV && ROOT);

let folderCounter = 0;
const buildAdapter = () => {
  folderCounter += 1;
  return webdav({
    baseUrl: URL_ENV as string,
    root: ROOT as string,
    folder: `t${Date.now()}-${folderCounter}`,
    ...(USERNAME !== undefined ? { username: USERNAME } : {}),
    ...(PASSWORD !== undefined ? { password: PASSWORD } : {}),
  });
};

storageAdapterTestSuite({
  name: 'webdav adapter',
  skip: !configured,
  adapter: buildAdapter,
  capabilities: {
    // WebDAV's PROPPATCH dead properties are spec but support is
    // inconsistent; the adapter drops user metadata at the boundary.
    userMetadata: false,
    contentType: true,
    // No presigned upload concept; `url()` returns the plain resource
    // URL which requires the caller to supply auth.
    presignedUploads: false,
    fetchableSignedUrls: false,
  },
  // Local bytemark/webdav runs the whole suite in ~5s; hosted servers
  // (Nextcloud, pCloud, kDrive, NAS appliances) routinely sit at
  // 1–2s per HTTP op due to auth + DB lookups, and snapshot/fork
  // tests do 5–10 ops sequentially. 90s is enough for slow real-world
  // servers; the cascaded hook timeout means cleanup doesn't expire
  // either.
  testTimeoutMs: 90_000,
});

if (!configured) {
  describe('webdav adapter (skipped)', () => {
    it('skipped: WEBDAV_TEST_URL / WEBDAV_TEST_ROOT not set', () => {
      expect(true).toBe(true);
    });
  });
}

// Runs against an in-process HTTP server, so it needs no WebDAV setup.
describe('webdav adapter implementation', () => {
  it('sends the body while the stream is still open', async () => {
    let received = 0;
    let putAuth: string | undefined;
    const server = createServer((req, res) => {
      if (req.method === 'PUT') {
        putAuth = req.headers.authorization;
        req.on('data', (d: Buffer) => {
          received += d.byteLength;
        });
        req.on('end', () => {
          res.writeHead(201);
          res.end();
        });
        return;
      }
      // PROPFIND: nothing exists yet; MKCOL and the rest: created.
      res.writeHead(req.method === 'PROPFIND' ? 404 : 201);
      res.end();
    });
    await new Promise<void>((resolve) =>
      server.listen(0, '127.0.0.1', resolve)
    );
    const { port } = server.address() as AddressInfo;
    const chunk = new Uint8Array(64 * 1024).fill(1);
    let sent = 0;
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        if (sent === 2) {
          // Hold the stream open until the server has bytes. An upload
          // that collects the whole body first never sends any.
          const deadline = Date.now() + 3_000;
          while (received === 0) {
            if (Date.now() > deadline) {
              controller.error(
                new Error('nothing sent before the stream ended')
              );
              return;
            }
            await new Promise((r) => setTimeout(r, 20));
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
      const adapter = webdav({
        baseUrl: `http://127.0.0.1:${port}`,
        root: '/',
        folder: 'f',
        username: 'alice',
        password: 'pä:ss',
      });
      const meta = await adapter.upload('a.bin', body);
      expect(meta.size).toBe(3 * chunk.byteLength);
      expect(received).toBe(3 * chunk.byteLength);
      expect(putAuth).toBe(
        `Basic ${Buffer.from('alice:pä:ss').toString('base64')}`
      );
    } finally {
      server.close();
    }
  });
});
