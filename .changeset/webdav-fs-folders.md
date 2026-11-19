---
'@storagesdk/adapters': patch
---

The WebDAV adapter sends streamed uploads with `fetch` instead of the
`webdav` client, so they also work where bundlers pick the client's browser
build (Cloudflare Workers and other edge runtimes). Digest auth still goes
through the client and reads the stream into memory first. `head` on a
WebDAV collection or a filesystem directory now throws `NotFound`, like a
prefix on an object store, instead of returning the folder's metadata.
