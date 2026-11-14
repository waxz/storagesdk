---
'@storagesdk/adapters': patch
---

The Azure, GCS, WebDAV, Vercel Blob and Code Storage adapters now stream
`ReadableStream` upload bodies instead of reading them into memory first.
Azure stages `partSize` blocks (8 MiB by default) and commits them; GCS
pipes into a resumable upload; WebDAV sends the body with chunked transfer
encoding; Vercel Blob always uses its multipart upload for streams; Code
Storage passes the stream to `addFile`. Other body types upload as before.
