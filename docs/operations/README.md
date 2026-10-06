# Orbit Storage: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-storage` defines a provider-neutral asynchronous object storage capability. It has no cloud
SDK, HTTP client, filesystem backend, or dependency on Orbit Core. The planned `orbit-storage-s3`,
`orbit-storage-gcs`, and `orbit-storage-azure` packages can implement its `ObjectStore` protocol without
making vendor dependencies mandatory for applications that only need the contract.

```bash
python -m pip install orbit-storage
```

```python
from orbit_storage import ObjectStore


async def archive_report(store: ObjectStore, report: bytes) -> str:
    info = await store.put(
        "reports/weekly.json",
        report,
        content_type="application/json",
        metadata={"team": "analytics"},
    )
    return info.key
```

The protocol provides streaming-capable `put`, `get`, `delete`, paginated `list_page`, and `aclose`.
Uploads accept `bytes` or an async iterable of byte chunks. Downloads return a `StoredObject`; use
it as an async context manager and consume its closeable `ObjectReader` to release the provider
response on normal completion, failure, or cancellation. Concurrent close calls share one cleanup,
and cancellation is propagated only after that cleanup finishes. This avoids requiring whole-object
buffering for large files. Use
`ListObjectsRequest` to validate page size (1–1000), prefix, and bounded opaque continuation tokens.
`put` creates or
replaces a key; `get` returns `None` for an absent key; `delete` reports whether the key existed.
`OBJECT_STORE_DEPENDENCY_KEY` (`orbit.storage`) is the shared Core-container key adapters may use
to expose their implementation consistently.
Object keys are opaque bounded strings—not filesystem paths—and list tokens are opaque and specific
to the provider and configured bucket. The adapter configures its bucket/container, credentials,
transport, retries, and resource lifetime. Conditional writes, versioning, consistency, encryption,
retention, and multipart upload remain provider-specific and are not promised by this contract.

Implementations should raise `StorageConfigurationError` for invalid configuration/arguments and
`StorageOperationError` for sanitized provider failures; do not expose credentials or raw vendor
messages. Cancellation should propagate unchanged. Call `aclose` for resources owned by the store.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
