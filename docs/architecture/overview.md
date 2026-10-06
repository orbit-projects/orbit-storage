# Orbit Storage: architecture and boundaries

## Responsibility

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

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `pydantic>=2.9,<3`
- Optional `dev` group: `pytest>=8,<10`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_storage/__init__.py`
- `src/orbit_storage/contracts.py`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
