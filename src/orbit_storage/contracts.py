# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Provider-neutral asynchronous object storage models and protocol."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_serializer,
    field_validator,
    model_validator,
)

_MAX_KEY_LENGTH = 1024
_MAX_METADATA_ITEMS = 128
_MAX_METADATA_TEXT = 2048
OBJECT_STORE_DEPENDENCY_KEY = "orbit.storage"
UploadBody = bytes | AsyncIterable[bytes]


def validate_object_key(value: str) -> str:
    """Validate an opaque storage key without treating it as a filesystem path."""
    if not isinstance(value, str) or not value or len(value) > _MAX_KEY_LENGTH:
        raise ValueError("Object keys must contain between 1 and 1024 characters.")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError("Object keys cannot contain control characters.")
    return value


class ObjectInfo(BaseModel):
    """Provider-neutral metadata returned for one stored object."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    key: StrictStr
    size_bytes: StrictInt = Field(ge=0)
    content_type: StrictStr | None = Field(default=None, max_length=255)
    etag: StrictStr | None = Field(default=None, max_length=1024)
    modified_at: datetime | None = None
    metadata: Mapping[StrictStr, StrictStr] = Field(default_factory=dict)

    @field_validator("key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        """Apply the shared opaque-key constraints to provider results too."""
        return validate_object_key(value)

    @field_validator("content_type", "etag")
    @classmethod
    def reject_header_controls(cls, value: str | None) -> str | None:
        """Prevent control characters from crossing header-oriented provider APIs."""
        if value is not None and any(
            ord(character) < 32 or ord(character) == 127 for character in value
        ):
            raise ValueError("Object metadata values cannot contain control characters.")
        return value

    @field_validator("metadata", mode="before")
    @classmethod
    def validate_metadata(cls, value: object) -> Mapping[str, str]:
        """Bound provider-neutral user metadata and reject implicit coercion."""
        if not isinstance(value, Mapping) or len(value) > _MAX_METADATA_ITEMS:
            raise ValueError("Object metadata must be a mapping with at most 128 entries.")
        normalized: dict[str, str] = {}
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                raise ValueError("Object metadata keys and values must be strings.")
            if not key or len(key) > _MAX_METADATA_TEXT or len(item) > _MAX_METADATA_TEXT:
                raise ValueError("Object metadata keys and values must be bounded and non-empty.")
            if any(ord(char) < 32 or ord(char) == 127 for char in key + item):
                raise ValueError("Object metadata cannot contain control characters.")
            normalized[key] = item
        return normalized

    def model_post_init(self, __context: object) -> None:
        """Prevent mutation of user metadata after model validation."""
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @field_serializer("metadata")
    def serialize_metadata(self, value: Mapping[str, str]) -> dict[str, str]:
        """Serialize the immutable mapping as a portable ordinary dictionary."""
        return dict(value)

    @model_validator(mode="after")
    def validate_timestamp(self) -> ObjectInfo:
        """Reject naive provider timestamps rather than inventing a timezone."""
        if self.modified_at is not None and self.modified_at.utcoffset() is None:
            raise ValueError("Object modified_at timestamps must include timezone information.")
        return self


@runtime_checkable
class ObjectReader(Protocol):
    """Async byte reader with explicit close support for streamed downloads."""

    def __aiter__(self) -> AsyncIterator[bytes]:
        """Return the asynchronous iterator over bounded byte chunks."""

    async def __anext__(self) -> bytes:
        """Return the next byte chunk or raise ``StopAsyncIteration``."""

    async def aclose(self) -> None:
        """Release the provider response and any network resources it owns."""


@dataclass(slots=True)
class StoredObject:
    """Metadata plus a streamed response body that callers must close.

    Use this as an async context manager so the response is closed even when consumption fails or
    is cancelled. The store adapter remains responsible for constructing a reader that releases
    its provider response in ``aclose``.
    """

    info: ObjectInfo
    body: ObjectReader
    _closed: bool = field(default=False, init=False, repr=False)
    _close_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)

    async def aclose(self) -> None:
        """Close the response body once and drain that close before propagating cancellation.

        Concurrent cleanup callers share the same close task. This prevents context cleanup and
        stream completion from concurrently returning or invalidating the same provider response.
        """
        task = self._close_task
        if task is None:
            task = asyncio.create_task(self.body.aclose())
            self._close_task = task

        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True

        task.result()
        self._closed = True
        if cancelled:
            raise asyncio.CancelledError

    async def __aenter__(self) -> StoredObject:
        """Enter a managed lifetime for the streamed object body."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the underlying response on success, failure, or cancellation."""
        await self.aclose()


class ListObjectsRequest(BaseModel):
    """Validated bounded pagination inputs shared by object store adapters."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    prefix: StrictStr = Field(default="", max_length=_MAX_KEY_LENGTH)
    limit: StrictInt = Field(default=100, ge=1, le=1000)
    continuation_token: StrictStr | None = Field(default=None, max_length=4096)

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        """Allow an empty prefix for a full listing; reject control characters."""
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("Object list prefixes cannot contain control characters.")
        return value

    @field_validator("continuation_token")
    @classmethod
    def validate_token(cls, value: str | None) -> str | None:
        """Treat provider pagination tokens as opaque text, not unbounded input."""
        if value is not None and any(
            ord(character) < 32 or ord(character) == 127 for character in value
        ):
            raise ValueError("Continuation tokens cannot contain control characters.")
        return value


class ObjectPage(BaseModel):
    """One bounded page of object metadata and an opaque continuation token."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    items: tuple[ObjectInfo, ...] = Field(max_length=1000)
    continuation_token: StrictStr | None = Field(default=None, max_length=4096)


class StorageError(Exception):
    """Base class for sanitized provider-neutral object storage failures."""


class StorageConfigurationError(StorageError, ValueError):
    """Raised when a storage operation argument or adapter setting is invalid."""


class StorageOperationError(StorageError):
    """Raised when a provider cannot safely complete a storage operation."""


@runtime_checkable
class ObjectStore(Protocol):
    """Async object-store contract; the adapter owns bucket and client configuration."""

    async def put(
        self,
        key: str,
        body: UploadBody,
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        """Create or replace an object and return its normalized metadata."""

    async def get(self, key: str) -> StoredObject | None:
        """Return an object, or ``None`` when the key is absent."""

    async def delete(self, key: str) -> bool:
        """Delete an object and report whether it existed."""

    async def list_page(
        self,
        prefix: str = "",
        *,
        limit: int = 100,
        continuation_token: str | None = None,
    ) -> ObjectPage:
        """Return a bounded page; tokens are opaque and specific to one adapter/bucket."""

    async def aclose(self) -> None:
        """Release resources owned by this store; repeated calls should be safe."""


__all__ = [
    "OBJECT_STORE_DEPENDENCY_KEY",
    "ObjectInfo",
    "ObjectPage",
    "ObjectReader",
    "ObjectStore",
    "ListObjectsRequest",
    "StorageConfigurationError",
    "StorageError",
    "StorageOperationError",
    "StoredObject",
    "UploadBody",
    "validate_object_key",
]
