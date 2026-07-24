"""AWS S3 attachment managers, backed by boto3.

The notification backend never touches a byte itself: it persists ``AttachmentFileRecord`` /
``NotificationAttachment`` rows and hands the opaque ``storage_identifiers`` back to whichever
manager was injected. These managers own the bytes, storing each uploaded file as an object in
an S3 bucket under a generated, collision-free key.

``storage_identifiers`` always carries a non-empty ``id`` (the object key) plus the ``bucket``
and ``key`` used to write it, so a handle can be rebuilt even when a differently-configured
manager instance reconstructs it later.

boto3 is synchronous, so the AsyncIO manager wraps the same blocking calls its sync twin makes.
This matches the library's other AsyncIO seams (the local-filesystem manager does the same) --
there is no async S3 client in this dependency set to await against.
"""

import datetime
import io
import posixpath
import uuid
from typing import TYPE_CHECKING, Any, BinaryIO

from vintasend.exceptions import UnsupportedAttachmentFileTypeError
from vintasend.services.attachment_managers.asyncio_base import AsyncIOBaseAttachmentManager
from vintasend.services.attachment_managers.base import BaseAttachmentManager
from vintasend.services.dataclasses import (
    AttachmentFile,
    AttachmentFileRecord,
    FileAttachment,
    StorageIdentifiers,
)


if TYPE_CHECKING:
    # Typed only for readers/IDE; the real client is duck-typed at runtime so any boto3-style
    # S3 client (or a test double) works without a hard dependency on boto3-stubs.
    from mypy_boto3_s3 import S3Client
else:
    S3Client = Any


def _now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def _resolve(storage_identifiers: StorageIdentifiers, fallback_bucket: str) -> tuple[str, str]:
    """Pull ``(bucket, key)`` out of ``storage_identifiers``.

    ``key`` falls back to the required ``id`` key; ``bucket`` falls back to the manager's own
    configured bucket, so a record written before the ``bucket`` key existed still resolves.
    """
    key = storage_identifiers.get("key") or storage_identifiers.get("id")
    if not key:
        raise UnsupportedAttachmentFileTypeError(
            "storage_identifiers must carry a non-empty 'key' or 'id'"
        )
    bucket = storage_identifiers.get("bucket") or fallback_bucket
    return bucket, str(key)


def _is_not_found(error: Exception) -> bool:
    """True when a botocore ``ClientError`` means "no such object"."""
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return False
    code = response.get("Error", {}).get("Code")
    return code in {"NoSuchKey", "404", "NotFound"}


class S3StoredFile(AttachmentFile):
    """Lazy handle to an object stored in S3.

    Built with no I/O by ``reconstruct_attachment_file``; the object is only fetched when
    ``read``/``stream``/``url``/``delete`` is called.
    """

    def __init__(self, client: S3Client, bucket: str, key: str):
        self._client = client
        self._bucket = bucket
        self._key = key

    def read(self) -> bytes:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._key)
        except Exception as e:
            if _is_not_found(e):
                raise FileNotFoundError(
                    f"No object stored at s3://{self._bucket}/{self._key}"
                ) from e
            raise
        body = response["Body"].read()
        return body if isinstance(body, bytes) else bytes(body)

    def stream(self) -> BinaryIO:
        return io.BytesIO(self.read())

    def url(self, expires_in: int = 3600) -> str:
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": self._key},
            ExpiresIn=expires_in,
        )

    def delete(self) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._key)


class _S3ManagerMixin:
    """Shared upload/reconstruct/delete mechanics for both the sync and async managers."""

    bucket: str
    prefix: str
    _client: S3Client | None
    _client_kwargs: dict[str, Any]

    @property
    def client(self) -> S3Client:
        """The boto3 S3 client, built lazily from ``client_kwargs`` on first use.

        Building it lazily keeps a manager instantiable (and its non-network methods testable)
        without live AWS credentials, and lets a caller inject a preconfigured client instead.
        """
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", **self._client_kwargs)
        return self._client

    def _build_key(self, filename: str) -> str:
        # Prefix with a uuid so two files that share a filename never collide, then nest under
        # the configured prefix ("folder") if one was given.
        name = f"{uuid.uuid4().hex}_{filename}"
        return posixpath.join(self.prefix, name) if self.prefix else name

    def _put(
        self, data: bytes, filename: str, content_type: str | None
    ) -> AttachmentFileRecord:
        content_type = content_type or self.detect_content_type(filename)  # type: ignore[attr-defined]
        key = self._build_key(filename)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        now = _now()
        return AttachmentFileRecord(
            id=str(uuid.uuid4()),
            filename=filename,
            content_type=content_type,
            size=len(data),
            checksum=self.calculate_checksum(data),  # type: ignore[attr-defined]
            created_at=now,
            updated_at=now,
            storage_identifiers={"id": key, "bucket": self.bucket, "key": key},
        )

    def reconstruct_attachment_file(
        self, storage_identifiers: StorageIdentifiers
    ) -> AttachmentFile:
        bucket, key = _resolve(storage_identifiers, self.bucket)
        return S3StoredFile(self.client, bucket, key)

    def _delete(self, storage_identifiers: StorageIdentifiers) -> None:
        bucket, key = _resolve(storage_identifiers, self.bucket)
        self.client.delete_object(Bucket=bucket, Key=key)


class S3AttachmentManager(_S3ManagerMixin, BaseAttachmentManager):
    """Sync attachment manager that stores every byte as an object in an S3 bucket.

    Pass a preconfigured boto3 S3 ``client`` to reuse an existing session/credentials, or let
    the manager build one from ``client_kwargs`` (forwarded to ``boto3.client("s3", ...)``) on
    first use. ``prefix`` nests every object under a common key prefix ("folder").
    """

    def __init__(
        self,
        bucket: str,
        *,
        client: S3Client | None = None,
        prefix: str = "",
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self._client = client
        self._client_kwargs = client_kwargs or {}

    def upload_file(
        self,
        file: FileAttachment,
        filename: str,
        content_type: str | None = None,
    ) -> AttachmentFileRecord:
        return self._put(self.file_to_bytes(file), filename, content_type)

    def delete_file_by_identifiers(self, storage_identifiers: StorageIdentifiers) -> None:
        self._delete(storage_identifiers)


class S3AsyncIOAttachmentManager(_S3ManagerMixin, AsyncIOBaseAttachmentManager):
    """AsyncIO attachment manager backed by S3.

    ``reconstruct_attachment_file`` stays synchronous (it only builds a lazy handle). boto3's
    calls in ``upload_file`` / ``delete_file_by_identifiers`` are blocking; they are wrapped in
    ``async def`` to match the AsyncIO seam, the same way the library's other AsyncIO managers
    wrap otherwise-blocking work.
    """

    def __init__(
        self,
        bucket: str,
        *,
        client: S3Client | None = None,
        prefix: str = "",
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self._client = client
        self._client_kwargs = client_kwargs or {}

    async def upload_file(
        self,
        file: FileAttachment,
        filename: str,
        content_type: str | None = None,
    ) -> AttachmentFileRecord:
        return self._put(self.file_to_bytes(file), filename, content_type)

    async def delete_file_by_identifiers(self, storage_identifiers: StorageIdentifiers) -> None:
        self._delete(storage_identifiers)
