import hashlib
from typing import TYPE_CHECKING, cast
from unittest import IsolatedAsyncioTestCase, TestCase

from botocore.exceptions import ClientError
from vintasend.services.attachment_managers.asyncio_base import AsyncIOBaseAttachmentManager
from vintasend.services.attachment_managers.base import BaseAttachmentManager

from vintasend_s3_attachments.attachment_manager import (
    S3AsyncIOAttachmentManager,
    S3AttachmentManager,
    S3StoredFile,
)


if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class FakeS3Client:
    """In-memory stand-in for a boto3 S3 client covering the calls the managers make.

    Objects live in a ``{(bucket, key): {...}}`` dict. ``get_object`` raises the same
    ``ClientError`` shape (``Error.Code == "NoSuchKey"``) boto3 raises for a missing key, so the
    ``read`` -> ``FileNotFoundError`` translation is exercised for real.
    """

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType}
        return {}

    def get_object(self, *, Bucket, Key):  # noqa: N803
        try:
            stored = self.objects[(Bucket, Key)]
        except KeyError:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
                "GetObject",
            ) from None
        return {"Body": _Body(stored["Body"]), "ContentType": stored["ContentType"]}

    def delete_object(self, *, Bucket, Key):  # noqa: N803
        self.objects.pop((Bucket, Key), None)
        return {}

    def generate_presigned_url(self, operation, *, Params, ExpiresIn):  # noqa: N803
        return (
            f"https://{Params['Bucket']}.s3.amazonaws.com/{Params['Key']}"
            f"?op={operation}&expires_in={ExpiresIn}"
        )


class _Body:
    """Minimal StreamingBody stand-in: a ``read()`` that returns the stored bytes."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class S3AttachmentManagerTestCase(TestCase):
    def setUp(self) -> None:
        self.client = FakeS3Client()
        self.manager = S3AttachmentManager(
            "my-bucket", client=cast("S3Client", self.client), prefix="attachments"
        )

    def test_subclasses_the_real_abc(self):
        assert issubclass(S3AttachmentManager, BaseAttachmentManager)

    def test_no_abstract_methods_remain_unimplemented(self):
        assert S3AttachmentManager.__abstractmethods__ == frozenset()

    def test_upload_puts_object_and_returns_record(self):
        data = b"%PDF-1.4 fake invoice bytes"
        record = self.manager.upload_file(file=data, filename="invoice.pdf")

        assert record.filename == "invoice.pdf"
        assert record.content_type == "application/pdf"
        assert record.size == len(data)
        assert record.checksum == hashlib.sha256(data).hexdigest()

        identifiers = record.storage_identifiers
        assert identifiers["id"] == identifiers["key"]
        assert identifiers["bucket"] == "my-bucket"
        assert identifiers["key"].startswith("attachments/")
        assert identifiers["key"].endswith("_invoice.pdf")
        # The bytes really landed in the (fake) bucket under that key.
        assert self.client.objects[("my-bucket", identifiers["key"])]["Body"] == data

    def test_explicit_content_type_is_respected(self):
        record = self.manager.upload_file(
            file=b"data", filename="thing.bin", content_type="application/x-custom"
        )
        assert record.content_type == "application/x-custom"

    def test_reconstruct_then_read_round_trips(self):
        data = b"hello s3"
        record = self.manager.upload_file(file=data, filename="note.txt")
        handle = self.manager.reconstruct_attachment_file(record.storage_identifiers)
        assert isinstance(handle, S3StoredFile)
        assert handle.read() == data
        assert handle.stream().read() == data

    def test_reconstruct_read_missing_object_raises_file_not_found(self):
        handle = self.manager.reconstruct_attachment_file(
            {"id": "attachments/missing", "bucket": "my-bucket", "key": "attachments/missing"}
        )
        with self.assertRaises(FileNotFoundError):
            handle.read()

    def test_url_is_presigned_for_the_stored_object(self):
        record = self.manager.upload_file(file=b"data", filename="note.txt")
        handle = self.manager.reconstruct_attachment_file(record.storage_identifiers)
        url = handle.url(expires_in=120)
        assert record.storage_identifiers["key"] in url
        assert "expires_in=120" in url

    def test_delete_by_identifiers_removes_the_object(self):
        record = self.manager.upload_file(file=b"data", filename="note.txt")
        key = record.storage_identifiers["key"]
        assert ("my-bucket", key) in self.client.objects

        self.manager.delete_file_by_identifiers(record.storage_identifiers)
        assert ("my-bucket", key) not in self.client.objects

    def test_reconstruct_without_key_or_id_raises(self):
        from vintasend.exceptions import UnsupportedAttachmentFileTypeError

        with self.assertRaises(UnsupportedAttachmentFileTypeError):
            self.manager.reconstruct_attachment_file({"bucket": "my-bucket"})

    def test_bucket_falls_back_to_manager_when_missing_from_identifiers(self):
        data = b"legacy record with no bucket key"
        record = self.manager.upload_file(file=data, filename="legacy.txt")
        # A record written before the "bucket" key existed still resolves against the manager's
        # own configured bucket.
        legacy = {"id": record.storage_identifiers["key"]}
        handle = self.manager.reconstruct_attachment_file(legacy)
        assert handle.read() == data


class S3AsyncIOAttachmentManagerTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = FakeS3Client()
        self.manager = S3AsyncIOAttachmentManager(
            "my-bucket", client=cast("S3Client", self.client)
        )

    def test_subclasses_the_real_abc(self):
        assert issubclass(S3AsyncIOAttachmentManager, AsyncIOBaseAttachmentManager)

    def test_no_abstract_methods_remain_unimplemented(self):
        assert S3AsyncIOAttachmentManager.__abstractmethods__ == frozenset()

    async def test_upload_reconstruct_and_delete_round_trip(self):
        data = b"async bytes"
        record = await self.manager.upload_file(file=data, filename="async.txt")
        assert record.size == len(data)
        assert record.storage_identifiers["bucket"] == "my-bucket"

        handle = self.manager.reconstruct_attachment_file(record.storage_identifiers)
        assert handle.read() == data

        await self.manager.delete_file_by_identifiers(record.storage_identifiers)
        assert self.client.objects == {}

    async def test_reconstruct_stays_synchronous(self):
        record = await self.manager.upload_file(file=b"data", filename="note.txt")
        # reconstruct_attachment_file is a plain def even here -- it returns a handle, not a
        # coroutine to await.
        handle = self.manager.reconstruct_attachment_file(record.storage_identifiers)
        assert isinstance(handle, S3StoredFile)
