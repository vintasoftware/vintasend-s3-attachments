# vintasend-s3-attachments

An AWS S3 [attachment manager](https://github.com/vintasoftware/vintasend) for
[vintasend](https://github.com/vintasoftware/vintasend), backed by
[boto3](https://boto3.amazonaws.com/).

VintaSend splits an attachment into two things: the bytes, owned by an **attachment manager**,
and a row describing them, owned by a **notification backend**. This package is the manager half:
it stores every uploaded file as an object in an S3 bucket and hands the backend an opaque
`storage_identifiers` dict so the backend never has to talk to S3 itself. Pair it with any
vintasend backend that supports attachments (`vintasend-django`, `vintasend-sqlalchemy`, ...).

## Install

```bash
poetry add vintasend-s3-attachments
# or
pip install vintasend-s3-attachments
```

`boto3` comes as a dependency. AWS credentials and region are resolved the usual boto3 way
(environment variables, `~/.aws/`, an instance role, ...), unless you inject a preconfigured
client.

## Usage

```python
from vintasend.services.notification_service import NotificationService
from vintasend_s3_attachments import S3AttachmentManager

service = NotificationService(
    notification_adapters=[...],
    notification_backend=my_backend,           # any attachment-aware vintasend backend
    attachment_manager=S3AttachmentManager(
        bucket="my-notification-attachments",
        prefix="attachments",                  # optional key prefix ("folder")
    ),
)
```

Instead of passing an instance, you can point the `NOTIFICATION_ATTACHMENT_MANAGER` setting at a
dotted path and let the service resolve it.

### Configuring the S3 client

The manager builds `boto3.client("s3")` lazily on first use. Steer that with `client_kwargs`:

```python
S3AttachmentManager(
    bucket="my-bucket",
    client_kwargs={"region_name": "eu-west-1", "endpoint_url": "https://minio.local"},
)
```

Or inject a client you already built (a shared session, a MinIO/LocalStack endpoint, a custom
retry config, and so on):

```python
import boto3

S3AttachmentManager(bucket="my-bucket", client=boto3.client("s3"))
```

### AsyncIO

`S3AsyncIOAttachmentManager` has the same constructor and mirrors the sync class for
`AsyncIONotificationService`. boto3 is synchronous, so `upload_file` /
`delete_file_by_identifiers` are `async def` that wrap boto3's blocking calls;
`reconstruct_attachment_file` stays a plain method (it only builds a lazy handle, doing no I/O),
matching the base seam.

## How storage identifiers work

`upload_file` writes the object under a collision-free key (`<prefix>/<uuid>_<filename>`) and
returns an `AttachmentFileRecord` whose `storage_identifiers` is:

```python
{"id": key, "bucket": bucket, "key": key}
```

`id` is the required, non-empty key every manager must provide; `bucket` and `key` are what this
manager reads back in `reconstruct_attachment_file` / `delete_file_by_identifiers`. A record
written without a `bucket` key (a legacy row) still resolves against the manager's own configured
bucket.

The handle returned by `reconstruct_attachment_file` is an `S3StoredFile`:

- `read()` / `stream()` fetch the object (a missing object raises `FileNotFoundError`),
- `url(expires_in=3600)` returns a **presigned GET URL**,
- `delete()` removes the object.

## Reclaiming orphaned files

VintaSend never deletes stored bytes on its own. To reclaim files no notification references
anymore, drive the backend's orphan query and this manager together in a periodic task of your
own:

```python
for record in backend.get_orphaned_attachment_files():
    manager.delete_file_by_identifiers(record.storage_identifiers)  # delete the S3 object
    backend.delete_attachment_file(record.id)                       # drop the row
```

## Development

```bash
poetry install
poetry run pytest   # tests run fully offline against an in-memory fake S3 client
poetry run ruff check
poetry run mypy
```

The test suite injects a fake boto3-style client, so no AWS account, credentials, or network
access are needed to run it.
