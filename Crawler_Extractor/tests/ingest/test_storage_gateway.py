from src.ingest.storage_gateway import StorageGateway


class _Bucket:
    def __init__(self) -> None:
        self.uploads = []

    def upload(self, key, data, options):  # pragma: no cover - exercised via gateway
        self.uploads.append((key, data, options))
        return {"path": key}


class _Client:
    def __init__(self) -> None:
        self.storage = self
        self.bucket = _Bucket()
        self.last_bucket_name = ""

    def from_(self, name):
        self.last_bucket_name = name
        return self.bucket


def _setup_gateway():
    client = _Client()
    gateway = StorageGateway(client, "pdfs", "texts")
    return client, gateway


def test_store_pdf_uses_pdf_bucket_with_content_type():
    client, gateway = _setup_gateway()
    ref = gateway.store_pdf("paper", "doc.pdf", b"payload")
    bucket = client.bucket
    assert client.last_bucket_name == "pdfs"
    key, data, options = bucket.uploads[0]
    assert key.endswith("doc.pdf")
    assert data == b"payload"
    assert options["content-type"] == "application/pdf"
    assert options["cache-control"] == "3600"
    assert ref.uri.startswith("storage://pdfs/")


def test_store_text_uses_text_bucket_with_content_type():
    client, gateway = _setup_gateway()
    ref = gateway.store_text("paper", "contents")
    bucket = client.bucket
    assert client.last_bucket_name == "texts"
    key, data, options = bucket.uploads[-1]
    assert key.endswith("extracted.txt")
    assert data == b"contents"
    assert options["content-type"] == "text/plain"
    assert options["cache-control"] == "3600"
    assert ref.uri.startswith("storage://texts/")
