"""Writing the results out, locally or to GCS.

The output contract is with the Crash Stats frontend, not a BigQuery table, so the
filenames, the gzip and the upload metadata are all load-bearing. See README.md.

Local and remote writes produce byte-identical files, which is the point: a local run
is a real rehearsal of what would be uploaded, and render_frontend.py reads either.
"""

import gzip
import hashlib
import io
import json
import pathlib
import shutil


# The job name is the first path segment under the bucket, and the frontend's URL is
# built from it, so it isn't configurable.
JOB_NAME = "top-signatures-correlations"

CONTENT_TYPE = "application/json"
CONTENT_ENCODING = "gzip"


def signature_filename(signature):
    """sha1 of the signature, which is how the frontend addresses these.

    correlation.js hashes the signature client side and fetches
    <channel>/<sha1>.json.gz, so this has to stay sha1 of the UTF-8 bytes.
    """
    return hashlib.sha1(signature.encode("utf-8")).hexdigest() + ".json.gz"


def encode(payload):
    """Serialise to the exact bytes that get stored.

    Separators without spaces because that's what json.dump produces by default and
    what the current output has; mtime zeroed so the same input gives the same bytes,
    which makes a byte diff between two runs meaningful.
    """
    raw = json.dumps(payload).encode("utf-8")
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", compresslevel=9, mtime=0) as handle:
        handle.write(raw)
    return out.getvalue()


def addon_related(results, totals_by_signature, total_reference):
    """Signatures whose addon correlations are over-represented.

    Ported from the driver's addon_related_signatures loop. A signature qualifies when
    it has at least one single-item result whose label mentions an addon and whose
    support in the signature exceeds its support in the channel.

    Note this has always been empty in production, because upstream's addon pass got a
    Row where it expected a string and dropped every addon. Counting addons in SQL here
    avoids that, so this file becomes populated for the first time.
    """
    entries = []
    for signature, rows in results.items():
        total_group = totals_by_signature.get(signature, 0)
        if not total_group:
            continue

        addons = [
            row
            for row in rows
            if len(row["item"]) == 1
            and any("Addon" in label for label in row["item"])
            and row["count_group"] / total_group
            > row["count_reference"] / total_reference
        ]
        if addons:
            entries.append(
                {
                    "signature": signature,
                    "addons": addons,
                    "total": total_group,
                }
            )
    return entries


class Writer:
    """Collects the output files, then flushes them locally and/or to GCS."""

    def __init__(self, directory, bucket=None, gcs_client=None):
        self.directory = pathlib.Path(directory)
        self.bucket = bucket
        self.gcs_client = gcs_client
        self.files = {}

    def add(self, relative_path, payload):
        """Queue one file. relative_path is under <job>/data/."""
        self.files[relative_path] = encode(payload)

    def add_signature(self, channel, signature, total, rows):
        self.add(
            f"{channel}/{signature_filename(signature)}",
            {"total": total, "results": rows},
        )

    def write_local(self):
        """Write everything under the output directory.

        The directory is cleared first, because the job's contract is that the output
        is the complete current state, and a stale file from a previous run with
        different signatures would look like a live result.
        """
        if self.directory.exists():
            shutil.rmtree(self.directory)
        for relative_path, blob in sorted(self.files.items()):
            path = self.directory / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(blob)
        return len(self.files)

    def upload(self):
        """Replace the bucket's contents with these files.

        Deletes the existing prefix first, matching the job's behaviour: signatures
        that dropped out of the top 200 have to disappear, or the frontend serves data
        for a signature this run didn't analyse.
        """
        if not self.bucket:
            raise ValueError("no bucket configured")

        bucket = self.gcs_client.bucket(self.bucket)
        prefix = f"{JOB_NAME}/data/"

        for blob in bucket.list_blobs(prefix=prefix):
            blob.delete()

        for relative_path, blob_bytes in sorted(self.files.items()):
            blob = bucket.blob(prefix + relative_path)
            blob.content_encoding = CONTENT_ENCODING
            blob.upload_from_string(blob_bytes, content_type=CONTENT_TYPE)
        return len(self.files)

    def total_bytes(self):
        return sum(len(blob) for blob in self.files.values())
