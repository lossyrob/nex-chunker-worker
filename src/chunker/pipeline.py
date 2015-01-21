"""
Code for chunking up NEX climate data. Performs the following pipeline:

1. Read the file netCDF file from S3 when a path pops in the sqs queue onto the local file system.
2. Tile that netCDF.
3. Upload to the appropriate bucket.

Note: time units is "days since 1950-01-01 00:00:00"
"""
from util import *
import os, re, shutil
import tempfile
from datetime import datetime, timedelta
import tiler
from boto.s3.connection import S3Connection    
from boto.s3.key import Key

base_time = datetime(1950, 01, 01, 0, 0)
target_cols = 512
target_rows = 512

def parse_filename(path):
    name = os.path.splitext(os.path.basename(path))[0]
    m = re.match("""(?P<datatype>[^_]+)_amon_BCSD_(?P<context>[^_]+)_r1i1p1_CONUS_(?P<model>[^_]+)\_""", name)
    if m:
        datatype = m.group("datatype")
        context = m.group("context")
        model = m.group("model")
        return (datatype, context, model)

    m = re.match("""(?P<datatype>[^_]+)_quartile75_amon_(?P<context>[^_]+)_CONUS_.*""", name)
    if m:
        datatype = m.group("datatype")
        context = m.group("context")
        model = "ensemble"
        return (datatype, context, model)

    log("Could not parse name " + name)
    return None

def read_from_s3(s3path):
    m = re.match("""s3://([^/]+)/(.+)""", s3path)
    if m:
        bucket_name = m.group(1)
        key_name = m.group(2)
        conn = S3Connection()
        bucket = conn.get_bucket(bucket_name)
        key = bucket.get_key(key_name)
        (handle, file_path) = tempfile.mkstemp(suffix='.nc')
        log("Saving to " + file_path)
        with os.fdopen(handle, 'wb') as tmp:
            key.get_file(tmp)
        return (key.name, file_path)
    else:
        log("ERROR: cannot parse s3key " + s3path)
        return None

def upload_to_s3(tile_dir, datatype, context, model, target_bucket):
    conn = S3Connection()
    bucket = conn.get_bucket(target_bucket)
    for f in os.listdir(tile_dir):
        path = os.path.join(tile_dir, f)
        key = Key(bucket)
        key.key = "%s/%s/%s/%s" % (context, datatype, model, f)
        log("Uploading %s to %s" % (f, key.key))
        key.set_contents_from_filename(path)

def process_path(s3path, target_bucket):
    (s3key, path) = read_from_s3(s3path)
    
    try:
        parsed = parse_filename(s3key)
        if parsed:
            (datatype, context, model) = parsed
            tempdir = tempfile.mkdtemp()
            log("Tiling to " + tempdir)
            tiler.tile(path, s3key, tempdir)
            try:
                upload_to_s3(tempdir, datatype, context, model, target_bucket)
            finally:
                log("Deleting directory " + tempdir)
                shutil.rmtree(tempdir)
    finally:
        log("Deleting " + path)
        os.remove(path)
