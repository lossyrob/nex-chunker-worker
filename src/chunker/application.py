from util import log

import pipeline, tiler
import re, os, sys, time
import boto.sqs

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

if __name__ == '__main__':
    q_name = os.environ['CHUNKER_QUEUE']
    target_bucket = os.environ['CHUNKER_TARGET_BUCKET']

    log("KEY: " + os.environ['AWS_ACCESS_KEY_ID'])
    log("SECRET: " + os.environ['AWS_SECRET_ACCESS_KEY'])

    log("TARGET QUEUE: " + q_name)
    log("TARGET BUCKET: " + target_bucket)

    conn = boto.sqs.connect_to_region("us-east-1")
    q = conn.get_queue(q_name)

    miss_count = 0
    while True:
        m = q.read()
        if m:
            s3key = m.get_body()
            log("GOT A MESSAGE! " + s3key)
            sys.stdout.flush()
            try:
                print "Processing path " + s3key
                sys.stdout.flush()
                pipeline.process_path(s3key, target_bucket)
                q.delete_message(m)
                print "Finished!" 
                sys.stdout.flush()
            except:
                print "ERROR: Could not process path " + s3key
                sys.stdout.flush()
        else:
            miss_count += 1
            if miss_count > 10:
                log("WARN: No messages in a while!")
                miss_count = 0
            time.sleep(10)
