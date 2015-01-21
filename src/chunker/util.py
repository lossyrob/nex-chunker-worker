import sys, datetime

def log(msg):
    print '[%s]  -  %s' % (datetime.datetime.now().isoformat(), msg)
    sys.stdout.flush()
