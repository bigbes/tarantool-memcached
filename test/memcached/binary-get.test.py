import os
import sys
import inspect

sys.path.append(os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0))))

from mclient import MemcachedConnection

hexify = (lambda istr: "\\x".join("{:02x}".format(ord(c)) for c in istr))

blobs_list = [ "mooo\0", "mumble\0\0\0\0\r\rblarg", "\0", "\r" ]

port = int(iproto.uri.split(':')[1]) + 3
mc_client = MemcachedConnection('localhost', port)

for i in range(len(blobs_list)):
    key = "foo_%d" % i
    blob = blobs_list[i]
    blob_len = len(blob)

    mc_client("set %s 0 0 %d\r\n%s\r\n" % (key, blob_len, blob))
    mc_client("get %s\r\n" % key)
