import os
import sys
import inspect

sys.path.append(os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0))))

from mclient import MemcachedConnection

port = int(iproto.uri.split(':')[1]) + 3
mc_client = MemcachedConnection('localhost', port)

import time
import yaml

###################################
def get_memcached_len(serv):
    resp = server.admin("mc_inst.mcs:len()", silent=True)
    return yaml.load(resp)[0]

def wait_for_empty_space(serv = server):
    serv_admin = serv.admin
    while True:
        if get_memcached_len(serv) == 0:
            return
        time.sleep(0.01)
###################################

print """# Test flush_all with zero delay. """
mc_client("set foo 0 0 6\r\nfooval\r\n")
mc_client("get foo\r\n")
mc_client("flush_all\r\n")
mc_client("get foo\r\n")

print """# check that flush_all doesn't blow away items that immediately get set """
mc_client("set foo 0 0 3\r\nnew\r\n")
mc_client("get foo\r\n")

print """# and the other form, specifying a flush_all time... """
expire = time.time() + 1
print "flush_all time + 1"
print mc_client("flush_all %d\r\n" % expire, silent=True)
mc_client("get foo\r\n")

mc_client("set foo 0 0 3\r\n123\r\n")
mc_client("get foo\r\n")
wait_for_empty_space()
mc_client("get foo\r\n")
