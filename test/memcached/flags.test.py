import os
import sys
import inspect

sys.path.append(os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0))))

from mclient import MemcachedConnection

port = int(iproto.uri.split(':')[1]) + 3
mc_client = MemcachedConnection('localhost', port)

flags_list = [0x0, 0x7b, 0xffff]

for flags in flags_list:
    mc_client("set foo %d 0 6\r\nfooval\r\n" % flags)
    result = mc_client("gets foo\r\n")
    ret_flags = int(result.split()[2])
    if flags == ret_flags:
        print "success: flags (0x%x) == ret_flags (0x%x)" % (flags, ret_flags)
    else:
        print "fail: flags (0x%x) != ret_flags (0x%x)" % (flags, ret_flags)
