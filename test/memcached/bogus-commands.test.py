import os
import sys
import inspect

sys.path.append(os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda:0))))

from mclient import MemcachedConnection

port = int(iproto.uri.split(':')[1]) + 3
mc_client = MemcachedConnection('localhost', port)

mc_client("boguscommand slkdsldkfjsd\r\n")
