import tempfile
import os
import time

from olpc.datastore.xapianindex import IndexManager
from olpc.datastore.datastore import DataStore

def tmpData(data):
    """Put data into a temporary file returning the filename """
    fd, fn = tempfile.mkstemp()
    os.write(fd, data)
    os.close(fd)
    return fn

def waitforindex(obj, interval=0.1):
    # wait for any/all index managers associated with object to finish
    # indexing so that tests can do there thing
    if isinstance(obj, IndexManager):
        obj.complete_indexing()
    elif isinstance(obj, DataStore):
        for mp in obj.mountpoints.values():
            im = mp.indexmanager
            im.complete_indexing()
    else:
        # backingstore
        obj.indexmanager.complete_indexing()
        
