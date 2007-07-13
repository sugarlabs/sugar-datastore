import tempfile
import os

def tmpData(data):
    """Put data into a temporary file returning the filename """
    fd, fn = tempfile.mkstemp()
    os.write(fd, data)
    os.close(fd)
    return fn

def waitforindex(obj):
    # wait for any/all index managers associated with object to finish
    # indexing so that tests can do there thing
    obj.complete_indexing()
