import tempfile
import os

def tmpData(data):
    """Put data into a temporary file returning the filename """
    fd, fn = tempfile.mkstemp()
    os.write(fd, data)
    os.close(fd)
    return fn

