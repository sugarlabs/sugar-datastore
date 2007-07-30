import tempfile
import os

def tmpData(data):
    """Put data into a temporary file returning the filename """
    fd, fn = tempfile.mkstemp()
    os.write(fd, data)
    os.close(fd)
    return fn

# Search result set handlers
def expect(r, count=None):
   if count: assert r[1] == count
   return list(r[0])
def expect_single(r):
   assert r[1] == 1
   return r[0].next()
def expect_none(r):
   assert r[1] == 0
   assert list(r[0]) == []
