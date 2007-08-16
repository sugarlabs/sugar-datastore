""" 
simplified xattr
~~~~~~~~~~~~~~~~~~~~
automatically manage prefixes into the xattr namespace

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'


import xattr

class Xattr(object):
    def __init__(self, filename, prefix, implicitUser=True):
        self.filename = filename
        self.prefix=[]
        if implicitUser: self.prefix.append('user')
        self.prefix.append(prefix)
        self.ns = '.'.join(self.prefix)
        self.keyed = lambda x: '.'.join((self.ns, x))
        
    def __getitem__(self, key):
        v = xattr.getxattr(self.filename, self.keyed(key))
        return v.decode('utf-8')

    def __setitem__(self, key, value):
        if isinstance(value, unicode):
            value = value.encode("utf-8")
        else:
            value = str(value)
        xattr.setxattr(self.filename, self.keyed(key), value)

    def __delitem__(self, key):
        xattr.removexattr(self.filename, self.keyed(key))

    def get(self, key, default=None):
        try:
            return self[key]
        except IOError:
            return default
        
    def iterkeys(self):
        all = xattr.listxattr(self.filename)
        for key in all:
            if key.startswith(self.ns):
                yield key[len(self.ns) + 1:]
            
    def keys(self):
        return list(self.iterkeys())
    
            
