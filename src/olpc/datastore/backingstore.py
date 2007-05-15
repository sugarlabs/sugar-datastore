""" 
olpc.datastore.backingstore
~~~~~~~~~~~~~~~~~~~~~~~~~~~
management of stable storage for the datastore

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'

import sha
import os
import subprocess

class BackingStore(object):
    """Backing stores manage stable storage. We abstract out the
    management of file/blob storage through this class, as well as the
    location of the backing store itself (it may be done via a network
    connection for example).

    While the backingstore is responsible for implementing the
    metadata interface no implementation is provided here. It is
    assumed by that interface that all the features enumerated in
    olpc.datastore.model are provided.
    
    """
    def __init__(self, uri, **kwargs):
        """The kwargs are used to configure the backend so it can
        provide its interface. See specific backends for details
        """
        pass

    def connect(self):
        """connect to the metadata store"""
        self._connect()

        
    def prepare(self, datastore, querymanager):
        """Verify the backingstore is ready to begin its duties"""
        return False




class FileBackingStore(BackingStore):
    """ A backing store that directs maps the storage of content
    objects to an available filesystem.


    # not really true, the change would result in the older version
    having the last content and the new version as well. The old one
    is left in the newest start rather than start state. if that can work...
    The semantics of interacting
    with the repositories mean that the current copy _can_ be edited
    in place. Its the actions create/update that create new revisions
    in the datastore and hence new versions.
    """
    
    def __init__(self, uri, **kwargs):
        """ FileSystemStore(path=<root of managed storage>)
        """
        self.base = uri
        super(FileBackingStore, self).__init__(uri, **kwargs)
        self.options = kwargs
        
    def prepare(self, datastore, querymanager):
        if not os.path.exists(self.base):
            os.makedirs(self.base)
        self.datastore = datastore
        self.querymanager = querymanager
        return True

    def _translatePath(self, uid):
        """translate a UID to a path name"""
        return os.path.join(self.base, str(uid))

    def create(self, content, filelike):
        self._writeContent(content.id, filelike, replace=False)
        
    
    def get(self, uid, env=None, allowMissing=False):
        path = self._translatePath(uid)
        if not os.path.exists(path):
            raise KeyError("object for uid:%s missing" % uid)            
        else:
            fp = open(path, 'r')
            # now return a Content object from the model associated with
            # this file object
        return self._mapContent(uid, fp, path, env)

    def set(self, uid, filelike):
        self._writeContent(uid, filelike)

    def delete(self, uid, allowMissing=False):
        path = self._translatePath(uid)
        if os.path.exists(path):
            os.unlink(path)
        else:
            if not allowMissing:
                raise KeyError("object for uid:%s missing" % uid)            
        
    def _targetFile(self, uid, fp, path, env):
        targetpath = os.path.join('/tmp/', path.replace('/', '_'))
        if subprocess.call(['cp', path, targetpath]):
            raise OSError("unable to create working copy")
        return open(targetpath, 'rw')
            
    def _mapContent(self, uid, fp, path, env=None):
        """map a content object and the file in the repository to a
        working copy.
        """
        content = self.querymanager.get(uid)
        # we need to map a copy of the content from the backingstore into the
        # activities addressable space.
        # map this to a rw file
        targetfile = self._targetFile(uid, fp, path, env)
        content.file = targetfile
        
        if self.options.get('verify', False):
            c  = sha.sha()
            for line in targetfile:
                c.update(line)
            fp.seek(0)
            if c.hexdigest() != content.checksum:
                raise ValueError("Content for %s corrupt" % uid)
        return content

    def _writeContent(self, uid, filelike, replace=True):
        content = self.querymanager.get(uid)
        path = self._translatePath(content.id)
        if replace is False and os.path.exists(path):
            raise KeyError("objects with path:%s for uid:%s exists" %(
                            path, content.id))
        fp = open(path, 'w')
        c  = sha.sha()
        filelike.seek(0)
        for line in filelike:
            c.update(line)
            fp.write(line)
        fp.close()
        content.checksum = c.hexdigest()
        
    
    
