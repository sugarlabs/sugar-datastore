""" 
olpc.datastore.backingstore
~~~~~~~~~~~~~~~~~~~~~~~~~~~
management of stable storage for the datastore

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'

import cPickle as pickle
import sha
import os
import re
import subprocess
import time

from olpc.datastore import query
from olpc.datastore import utils

# changing this pattern impacts _targetFile
filename_attempt_pattern = re.compile('\(\d+\)$')

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

    def __repr__(self):
        return "<%s %s: %s %s>" % (self.__class__.__name__, self.id,
                            self.title, self.uri)
    # Init phases
    @staticmethod
    def parse(uri):
        """parse the uri into an actionable mount-point.
        Returns True or False indicating if this backend handles a
        given uri.
        """
        return False

    def initialize_and_load(self):
        """phase to check the state of the located mount point, this
        method returns True (mount point is valid) or False (invalid
        or uninitialized mount point).

        self.check() which must return a boolean should check if the
        result of self.locate() is already a datastore and then
        initialize/load it according to self.options.
        
        When True self.load() is invoked.
        When False self.create() followed by self.load() is invoked.
        """
        if self.check() is False:
            self.initialize()
        self.load()

    def check(self):
        return False
    
    def load(self):
        """load the index for a given mount-point, then initialize its
        fulltext subsystem. This is the routine that will bootstrap
        the querymanager (though create() may have just created it)
        """
        pass

    def initialize(self):
        """Initialize a new mount point"""
        pass
    
    # Informational
    def descriptor(self):
        """return a dict with atleast the following keys
              'id' -- the id used to refer explicitly to the mount point
              'title' -- Human readable identifier for the mountpoint
              'uri' -- The uri which triggered the mount
        """
        pass

    @property
    def id(self): return self.descriptor()['id']
    @property
    def title(self): return self.descriptor()['title']
    

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
    STORE_NAME = "store"
    INDEX_NAME = "index"
    DESCRIPTOR_NAME = "metainfo"
    
    def __init__(self, uri, **kwargs):
        """ FileSystemStore(path=<root of managed storage>)
        """
        self.options = kwargs
        self.local_querymanager = self.options.get('local_querymanager', True)

        self.uri = uri
        self.base = os.path.join(uri, self.STORE_NAME)
        self.querymanager = None
        
    # Informational
    def descriptor(self):
        """return a dict with atleast the following keys
              'id' -- the id used to refer explicitly to the mount point
              'title' -- Human readable identifier for the mountpoint
              'uri' -- The uri which triggered the mount
        """
        # a hidden file with a pickled dict will live in the base
        # directory for each storage
        fn = os.path.join(self.base, self.DESCRIPTOR_NAME)
        if not os.path.exists(fn):
            # the data isn't there, this could happen for a number of
            # reasons (the store isn't writeable)
            desc = {'id' : self.uri,
                    'uri' : self.uri,
                    'title' : self.uri
                    }
            self.create_descriptor(**desc)
        else:
            fp = open(fn, 'r')
            desc = pickle.load(fp)
            fp.close()
            
        return desc
    
    
    def create_descriptor(self, **kwargs):
        # create the information descriptor for this store
        # defaults will be created if need be
        # passing limited values will leave existing keys in place
        fn = os.path.join(self.base, self.DESCRIPTOR_NAME)
        desc = {}
        if os.path.exists(fn):
            fp = open(fn, 'r')
            desc = pickle.load(fp)
            fp.close()
        if 'id' not in kwargs: desc['id'] = utils.create_uid()
        if 'uri' not in kwargs: desc['uri'] = self.uri
        if not kwargs.get('title', ''): desc['title'] = self.uri

        desc.update(kwargs)
        fp = open(fn, 'w')
        pickle.dump(desc, fp)
        fp.close()


    @staticmethod
    def parse(uri):
        return os.path.isabs(uri) or os.path.isdir(uri)

    def check(self):
        if not os.path.exists(self.uri): return False
        if not os.path.exists(self.base): return False
        return True
    
    def initialize(self):
        if not os.path.exists(self.base):
            os.makedirs(self.base)

        # examine options and see what the querymanager plan is
        if self.local_querymanager:
            # create a local storage using the querymanager
            # otherwise we will connect the global manager
            # in load
            index_name = os.path.join(self.base, self.INDEX_NAME)
            options = utils.options_for(self.options, 'querymanager_')
            if 'fulltext_repo' not in options:
                options['fulltext_repo'] = os.path.join(self.base,
                                                        query.DefaultQueryManager.FULLTEXT_NAME)
                
            qm = query.DefaultQueryManager(index_name, **options)
            # This will ensure the fulltext and so on are all assigned
            qm.bind_to(self)
            qm.prepare()
            self.create_descriptor(title=self.options.get('title', None))
            self.querymanager = qm
            
    def load(self):
        if not self.querymanager and self.local_querymanager:
            # create a local storage using the querymanager
            # otherwise we will connect the global manager
            # in load
            index_name = os.path.join(self.base, self.INDEX_NAME)
            if 'fulltext_repo' not in self.options:
                self.options['fulltext_repo'] = os.path.join(self.base,
                                                             query.DefaultQueryManager.FULLTEXT_NAME)

            qm = query.DefaultQueryManager(index_name, **self.options)

            # This will ensure the fulltext and so on are all assigned
            qm.bind_to(self)
            qm.prepare()

            self.querymanager = qm
            
    def bind_to(self, datastore):
        ## signal from datastore that we are being bound to it
        self.datastore = datastore

    def _translatePath(self, uid):
        """translate a UID to a path name"""
        # paths into the datastore
        return os.path.join(self.base, str(uid))

    def _targetFile(self, uid, target=None, ext=None, env=None):
        # paths out of the datastore, working copy targets
        if target: targetpath = target
        else:
            targetpath = uid.replace('/', '_').replace('.', '__')
            if ext:
                if not ext.startswith('.'): ext = ".%s" % ext
                targetpath = "%s%s" % (targetpath, ext)

        base = '/tmp'
        if env: base = env.get('cwd', base)
        
        targetpath = os.path.join(base, targetpath)
        attempt = 0
        while os.path.exists(targetpath):
            # here we look for a non-colliding name
            # this is potentially a race and so we abort after a few
            # attempts
            targetpath, ext = os.path.splitext(targetpath)
            
            if filename_attempt_pattern.search(targetpath):
                targetpath = filename_attempt_pattern.sub('', targetpath)
                
            attempt += 1
            if attempt > 9:
                targetpath = "%s(%s).%s" % (targetpath, time.time(), ext)
                break

            targetpath = "%s(%s)%s" % (targetpath, attempt, ext)

        path = self._translatePath(uid)
        if subprocess.call(['cp', path, targetpath]):
            raise OSError("unable to create working copy")
        return open(targetpath, 'rw')
            
    def _mapContent(self, uid, fp, path, env=None):
        """map a content object and the file in the repository to a
        working copy.
        """
        # env would contain things like cwd if we wanted to map to a
        # known space
        
        content = self.querymanager.get(uid)
        # we need to map a copy of the content from the backingstore into the
        # activities addressable space.
        # map this to a rw file
        if fp:
            target, ext = content.suggestName()
            targetfile = self._targetFile(uid, target, ext, env)
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
        path = self._translatePath(uid)
        if replace is False and os.path.exists(path):
            raise KeyError("objects with path:%s for uid:%s exists" %(
                            path, uid))
        fp = open(path, 'w')
        verify = self.options.get('verify', False)
        c = None
        if verify: c  = sha.sha()
        filelike.seek(0)
        for line in filelike:
            if verify:c.update(line)
            fp.write(line)
        fp.close()
        if verify:
            content = self.querymanager.get(uid)
            content.checksum = c.hexdigest()

    def _checksum(self, filename):
        c  = sha.sha()
        fp = open(filename, 'r')
        for line in fp:
            c.update(line)
        fp.close()
        return c.hexdigest()
        
    # File Management API
    def create(self, props, filelike):
        content = self.querymanager.create(props, filelike)
        filename = filelike
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            self._writeContent(content.id, filelike, replace=False)
        return content
    
    def get(self, uid, env=None, allowMissing=False):
        content = self.querymanager.get(uid)
        if not content: raise KeyError(uid)
        path = self._translatePath(uid)
        fp = None
        # not all content objects have a file
        if os.path.exists(path):
            fp = open(path, 'r')
            # now return a Content object from the model associated with
            # this file object
        return self._mapContent(uid, fp, path, env)

    def update(self, uid, props, filelike=None):
        self.querymanager.update(uid, props, filelike)
        filename = filelike
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            self.set(uid, filelike)

    def set(self, uid, filelike):
        self._writeContent(uid, filelike)

    def delete(self, uid, allowMissing=True):
        self.querymanager.delete(uid)
        path = self._translatePath(uid)
        if os.path.exists(path):
            os.unlink(path)
        else:
            if not allowMissing:
                raise KeyError("object for uid:%s missing" % uid)            
        
    

    def find(self, query):
        return self.querymanager.find(query)

    def stop(self):
        self.querymanager.stop()


class InplaceFileBackingStore(FileBackingStore):
    """Like the normal FileBackingStore this Backingstore manages the
    storage of files, but doesn't move files into a repository. There
    are no working copies. It simply adds index data through its
    querymanager and provides fulltext ontop of a regular
    filesystem. It does record its metadata relative to this mount
    point.

    This is intended for USB keys and related types of attachable
    storage.
    """

    STORE_NAME = ".olpc.store"

    def __init__(self, uri, **kwargs):
        # remove the 'inplace:' scheme
        uri = uri[len('inplace:'):]
        super(InplaceFileBackingStore, self).__init__(uri, **kwargs)
        # use the original uri
        self.uri = uri

    @staticmethod
    def parse(uri):
        return uri.startswith("inplace:")

    def check(self):
        if not os.path.exists(self.uri): return False
        if not os.path.exists(self.base): return False
        return True

        
    def load(self):
        super(InplaceFileBackingStore, self).load()
        # now map/update the existing data into the indexes
        self._walk()

    def _walk(self):
        # XXX: a version that checked xattr for uid would be simple
        # and faster
        # scan the uri for all non self.base files and update their
        # records in the db
        for dirpath, dirname, filenames in os.walk(self.uri):
            # see if there is an entry for the filename
            if self.base in dirpath: continue
            if self.STORE_NAME in dirname:
                dirname.remove(self.STORE_NAME)
                
            for fn in filenames:
                source = os.path.join(dirpath, fn)
                relative = source[len(self.uri)+1:]
                result, count = self.querymanager.find(dict(filename=relative))
                if not count:
                    # create a new record
                    self.create(dict(filename=relative), source)
                else:
                    # update the object with the new content iif the
                    # checksum is different
                    # XXX: what if there is more than one? (shouldn't happen)
                    content = result[0]
                    uid = content
                    # only if the checksum is different
                    checksum = self._checksum(source)
                    if checksum != content.checksum:
                        self.update(uid, dict(filename=relative), source)
                        
        #self.querymanager.index.flush()
                        

    # File Management API
    def create(self, props, filelike):
        # the file would have already been changed inplace
        # don't touch it
        return self.querymanager.create(props, filelike)
    
    def get(self, uid, env=None, allowMissing=False):
        content = self.querymanager.get(uid)
        if not content: raise KeyError(uid)
        return content.get_property('filename')

    def update(self, uid, props, filelike=None):
        # the file would have already been changed inplace
        # don't touch it
        self.querymanager.update(uid, props, filelike)
        
    def delete(self, uid, allowMissing=True):
        c = self.querymanager.get(uid)
        path = c.get_property('filename')
        self.querymanager.delete(uid)
        if os.path.exists(path):
            os.unlink(path)
        
