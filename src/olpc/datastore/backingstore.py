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
from datetime import datetime
import gnomevfs
import os
import re
import sha
import subprocess
import time
import threading

import dbus
import xapian

from olpc.datastore.xapianindex import IndexManager
from olpc.datastore import bin_copy
from olpc.datastore import utils

# changing this pattern impacts _targetFile
filename_attempt_pattern = re.compile('\(\d+\)$')

import logging
DS_LOG_CHANNEL = 'org.laptop.sugar.DataStore'
logger = logging.getLogger(DS_LOG_CHANNEL)
#logger.setLevel(logging.DEBUG)

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
        the indexmanager (though create() may have just created it)
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

    # Storage Translation
    def localizedName(self, uid=None, content=None, target=None):
        """taking any of uid, a content object, or a direct target
    filename (which includes all of the relative components under a
    store). Return the localized filename that should be used _within_
    the repository for the storage of this content object
    """
        pass

import time
class AsyncCopy:
    CHUNK_SIZE=65536

    def __init__(self, src, dest, completion):
        self.src = src
        self.dest = dest
        self.completion = completion
        self.src_fp = -1
        self.dest_fp = -1
        self.written = 0
        self.size = 0

    def _cleanup(self):
        os.close(self.src_fp)
        os.close(self.dest_fp)

    def _copy_block(self, user_data=None):
        try:
            data = os.read(self.src_fp, AsyncCopy.CHUNK_SIZE)
            count = os.write(self.dest_fp, data)
            self.written += len(data)

            # error writing data to file?
            if count < len(data):
                logger.debug("AC: Error writing %s -> %s: wrote less than expected" % (self.src, self.dest))
                self._cleanup()
                self.completion(RuntimeError("Error writing data to destination file"))
                return False

            # FIXME: emit progress here

            # done?
            if len(data) < AsyncCopy.CHUNK_SIZE:
                logger.debug("AC: Copied %s -> %s (%d bytes, %ds)" % (self.src, self.dest, self.written, time.time() - self.tstart))
                self._cleanup()
                self.completion(None, self.dest)
                return False
        except Exception, err:
            logger.debug("AC: Error copying %s -> %s: %r" % (self.src, self.dest, err))
            self._cleanup()
            self.completion(err)
            return False

        return True

    def start(self):
        self.src_fp = os.open(self.src, os.O_RDONLY)
        self.dest_fp = os.open(self.dest, os.O_RDWR | os.O_TRUNC | os.O_CREAT, 0644)

        stat = os.fstat(self.src_fp)
        self.size = stat[6]

        logger.debug("AC: will copy %s -> %s (%d bytes)" % (self.src, self.dest, self.size))

        self.tstart = time.time()
        import gobject
        sid = gobject.idle_add(self._copy_block)

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
        self.local_indexmanager = self.options.get('local_indexmanager', True)

        self.uri = uri
        self.base = os.path.join(uri, self.STORE_NAME)
        self.indexmanager = None
        
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
        desc = None
        if os.path.exists(fn):
            try:
                fp = open(fn, 'r')
                desc = pickle.load(fp)
                fp.close()
            except:
                desc = None
        if not desc:
            # the data isn't there, this could happen for a number of
            # reasons (the store isn't writeable)
            # or if the information on it was corrupt
            # in this case, just create a new one
            desc = {'id' : self.uri,
                    'uri' : self.uri,
                    'title' : self.uri
                    }
            self.create_descriptor(**desc)
            
        return desc
    
    
    def create_descriptor(self, **kwargs):
        # create the information descriptor for this store
        # defaults will be created if need be
        # passing limited values will leave existing keys in place
        kwargs = utils._convert(kwargs)
        fn = os.path.join(self.base, self.DESCRIPTOR_NAME)
        desc = {}
        if os.path.exists(fn):
            fp = open(fn, 'r')
            try:
                desc = pickle.load(fp)
            except:
                desc = {}
            finally:
                fp.close()

        desc.update(kwargs)
        
        if 'id' not in desc: desc['id'] = utils.create_uid()
        if 'uri' not in desc: desc['uri'] = self.uri
        if 'title' not in desc: desc['title'] = self.uri


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

        # examine options and see what the indexmanager plan is
        if self.local_indexmanager:
            # create a local storage using the indexmanager
            # otherwise we will connect the global manager
            # in load
            index_name = os.path.join(self.base, self.INDEX_NAME)
            options = utils.options_for(self.options, 'indexmanager.')
            im = IndexManager()
            # This will ensure the fulltext and so on are all assigned
            im.bind_to(self)
            im.connect(index_name, **options)

            self.create_descriptor(**options)
            self.indexmanager = im
            
    def load(self):
        if not self.indexmanager and self.local_indexmanager:
            # create a local storage using the indexmanager
            # otherwise we will connect the global manager
            # in load
            index_name = os.path.join(self.base, self.INDEX_NAME)
            options = utils.options_for(self.options, 'indexmanager.')
            im = IndexManager()

            desc = utils.options_for(self.options,
                                     'indexmanager.',
                                     invert=True)
            if desc: self.create_descriptor(**desc)
                
            # This will ensure the fulltext and so on are all assigned
            im.bind_to(self)
            im.connect(index_name)

            self.indexmanager = im
            
    def bind_to(self, datastore):
        ## signal from datastore that we are being bound to it
        self.datastore = datastore

    def localizedName(self, uid=None, content=None, target=None):
        """taking any of uid, a content object, or a direct target
    filename (which includes all of the relative components under a
    store). Return the localized filename that should be used _within_
    the repository for the storage of this content object
    """
        if target: return os.path.join(self.base, target)
        elif content:
            # see if it expects a filename
            fn, ext = content.suggestName()
            if fn: return os.path.join(self.base, fn)
            if ext: return os.path.join(self.base, "%s.%s" %
                                        (content.id, ext))
            if not uid: uid = content.id

        if uid:
            return os.path.join(self.base, uid)
        else:
            raise ValueError("""Nothing submitted to generate internal
            storage name from""")
        
    def _translatePath(self, uid):
        """translate a UID to a path name"""
        # paths into the datastore
        return os.path.join(self.base, str(uid))

    def _targetFile(self, uid, target=None, ext=None, env=None):
        # paths out of the datastore, working copy targets
        path = self._translatePath(uid)
        if not os.path.exists(path):
            return None
        
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
            
        if subprocess.call(['cp', path, targetpath]):
            raise OSError("unable to create working copy")
        return open(targetpath, 'rw')
            
    def _mapContent(self, uid, fp, path, env=None):
        """map a content object and the file in the repository to a
        working copy.
        """
        # env would contain things like cwd if we wanted to map to a
        # known space
        
        content = self.indexmanager.get(uid)
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

    def _writeContent_complete(self, path, completion=None):
        if completion is None:
            return path
        completion(None, path)
        return None

    def _writeContent(self, uid, filelike, replace=True, can_move=False, target=None,
            completion=None):
        """Returns: path of file in datastore (new path if it was copied/moved)"""
        content = None
        if target: path = target
        else:
            path = self._translatePath(uid)
            
        if replace is False and os.path.exists(path):
            raise KeyError("objects with path:%s for uid:%s exists" %(
                path, uid))

        if filelike.name != path:
            # protection on inplace stores
            if completion is None:
                bin_copy.bin_copy(filelike.name, path)
                return path

            if can_move:
                bin_copy.bin_mv(filelike.name, path)
                return self._writeContent_complete(path, completion)

            # Otherwise, async copy
            aco = AsyncCopy(filelike.name, path, completion)
            aco.start()
        else:
            return self._writeContent_complete(path, completion)

    def _checksum(self, filename):
        c  = sha.sha()
        fp = open(filename, 'r')
        for line in fp:
            c.update(line)
        fp.close()
        return c.hexdigest()
        
    # File Management API
    def _create_completion(self, uid, props, completion, exc=None, path=None):
        if exc:
            completion(exc)
            return
        try:
            # Index the content this time
            self.indexmanager.index(props, path)
            completion(None, uid)
        except Exception, exc:
            completion(exc)

    def create_async(self, props, filelike, can_move=False, completion=None):
        if completion is None:
            raise RuntimeError("Completion must be valid for async create")
        uid = self.indexmanager.index(props)
        props['uid'] = uid
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            self._writeContent(uid, filelike, replace=False, can_move=can_move,
                    completion=lambda *args: self._create_completion(uid, props, completion, *args))
        else:
            completion(None, uid)

    def create(self, props, filelike, can_move=False):
        if filelike:
            uid = self.indexmanager.index(props)
            props['uid'] = uid
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            path = self._writeContent(uid, filelike, replace=False, can_move=can_move)
            self.indexmanager.index(props, path)
            return uid
        else:
            return self.indexmanager.index(props)
    
    def get(self, uid, env=None, allowMissing=False, includeFile=False):
        content = self.indexmanager.get(uid)
        if not content: raise KeyError(uid)
        path = self._translatePath(uid)
        fp = None
        # not all content objects have a file
        if includeFile and os.path.exists(path):
            fp = open(path, 'r')
            # now return a Content object from the model associated with
            # this file object
        content = self._mapContent(uid, fp, path, env)
        if fp:
            fp.close()
        return content

    def _update_completion(self, uid, props, completion, exc=None, path=None):
        if exc is not None:
            completion(exc)
            return
        try:
            self.indexmanager.index(props, path)
            completion()
        except Exception, exc:
            completion(exc)

    def update_async(self, uid, props, filelike, can_move=False, completion=None):
        logging.debug('backingstore.update_async')
        if filelike is None:
            raise RuntimeError("Filelike must be valid for async update")
        if completion is None:
            raise RuntimeError("Completion must be valid for async update")

        props['uid'] = uid
        if filelike:
            uid = self.indexmanager.index(props, filelike)
            props['uid'] = uid
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            self._writeContent(uid, filelike, can_move=can_move,
                completion=lambda *args: self._update_completion(uid, props, completion, *args))
        else:
            self.indexmanager.index(props)
            completion()

    def update(self, uid, props, filelike=None, can_move=False):
        props['uid'] = uid
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            path = self._writeContent(uid, filelike, can_move=can_move)
            self.indexmanager.index(props, path)
        else:
            self.indexmanager.index(props)

    def delete(self, uid, allowMissing=True):
        self.indexmanager.delete(uid)
        path = self._translatePath(uid)
        if os.path.exists(path):
            os.unlink(path)
        else:
            if not allowMissing:
                raise KeyError("object for uid:%s missing" % uid)            
        
    def get_uniquevaluesfor(self, propertyname):
        return self.indexmanager.get_uniquevaluesfor(propertyname)


    def get_external_property(self, doc_id, key):
        # external properties default to the following storage
        # <repo>/key/uid which is the file containing the external
        # data. its contents is returned by this call
        # when missing or absent '' is returned
        pfile = os.path.join(self.base, key, str(doc_id))
        if os.path.exists(pfile): v = open(pfile, 'r').read()
        else: v = ''
        return dbus.ByteArray(v)
    

    def set_external_property(self, doc_id, key, value):
        pdir = os.path.join(self.base, key)
        if not os.path.exists(pdir): os.mkdir(pdir)
        pfile = os.path.join(pdir, doc_id)
        fp = open(pfile, 'w')
        fp.write(value)
        fp.close()
        
        
    def find(self, query, order_by=None, limit=None, offset=0):
        if not limit: limit = 4069
        return self.indexmanager.search(query, start_index=offset, end_index=limit, order_by=order_by)

    def ids(self):
        return self.indexmanager.get_all_ids()
    
    def stop(self):
        self.indexmanager.stop()

    def complete_indexing(self):
        self.indexmanager.complete_indexing()

class InplaceFileBackingStore(FileBackingStore):
    """Like the normal FileBackingStore this Backingstore manages the
    storage of files, but doesn't move files into a repository. There
    are no working copies. It simply adds index data through its
    indexmanager and provides fulltext ontop of a regular
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
        self.walker = None
        
    @staticmethod
    def parse(uri):
        return uri.startswith("inplace:")

    def check(self):
        if not os.path.exists(self.uri): return False
        if not os.path.exists(self.base): return False
        return True

        
    def load(self):
        try:
            super(InplaceFileBackingStore, self).load()
        except xapian.DatabaseCorruptError, e:
            # TODO: Try to recover in a smarter way than deleting the base
            # dir and reinitializing the index.

            logging.error('Error while trying to load mount point %s: %s. ' \
                            'Will try to renitialize and load again.' % (self.base, e))

            # Delete the base dir and its contents
            for root, dirs, files in os.walk(self.base, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                os.rmdir(root)

            self.initialize()
            self.load()
            return

        # now map/update the existing data into the indexes
        # but do it async
        self.walker = threading.Thread(target=self._walk)
        self._runWalker = True
        self.walker.setDaemon(True)
        self.walker.start()

    def _walk(self):
        # XXX: a version that checked xattr for uid would be simple
        # and faster
        # scan the uri for all non self.base files and update their
        # records in the db
        for dirpath, dirname, filenames in os.walk(self.uri):
            try:
                # see if there is an entry for the filename
                if self.base in dirpath: continue
                if self.STORE_NAME in dirname:
                    dirname.remove(self.STORE_NAME)

                # blacklist all the hidden directories
                if '/.' in dirpath: continue

                for fn in filenames:
                    try:
                        # give the thread a chance to exit
                        if not self._runWalker: break
                        # blacklist files
                        #   ignore conventionally hidden files
                        if fn.startswith("."): continue
                        
                        source = os.path.join(dirpath, fn)
                        relative = source[len(self.uri)+1:]

                        result, count = self.indexmanager.search(dict(filename=relative))
                        mime_type = gnomevfs.get_mime_type(source)
                        stat = os.stat(source)
                        ctime = datetime.fromtimestamp(stat.st_ctime).isoformat()
                        mtime = datetime.fromtimestamp(stat.st_mtime).isoformat()
                        title = os.path.splitext(os.path.split(source)[1])[0]
                        metadata = dict(filename=relative,
                                        mime_type=mime_type,
                                        ctime=ctime,
                                        mtime=mtime,
                                        title=title)
                        if not count:
                            # create a new record
                            self.create(metadata, source)
                        else:
                            # update the object with the new content iif the
                            # checksum is different
                            # XXX: what if there is more than one? (shouldn't
                            # happen)

                            # FIXME This is throwing away all the entry metadata.
                            # Disabled for trial-3. We are not doing indexing
                            # anyway so it would just update the mtime which is
                            # not that useful. Also the journal is currently
                            # setting the mime type before saving the file making
                            # the mtime check useless.
                            #
                            # content = result.next()
                            # uid = content.id
                            # saved_mtime = content.get_property('mtime')
                            # if mtime != saved_mtime:
                            #     self.update(uid, metadata, source)
                            pass
                    except Exception, e:
                        logging.exception('Error while processing %r: %r' % (fn, e))
            except Exception, e:
                logging.exception('Error while indexing mount point %r: %r' % (self.uri, e))
        self.indexmanager.flush()
        return

    def _translatePath(self, uid):
        try: content = self.indexmanager.get(uid)
        except KeyError: return None
        return os.path.join(self.uri, content.get_property('filename', uid))

##     def _targetFile(self, uid, target=None, ext=None, env=None):
##         # in this case the file should really be there unless it was
##         # deleted in place or something which we typically isn't
##         # allowed
##         # XXX: catch this case and remove the index
##         targetpath =  self._translatePath(uid)
##         return open(targetpath, 'rw')

    # File Management API
    def create_async(self, props, filelike, completion, can_move=False):
        """Inplace backing store doesn't copy, so no need for async"""
        if not filelike:
            raise RuntimeError("Filelike must be valid for async create")
        try:
            uid = self.create(props, filelike, can_move)
            completion(None, uid)
        except Exception, exc:
            completion(exc)

    def _get_unique_filename(self, suggested_filename):
        filename = suggested_filename.replace('/', '_')
        filename = filename.replace(':', '_')

        # FAT limit is 255, leave some space for uniqueness
        max_len = 250
        if len(filename) > max_len:
            name, extension = os.path.splitext(filename)
            filename = name[0:max_len - extension] + extension

        if os.path.exists(os.path.join(self.uri, filename)):
            i = 1
            while len(filename) <= max_len:
                name, extension = os.path.splitext(filename)
                filename = name + '_' + str(i) + extension
                if not os.path.exists(os.path.join(self.uri, filename)):
                    break
                i += 1

        if len(filename) > max_len:
            filename = None

        return filename

    def create(self, props, filelike, can_move=False):
        # the file would have already been changed inplace
        # don't touch it
        proposed_name = None
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            # usually with USB drives and the like the file we are
            # indexing is already on it, however in the case of moving
            # files to these devices we need to detect this case and
            # place the file
            proposed_name = props.get('filename', None)
            if not proposed_name:
                suggested = props.get('suggested_filename', None)
                if suggested:
                    proposed_name = self._get_unique_filename(suggested)
            if not proposed_name:
                proposed_name = os.path.split(filelike.name)[1]
            # record the name before qualifying it to the store
            props['filename'] = proposed_name
            proposed_name = os.path.join(self.uri, proposed_name)

        uid = self.indexmanager.index(props)
        props['uid'] = uid
        path = filelike
        if proposed_name and not os.path.exists(proposed_name):
            path = self._writeContent(uid, filelike, replace=False, target=proposed_name)
        self.indexmanager.index(props, path)
        return uid

    def get(self, uid, env=None, allowMissing=False):
        content = self.indexmanager.get(uid)
        if not content: raise KeyError(uid)
        return content

    def update_async(self, uid, props, filelike, completion, can_move=False):
        if filelike is None:
            raise RuntimeError("Filelike must be valid for async update")
        try:
            self.update(uid, props, filelike, can_move)
            completion()
        except Exception, exc:
            completion(exc)

    def update(self, uid, props, filelike=None, can_move=False):
        # the file would have already been changed inplace
        # don't touch it
        props['uid'] = uid

        proposed_name = None
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            filelike.seek(0)
            # usually with USB drives and the like the file we are
            # indexing is already on it, however in the case of moving
            # files to these devices we need to detect this case and
            # place the file
            proposed_name = props.get('filename', None)
            if not proposed_name:
                proposed_name = os.path.split(filelike.name)[1]
            # record the name before qualifying it to the store
            props['filename'] = proposed_name
            proposed_name = os.path.join(self.uri, proposed_name)

        path = filelike
        if proposed_name:
            path = self._writeContent(uid, filelike, replace=True, target=proposed_name)
        self.indexmanager.index(props, path)
        
    def delete(self, uid):
        c = self.indexmanager.get(uid)
        path = c.get_property('filename', None)
        self.indexmanager.delete(uid)

        if path:
            path = os.path.join(self.uri, path)
            if os.path.exists(path):
                os.unlink(path)
        
    def stop(self):
        if self.walker and self.walker.isAlive():
            # XXX: just force the unmount, flush the index queue
            self._runWalker = False
        self.indexmanager.stop(force=True)

    def complete_indexing(self):
        if self.walker and self.walker.isAlive():
            self.walker.join()
        self.indexmanager.complete_indexing()
