from olpc.datastore.backingstore import FileBackingStore
from olpc.datastore.sxattr import Xattr
from olpc.datastore.utils import create_uid
from olpc.datastore.bin_copy import bin_copy

from mercurial import repo, filelog, transaction, util, revlog
import os, sys, tempfile

# Xattr attribute namespace
NAMESPACE = "datastore"



class localizedFilelog(filelog.filelog):
    def __init__(self, opener, path, cwd=None):
        self._fullpath = os.path.realpath(cwd)
        revlog.revlog.__init__(self, opener,
                               self.encodedir(path + ".i"))
    @property
    def rindexfile(self): return os.path.join(self._fullpath, self.indexfile)

    @property
    def rdatafile(self): return os.path.join(self._fullpath, self.datafile)

class FileRepo(repo.repository):
    """A very simple file repository that functions without the need
    for global changesets or a working copy.
    """

    def __init__(self, path):
        self.basepath = path
        self.root = os.path.realpath(self.basepath)
        self.repo = "data"
        self.datadir = os.path.join(self.basepath, ".ds")
        if not os.path.exists(self.basepath):
            os.makedirs(self.basepath)
            #os.chdir(self.basepath)

    def file(self, path):
        eopen = util.encodedopener(util.opener(self.datadir), util.encodefilename)
        fl = localizedFilelog(eopen, path, cwd=self.datadir)
        return fl


    def __contains__(self, path):
        """Is this path already managed by the repo?"""
        f = self.file(path)
        p = f.rindexfile
        return os.path.exists(p)

    def raw_copy(self, filenames):
        # create localized repository versions of the raw data from
        # another repo
        # this doesn't do indexing or anything like that 
        for fn in filenames:
            srcdir, n = os.path.split(fn)
            target = self._rjoin(n)
            bin_copy(fn, target)

            
    def raw_sources(self):
        # return list of filenames which must be copied
        return [d for d in self._rdata() if d]
        
    def _rjoin(self, path):
        # repository path
        return os.path.join(self.basepath, ".ds", path)
        
    def _rdata(self, path):
        """return the index and datafile names in the repository"""
        f = self.file(path)
        base = os.path.join(self.basepath, ".ds")
        i = os.path.join(base, f.rindexfile)
        d = os.path.join(base, f.rdatafile)
        return (i and i or None, d and d or None)
    
    def put(self, path, source, parent=None, text=None, meta=None):
        """Create a new revision of the content indicated by
        'path'. 'source' is the filename containing the data we wish
        to commit. parent when present is the parent revision this
        data comes from. When parent is not provided we first look at
        the source file for the xattr 'user.datastore.revision', if
        that is present we assume it is the parent revision of the
        element in path. When absent we assume that this is a delta to
        the tip.

        
        @return rev, parent, parent is tip, changeset
        - rev is this files new revision number
        - parent is the id of the revision used as the parent
        - parent_is_tip indicates if the parent was the most recent
          head
        - changeset is the random uid of this transaction used in
          merges
        """

        # we don't have a real working copy model. The source file may
        # exist in some path or location well outside the repo (and
        # most likely does)
        f = self.file(path)
        data = open(source, 'r').read()
        
        tip = f.tip()
        if not parent:
            x = Xattr(source, NAMESPACE)
            # attempt to resolve the revision number from the property
            parent = x.get('revision')
            if parent:
                parent = int(parent) # from unicode
            else:
                # there wasn't an attribute on the file
                # this can happen if the file was created by the
                # client or
                # the filesystem didn't support xattr
                # in this case we assume the revision is the tip
                parent = tip

        if isinstance(parent, int):
                # its a revision number, resolve it to a node id
                parent = f.node(parent)

        if not f.cmp(parent, data):
            # they are the same
            return

        # assume some default values for missing metadata
        # changeset is important here. Files don't properly belong to
        # change sets, but this uid is used to discriminate versions
        # with identical revision numbers from different sources
        changeset = create_uid()
        if not meta: meta = {}
        meta.setdefault('text', text and text or "automatic commit")
        meta['changeset'] = changeset

        # commit the data to the log
        t = self.transaction()
        rev = f.count() + 1
        f.add(data, meta, t, rev, parent)
        t.close()

        return rev, parent, parent == tip, changeset

    def transaction(self):
        return transaction.transaction(sys.stderr.write, open, "journal")


    def tip(self, path):
        # return the revision id that is the tip for a given path
        l = self.file(path)
        return l.rev(l.tip())
    
    def revision(self, path, rev):
        """Given the path name return the data associated with the raw
        revision"""
        # we really need a less memory intensive way of doing this
        # stream the data to stable media as it processes the delta
        if path not in self: raise KeyError("%s is not managed by repo" % path)
        l = self.file(path)
        if isinstance(rev, int):
            n = l.node(rev)
        else:
            n = rev
        return l.read(n)

    def dump(self, path, rev, target):
        """Dump the contents of a revision to the filename indicated
        by target"""
        fp = open(target, "w")
        fp.write(self.revision(path, rev))
        fp.close()
        # tag the checkout with its current revision
        # this is used to aid in parent chaining on commits
        x = Xattr(target, NAMESPACE)
        x['revision'] = rev
        
    def remove(self, path):
        """Hard remove the whole version history of an object"""
        i, d = self._rdata(path)
        if i and os.path.exists(i):
            os.unlink(i)
        # for small files d will not exist as the data is inlined to
        # the the index
        if d and os.path.exists(d):
            os.unlink(d)

    def strip(self, path, rev):
        """attempt to remove a given revision from the history of
        path"""
        f = self.file(path)
        f.strip(rev, rev)
    

class HgBackingStore(FileBackingStore):
    """This backingstore for the datastore supports versioning by
    keeping a barebones Mercurial repository under the hood
    """
    capabilities = ("file", "versions")
    
    def __init__(self, uri, **kwargs):
        # the internal handle to the HgRepo
        self.repo = None
        uri = uri[len('hg:'):]
        super(HgBackingStore, self).__init__(uri, **kwargs)
        
    @staticmethod
    def parse(uri):
        return uri.startswith("hg:")

    def check(self):
        if not os.path.exists(self.uri): return False
        if not os.path.exists(self.base): return False
        return True

    def initialize(self):
        super(FileBackingStore, self).initialize()
        self.repo = FileRepo(self.base)
        
    def load(self):
        super(HgBackingStore, self).load()
        # connect the repo
        if not self.repo:
            self.repo = FileRepo(self.base)

    # File Management API
    def create(self, props, filelike):
        # generate the uid ourselves. we do this so we can track the
        # uid and changeset info
        # Add it to the index
        uid = create_uid()
        props['uid'] = uid
        props.setdefault('message', 'initial')
        uid, rev = self.checkin(props, filelike)
        return uid

    def get(self, uid, rev=None, env=None):
        # we have a whole version chain, but get contracts to
        # return a single entry. In this case we default to 'tip'
        if not rev:
            rev = self.repo.tip(uid)
        results, count = self.indexmanager.get_by_uid_prop(uid, rev)
        if count == 0:
            raise KeyError(uid)
        elif count == 1:
            return results.next()

        raise ValueError("Got %d results for 'get' operation on %s" %(count, uid))

######################################################################
#        XXX: This whole policy is botched unless the journal grows an
#        # interface to display other versions of the main document
#        # which it doesn't have. If I do this then we don't see those
#        # versions in the journal and have no means to access
#        # them. For the time being we just index everything and let
#        # date sort them out.
######################################################################
#        # recover the old records for this uid
#        # update may rewrite/remove 1-n documents
#        # depending on the number of heads and so on
#        # this needs to be done under a single transaction
#        # actions will be a set of commands passed to
#        # xapianindex.enque
#        # the loop will process the entire action set at once
#
#        # we need to pass head/tip tags from the parent to the child
#        # as a result of the update
#        # XXX: currently we are only going to index the HEADs and TIP
#        # revisions in the repository (and those marked with KEEP).
#        # This means that when we update
#        #    with these tags:
#        #           we can remove the old version from xapian
#        #    w/o these tags:
#        #           it gets a head tag, implying a branch
#        #
#        # because the keep flag indicates content is needed to be kept
#        # locally we have two real choices, either
#        #   move it forward with each revision
#        #   keep only the original tagged version
#        #      and index the new one as well (because it will have the
#        #                                     HEAD tag)
##########################################################################
    def update(self, uid, props, filelike):
        props['uid'] = uid
        uid, rev = self.checkin(props, filelike)
        return uid


    def delete(self, uid, rev=None):
        # delete the object at 'uid', when no rev is passed tip is
        # removed
        if rev is None:
            rev = self.repo.tip(uid)
        c = self.get(uid, rev)
        self.indexmanager.delete(c.id)
        self.repo.strip(uid, rev)

    def _targetFile(self, uid, target=None, ext=None, env=None):
        c = self.indexmanager.get(uid)
        rev = int(c.get_property('vid'))
        rev -= 1 # adjust for 0 based counting
        self.repo.dump(uid, rev, target)
        return open(target, 'rw')


    def checkin(self, props, filelike):
        """create or update the content object, creating a new
        version"""
        uid = props.setdefault('uid', create_uid())
        if filelike:
            message = props.setdefault('message', 'initial')
            parent = props.pop('parent', None)
            rev, parent, isTip, changeset = self.repo.put(uid, filelike,
                                                          parent, message,
                                                          meta=dict(uid=uid))
            # the create case is pretty simple
            # mark everything with defaults
            props['changeset'] = changeset
            props['vid'] = str(rev)
            
        self.indexmanager.index(props, filelike)
        return uid, rev

    def checkout(self, uid, vid=None, target=None, dir=None):
        """checkout the object with this uid at vid (or HEAD if
        None). Returns (props, filename)"""
        # use the repo to drive the property search
        f = self.repo.file(uid)
        if vid:
            vid = f.node(int(vid) -1) # base 0 counting
        else:
            vid = f.tip()
        rev = f.rev(vid)
        # there will only be one thing with the changeset id of this
        # 'f'
        m = f._readmeta(vid)
        changeset = m['changeset']
        objs, count = self.indexmanager.search(dict(changeset=changeset))
        assert count == 1
        obj = objs.next()

        if not target:
            target, ext = obj.suggestName()
            if not target:
                fd, fn = tempfile.mkstemp(dir=dir)
                target = fn

        if not target.startswith('/'):
            if dir: target = os.path.join(dir, target)
            else: os.path.join('/tmp', target)
                
        self.repo.dump(uid, rev, target)
        return obj.properties, target
    
if __name__ == "__main__":
    import rlcompleter2
    rlcompleter2.setup(verbose=0)
    
    TESTLOC = "/tmp/fltest"
    os.system('rm -rf %s' % TESTLOC)

    c = FileRepo(TESTLOC)
    
    n = c.blit("foo", "this is a test")
    m = c.blit("bar", "another test")
    

    o = c.blit("foo", "\nanother line", mode="a")

    c.revisions("foo")
