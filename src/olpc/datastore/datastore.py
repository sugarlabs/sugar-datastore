""" 
olpc.datastore.datastore
~~~~~~~~~~~~~~~~~~~~~~~~
the datastore facade

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'


from olpc.datastore import backingstore
from olpc.datastore import query
import utils
import logging

class DataStore(object):
    __metaclass__ = utils.Singleton

    def __init__(self, backingstore=None, querymanager=None):
        self.backingstore = None
        self.querymanager = None
        if backingstore: self.connect_backingstore(backingstore)
        if querymanager: self.connect_querymanager(querymanager)
        
    def connect_backingstore(self, uri_or_backingstore):
        """
        connect to a new backing store

        @returns: Boolean for success condition
        """
        if isinstance(uri_or_backingstore, basestring):
            # XXX: divert to factory
            # for now we assume a local FS store
            bs = backingstore.FileBackingStore(uri_or_backingstore)
        else:
            bs = uri_or_backingstore

        self.backingstore = bs
        return self._bind()

    def connect_querymanager(self, uri_or_querymanager):
        if isinstance(uri_or_querymanager, basestring):
            qm = query.DefaultQueryManager(uri_or_querymanager)
        else:
            qm = uri_or_querymanager
        self.querymanager = qm
        return self._bind()
    
    def _bind(self):
        """Notify components they are being bound to this datastore"""
        # verify that both are set, then init both in order
        if not self.backingstore or not self.querymanager:
            return False
        
        self.backingstore.prepare(self, self.querymanager)
        self.querymanager.prepare(self, self.backingstore)
        return True
    

    # PUBLIC API
    def create(self, props, filelike=None):
        """create a new entry in the datastore. If a file is passed it
        will be consumed by the datastore. Because the repository has
        a checkin/checkout model this will create a copy of the file
        in the repository. Changes to this file will not automatically
        be be saved. Rather it is recorded in its current state.

        When many backing stores are associated with a datastore
        new objects are created in the first datastore. More control
        over this process can come at a later time.
        """
        t = filelike.tell()
        content = self.querymanager.create(props, filelike)
        filelike.seek(t)
        if filelike is not None:
            self.backingstore.create(content, filelike)
        return content.id

    def find(self, query=None, **kwargs):
        # only goes to the primary now. Punting on the merge case
        results = self.querymanager.find(query, **kwargs)
        return [r.id for r in results]

    def get(self, uid):
        c = self.querymanager.get(uid)
        # XXX: this is a workaround to the extension not being
        # properly called in the current codebase
        if c: c.backingstore = self.backingstore
        return c
    
    def get_filename(self, uid):
        content = self.get(uid)
        if content:
            return self.backingstore.get(uid).filename

    def get_data(self, uid):
        content = self.get(uid)
        if content:
            return content.get_data()


    
    def update(self, uid, props, file=None):
        """Record the current state of the object checked out for a
        given uid. If contents have been written to another file for
        example. You must create it
        """
        content = self.get(uid)
        if content:
            self.querymanager.update(uid, props, file)
            self.backingstore.set(uid, file)

    def delete(self, uid):
        content = self.get(uid)
        if content:
            self.querymanager.delete(uid)
            self.backingstore.delete(uid)
        
    
def configure():
    # disable as much logging by default for the OLPC
    logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)
    logging.getLogger('lemur').setLevel(logging.CRITICAL)
