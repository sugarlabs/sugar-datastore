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
import logging
import dbus.service
import dbus.mainloop.glib
import dbus_helpers
from StringIO import StringIO

# the name used by the logger
DS_LOG_CHANNEL = 'org.laptop.sugar.DataStore'

_DS_SERVICE = "org.laptop.sugar.DataStore"
_DS_DBUS_INTERFACE = "org.laptop.sugar.DataStore"
_DS_OBJECT_PATH = "/org/laptop/sugar/DataStore"

logger = logging.getLogger(DS_LOG_CHANNEL)

class DataStore(dbus.service.Object):

    def __init__(self, backingstore=None, querymanager=None):
        # global handle to the main look
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        session_bus = dbus.SessionBus()

        self.bus_name = dbus.service.BusName(_DS_SERVICE,
                                             bus=session_bus,
                                             replace_existing=True,
                                             allow_replacement=True)
        dbus.service.Object.__init__(self, self.bus_name, _DS_OBJECT_PATH)
        
        self.emitter = dbus_helpers.emitter(session_bus,
                                            _DS_OBJECT_PATH,
                                            _DS_DBUS_INTERFACE)
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
    @dbus.service.method(_DS_DBUS_INTERFACE,
                         in_signature='a{ss}s',
                         out_signature='s')
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
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            t = filelike.tell()

        content = self.querymanager.create(props, filelike)

        if filelike:
            filelike.seek(t)
            self.backingstore.create(content, filelike)

        self.emitter('create', content.id, props, signature="sa{sv}")
        logger.debug("created %s" % content.id)
        
        return content.id

    @dbus.service.method(_DS_DBUS_INTERFACE,
             in_signature='',
             out_signature='as')
    def all(self):
        # workaround for not having optional args or None in
        # DBus ..  blah
        results = self.querymanager.find()
        return [r.id for r in results]

    @dbus.service.method(_DS_DBUS_INTERFACE,
             in_signature='a{sv}',
             out_signature='as')
    def find(self, query=None, **kwargs):
        # only goes to the primary now. Punting on the merge case
        results = self.querymanager.find(query, **kwargs)
        return [r.id for r in results]

    def get(self, uid):
        c = self.querymanager.get(uid)
        # XXX: this is a workaround to the sqla mapping extension not
        # being properly called in the current codebase
        if c: c.backingstore = self.backingstore
        return c

    @dbus.service.method(_DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='s')
    def get_filename(self, uid):
        content = self.get(uid)
        if content:
            try: return self.backingstore.get(uid).filename
            except KeyError: pass
        return ''
        
    def get_data(self, uid):
        content = self.get(uid)
        if content:
            return content.get_data()

    def put_data(self, uid, data):
        self.update(uid, None, StringIO(data))

    @dbus.service.method(_DS_DBUS_INTERFACE,
                         in_signature='s',
                         out_signature='a{sv}')
    def get_properties(self, uid):
        content = self.get(uid)
        dictionary = {}
        for prop in content.get_properties():
            dictionary[prop.key] = prop.marshall()
        return dictionary

    @dbus.service.method(_DS_DBUS_INTERFACE,
             in_signature='sa{ss}s',
             out_signature='')
    def update(self, uid, props, filelike=None):
        """Record the current state of the object checked out for a
        given uid. If contents have been written to another file for
        example. You must create it
        """
        if filelike:
            if isinstance(filelike, basestring):
                filelike = open(filelike, 'r')
        content = self.get(uid)
        if content:
            self.querymanager.update(uid, props, filelike)
            if filelike: self.backingstore.set(uid, filelike)
            self.emitter('update', content.id, props, signature="sa{sv}")
            logger.debug("updated %s" % content.id)

    
    @dbus.service.method(_DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='')
    def delete(self, uid):
        content = self.get(uid)
        if content:
            self.querymanager.delete(uid)
            self.backingstore.delete(uid)
            self.emitter('delete', content.id, signature="s")
            logger.debug("deleted %s" % content.id)
    
