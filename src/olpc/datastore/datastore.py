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
from olpc.datastore import utils
import logging
import dbus.service
import dbus.mainloop.glib

from StringIO import StringIO

# the name used by the logger
DS_LOG_CHANNEL = 'org.laptop.sugar.DataStore'

DS_SERVICE = "org.laptop.sugar.DataStore"
DS_DBUS_INTERFACE = "org.laptop.sugar.DataStore"
DS_OBJECT_PATH = "/org/laptop/sugar/DataStore"

logger = logging.getLogger(DS_LOG_CHANNEL)

class DataStore(dbus.service.Object):

    def __init__(self, backingstore=None, querymanager=None, **options):
        # global handle to the main look
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        session_bus = dbus.SessionBus()

        self.bus_name = dbus.service.BusName(DS_SERVICE,
                                             bus=session_bus,
                                             replace_existing=True,
                                             allow_replacement=True)
        dbus.service.Object.__init__(self, self.bus_name, DS_OBJECT_PATH)
        self.options = options
        
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
        
        self.backingstore.prepare(self, self.querymanager,
                                  **utils.options_for(self.options, 'backingstore_'))
        self.querymanager.prepare(self, self.backingstore,
                                  **utils.options_for(self.options, 'querymanager_'))
        return True
    

    def mount(self, uri):
        """Given a URI attempt to mount/remount it as a datastore."""
        # on some media we don't want to write the indexes back to the
        # medium (maybe an SD card for example) and we'd want to keep
        # that on the XO itself. In these cases their might be very
        # little identifying information on the media itself.
        pass
    
    
    # PUBLIC API
    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='a{sv}s',
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
        filename = filelike
        if filelike:
            if isinstance(filelike, basestring):
                # lets treat it as a filename
                filelike = open(filelike, "r")
            t = filelike.tell()

        content = self.querymanager.create(props, filename)

        if filelike:
            filelike.seek(t)
            self.backingstore.create(content, filelike)

        self.Created(content.id, props)
        logging.debug("created %s" % content.id)
        
        return content.id

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="sa{sv}")
    def Created(self, uid, props): pass
        
    
    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='',
             out_signature='as')
    def all(self):
        # workaround for not having optional args or None in
        # DBus ..  blah
        results = self.querymanager.find()
        return [r.id for r in results]

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='a{sv}',
             out_signature='aa{sv}u')
    def find(self, query=None, **kwargs):
        """find(query)
        takes a dict of parameters and returns data in the following
             format

             (results, count)

             where results are:
             [ {props}, {props}, ... ]

        which is to be read, results is an ordered list of property
        dicts, akin to what is returned from get_properties. 'uid' is
        included in the properties dict as well and is the unique
        identifier used in subsequent calls to refer to that object.

        special keywords in the query that are supported are more
        fully documented in the query.py::find method docstring.

        The 'include_files' keyword will trigger the availability of
        user accessible files. Because these are working copies we
        don't want to generate them unless needed. In the case the
        the full properties set matches doing the single roundtrip
        to start an activity makes sense.
        """
        # only goes to the primary now. Punting on the merge case
        include_files = kwargs.pop('include_files', False)
        results, count = self.querymanager.find(query, **kwargs)
        d = []
        for r in results:
            props =  {}
            for prop in r.get_properties():
                props[prop.key] = prop.marshall()

            if 'uid' not in props:
                props['uid'] = r.id
                
            filename = ''
            if include_files :
                try: filename = self.backingstore.get(r.id).filename
                except KeyError: pass
                props['filename'] = filename
            d.append(props)

        return (d, len(results))

    def get(self, uid):
        c = self.querymanager.get(uid)
        # XXX: this is a workaround to the sqla mapping extension not
        # being properly called in the current codebase
        if c: c.backingstore = self.backingstore
        return c

    @dbus.service.method(DS_DBUS_INTERFACE,
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

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='s',
                         out_signature='a{sv}')
    def get_properties(self, uid):
        content = self.get(uid)
        dictionary = {}
        for prop in content.get_properties():
            dictionary[prop.key] = prop.marshall()
        return dictionary

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='sa{sv}s',
             out_signature='')
    def update(self, uid, props, filelike=None):
        """Record the current state of the object checked out for a
        given uid. If contents have been written to another file for
        example. You must create it
        """
        filename = filelike
        if filelike:
            if isinstance(filelike, basestring):
                filelike = open(filelike, 'r')

                
        content = self.get(uid)
        if content:
            self.querymanager.update(uid, props, filename)
            if filelike: self.backingstore.set(uid, filelike)
            self.Updated(content.id, props)
            logger.debug("updated %s" % content.id)

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="sa{sv}")
    def Updated(self, uid, props): pass

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='')
    def delete(self, uid):
        content = self.get(uid)
        if content:
            self.querymanager.delete(uid)
            self.backingstore.delete(uid)
            self.Deleted(content.id)
            logger.debug("deleted %s" % content.id)

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Deleted(self, uid): pass

    def stop(self):
        """shutdown the service"""
        self.Stopped()
        self._connection.get_connection()._unregister_object_path(DS_OBJECT_PATH)
        self.querymanager.stop()

    @dbus.service.signal(DS_DBUS_INTERFACE)
    def Stopped(self): pass

        
