""" 
indexer
~~~~~~~~~~~~~~~~~~~~
fulltext index module

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'


# the name used by the logger
import logging
import dbus.service
import dbus.mainloop.glib
from olpc.datastore.query import XapianFulltext

INDEX_LOG_CHANNEL = 'org.laptop.sugar.Indexer'

INDEX_SERVICE = "org.laptop.sugar.Indexer"
INDEX_DBUS_INTERFACE = "org.laptop.sugar.Indexer"
INDEX_OBJECT_PATH = "/org/laptop/sugar/Indexer"

logger = logging.getLogger(INDEX_LOG_CHANNEL)

class Indexer(dbus.service.Object, XapianFulltext):
    # This object doesn't really publish an interface right now
    # Its a bus object so that dbus can start it automatically
    # when the datastore requests a binding to it
    def __init__(self, repo='fulltext'):
        # global handle to the main look
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        session_bus = dbus.SessionBus()

        self.bus_name = dbus.service.BusName(INDEX_SERVICE,
                                             bus=session_bus,
                                             replace_existing=True,
                                             allow_replacement=True)
        dbus.service.Object.__init__(self, self.bus_name, INDEX_OBJECT_PATH)

    
        self.connect_fulltext(repo, read_only=False)

    
