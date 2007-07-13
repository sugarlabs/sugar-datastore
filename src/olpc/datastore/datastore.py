""" 
olpc.datastore.datastore
~~~~~~~~~~~~~~~~~~~~~~~~
the datastore facade

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'



import logging
import dbus.service
import dbus.mainloop.glib

from olpc.datastore import utils

# the name used by the logger
DS_LOG_CHANNEL = 'org.laptop.sugar.DataStore'

DS_SERVICE = "org.laptop.sugar.DataStore"
DS_DBUS_INTERFACE = "org.laptop.sugar.DataStore"
DS_OBJECT_PATH = "/org/laptop/sugar/DataStore"

logger = logging.getLogger(DS_LOG_CHANNEL)

class DataStore(dbus.service.Object):

    def __init__(self, **options):
        self.options = options
        self.backends = []
        self.mountpoints = {}
        self.root = None
        
        # global handle to the main look
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        session_bus = dbus.SessionBus()

        self.bus_name = dbus.service.BusName(DS_SERVICE,
                                             bus=session_bus,
                                             replace_existing=False,
                                             allow_replacement=False)
        dbus.service.Object.__init__(self, self.bus_name, DS_OBJECT_PATH)

        
    ####
    ## Backend API
    ## register a set of datastore backend factories which will manage
    ## storage
    def registerBackend(self, backendClass):
        self.backends.append(backendClass)
        
    ## MountPoint API
    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature="sa{sv}",
                         out_signature='s')
    def mount(self, uri, options=None):
        """(re)Mount a new backingstore for this datastore.
        Returns the mountpoint id or an empty string to indicate failure.
        """
        # on some media we don't want to write the indexes back to the
        # medium (maybe an SD card for example) and we'd want to keep
        # that on the XO itself. In these cases their might be very
        # little identifying information on the media itself.
        uri = str(uri)

        _options = utils._convert(options)
        if _options is None: _options = {}
        
        mp = self.connect_backingstore(uri, **_options)
        if not mp: return ''
        if mp.id in self.mountpoints:
            self.mountpoints[mp.id].stop()

        mp.bind_to(self)
        self.mountpoints[mp.id] = mp
        if self.root is None:
            self.root = mp
        return mp.id

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature="",
                         out_signature="aa{sv}")
    def mounts(self):
        """return a list of mount point descriptiors where each
        descriptor is a dict containing atleast the following keys:
        'id' -- the id used to refer explicitly to the mount point
        'title' -- Human readable identifier for the mountpoint
        'uri' -- The uri which triggered the mount
        """
        return [mp.descriptor() for mp in self.mountpoints.itervalues()]

    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature="s",
                         out_signature="")
    def unmount(self, mountpoint_id):
        """Unmount a mountpoint by id"""
        if mountpoint_id not in self.mountpoints: return
        self.mountpoints[mountpoint_id].stop()
        del self.mountpoints[mountpoint_id]
    ### End Mount Points

    ### Buddy Management
    ##  A single datastore typically refers to a single user
    ##  this breaks down a little in the case of things like USB
    ##  sticks and so on. We provide a facility for tracking
    ##  co-authors of content
    ##  there are associated changes to 'find' to resolve buddies
    def addBuddy(self, id, name, fg_color, bg_color, mountpoint=None):
        mp = None
        if mountpoint is None: mp = self.root
        else: mp = self.mountpoints.get(mountpoint)
        if mp is None: raise ValueError("Invalid mountpoint")
        mp.addBuddy(id, name, fg_color, bg_color)

    def getBuddy(self, bid):
        """Get a buddy by its id"""
        b = None
        for mp in self.mountpoints.itervalues():
            b = mp.getBuddy(bid)
            if b: break
        return b

    
    def buddies(self):
        buddies = set()
        for mp in self.mountpoints.itervalues():
            buddies = buddies.union(mp.getBuddies())
        return buddies
        
    

    ## end buddy api
    
    def connect_backingstore(self, uri, **kwargs):
        """
        connect to a new backing store

        @returns: Boolean for success condition
        """
        bs = None
        for backend in self.backends:
            if backend.parse(uri) is True:
                bs = backend(uri, **kwargs)
                bs.initialize_and_load()
                # The backingstore should be ready to run
                break
        return bs
    

    def _resolveMountpoint(self, mountpoint=None):
        if isinstance(mountpoint, dict):
            mountpoint = mountpoint.pop('mountpoint', None)
            
        if mountpoint is not None:
            # this should be the id of a mount point
            mp = self.mountpoints[mountpoint]
        else:
            # the first one is the default
            mp = self.root
        return mp

    # PUBLIC API
    #@utils.sanitize_dbus
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
        mp = self._resolveMountpoint(props)
        uid = mp.create(props, filelike)
        self.Created(uid)
        logging.debug("created %s" % uid)
        
        return uid

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Created(self, uid): pass
        
    def _multiway_search(self, query):
        mountpoints = query.pop('mountpoints', self.mountpoints)
        mountpoints = [self.mountpoints[str(m)] for m in mountpoints]
        results = []
        # XXX: the merge will become *much* more complex in when
        # distributed versioning is implemented.
        # collect
        #  some queries mutate the query-dict so we pass a copy each
        #  time
        for mp in mountpoints:
            result, count =  mp.find(query.copy())
            results.append(result)
            
        # merge
        d = {}
        for res in results:
            for hit in res:
                existing = d.get(hit.id)
                if not existing or \
                   existing.get_property('mtime') < hit.get_property('mtime'):
                    # XXX: age/version check
                    d[hit.id] = hit

        return d, len(d)

    #@utils.sanitize_dbus    
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

        To order results by a given property you can specify:
        >>> ds.find(order_by=['author', 'title'])

        Order by must be a list of property names given in the order
        of decreasing precedence.

        """
        # only goes to the primary now. Punting on the merge case
        if isinstance(query, dict):
            kwargs.update(query)

        include_files = kwargs.pop('include_files', False)
        order_by = kwargs.pop('order_by', [])
        
        # distribute the search to all the mountpoints unless a
        # backingstore id set is specified
        results, count = self._multiway_search(kwargs)

        
        # ordering is difficult when we are dealing with sets from
        # more than one source. The model is this.
        # order by the primary (first) sort criteria, then do the rest
        # in post processing. This allows use to assemble partially
        # database sorted results from many sources and quickly
        # combine them.
        if order_by:
            # resolve key names to columns
            if isinstance(order_by, basestring):
                order_by = [o.strip() for o in order_by.split(',')]
                
            if not isinstance(order_by, list):
                logging.debug("bad query, order_by should be a list of property names")                
                order_by = None

            # generate a sort function based on the complete set of
            # ordering criteria which includes the primary sort
            # criteria as well to keep it stable.
            def comparator(a, b):
                # we only sort on properties so
                for criteria in order_by:
                    mode = 1 # ascending
                    if criteria.startswith('-'):
                        mode = -1
                        criteria = criteria[1:]
                    pa = a.get_property(criteria, None)
                    pb = b.get_property(criteria, None)
                    r = cmp(pa, pb) * mode
                    if r != 0: return r
                return 0
            

            r = results.values()
            r.sort(comparator)
            results = r
        else:
            results = results.values()
            
        d = []
        for r in results:
            props =  {}
            props.update(r.properties)
            
            if 'uid' not in props:
                props['uid'] = r.id

            if 'mountpoint' not in props:
                props['mountpoint'] = r.backingstore.id
            
            filename = ''
            if include_files :
                try: filename = r.filename
                except KeyError: pass
                props['filename'] = filename
            d.append(props)

        return (d, len(results))

    def get(self, uid):
        mp = self._resolveMountpoint()
        c = mp.get(uid)
        if not c:
            for mp in self.mountpoints.itervalues():
                c = mp.get(uid)
                if c: break
        return c

    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='s')
    def get_filename(self, uid):
        content = self.get(uid)
        if content:
            try: return content.filename
            except AttributeError: pass
        return ''
        
    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='s',
                         out_signature='a{sv}')
    def get_properties(self, uid):
        content = self.get(uid)
        return content.properties

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='sa{sv}',
                         out_signature='as')
    def get_uniquevaluesfor(self, propertyname, query=None):
        propertyname = str(propertyname)
        
        if not query: query = {}
        mountpoints = query.pop('mountpoints', self.mountpoints)
        mountpoints = [self.mountpoints[str(m)] for m in mountpoints]
        results = set()

        for mp in mountpoints:
            result = mp.get_uniquevaluesfor(propertyname)
            results = results.union(result)
        return results
    

    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='sa{sv}s',
             out_signature='')
    def update(self, uid, props, filelike=None):
        """Record the current state of the object checked out for a
        given uid. If contents have been written to another file for
        example. You must create it
        """
        content = self.get(uid)
        content.backingstore.update(uid, props, filelike)
        if filelike:
            self.Updated(content.id)
            logger.debug("updated %s" % content.id)

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Updated(self, uid): pass

    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='')
    def delete(self, uid):
        content = self.get(uid)
        if content:
            content.backingstore.delete(uid)
            self.Deleted(uid)
            logger.debug("deleted %s" % uid)

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Deleted(self, uid): pass

    def stop(self):
        """shutdown the service"""
        self.Stopped()
        self._connection.get_connection()._unregister_object_path(DS_OBJECT_PATH)
        for mp in self.mountpoints.values(): mp.stop()


    @dbus.service.signal(DS_DBUS_INTERFACE)
    def Stopped(self): pass

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='',
             out_signature='')
    def complete_indexing(self):
        """Block waiting for all queued indexing operations to
        complete. Used mostly in testing"""
        for mp in self.mountpoints.itervalues():
            mp.complete_indexing()
            
