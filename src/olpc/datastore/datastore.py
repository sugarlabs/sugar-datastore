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

DEFAULT_LIMIT = 65536

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

        self.Mounted(mp.descriptor())
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
        mp = self.mountpoints[mountpoint_id]
        try:
            mp.stop()
        except:
            logger.warn("Issue with unmounting store. Trying to continue")
            
        self.Unmounted(mp.descriptor())
        del self.mountpoints[mountpoint_id]

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="a{sv}")
    def Mounted(self, descriptior):
        """indicates that a new backingstore has been mounted by the
    datastore. Returns the mount descriptor, like mounts()"""
        pass

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="a{sv}")
    def Unmounted(self, descriptor):
        """indicates that a new backingstore has been mounted by the
    datastore. Returns the mount descriptor, like mounts()"""
        pass
    
    
    ### End Mount Points

    ### Backup support
    def pause(self, mountpoints=None):
        """pause the datastore, during this time it will not process
    requests. this allows the underlying stores to be backup up via
    traditional mechanisms
    """
        if mountpoints:
            mps = [self.mountpoints[mp] for mp in mountpoints]
        else:
            mps = self.mountpoints.values()

        for mp in mps:
            mp.stop()

    def unpause(self, mountpoints=None):
        """resume the operation of a set of paused mountpoints"""
        if mountpoints:
            mps = [self.mountpoints[mp] for mp in mountpoints]
        else:
            mps = self.mountpoints.values()

        for mp in mps:
            mp.initialize_and_load()
        
    ### End Backups
            
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

    def _create_completion(self, async_cb, async_err_cb, exc=None, uid=None):
        logger.debug("_create_completion_cb() called with %r / %r, exc %r, uid %r" % (async_cb, async_err_cb, exc, uid))
        if exc is not None:
            async_err_cb(exc)
            return

        self.Created(uid)
        logger.debug("created %s" % uid)
        async_cb(uid)

    # PUBLIC API
    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='a{sv}sb',
                         out_signature='s',
                         async_callbacks=('async_cb', 'async_err_cb'),
                         byte_arrays=True)
    def create(self, props, filelike=None, transfer_ownership=False, async_cb=None, async_err_cb=None):
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
        mp.create_async(props, filelike, can_move=transfer_ownership,
            completion=lambda *args: self._create_completion(async_cb, async_err_cb, *args))

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Created(self, uid): pass

    def _single_search(self, mountpoint, query, order_by, limit, offset):
        results, count = mountpoint.find(query.copy(), order_by,
                                         limit + offset, offset)
        return list(results), count, 1

    def _multiway_search(self, query, order_by=None, limit=None, offset=None):
        mountpoints = query.pop('mountpoints', self.mountpoints)
        mountpoints = [self.mountpoints[str(m)] for m in mountpoints]

        if len(mountpoints) == 1:
            # Fast path the single mountpoint case
            return self._single_search(mountpoints[0], query,
        order_by, limit, offset)

        results = []
        # XXX: the merge will become *much* more complex in when
        # distributed versioning is implemented.
        # collect
        #  some queries mutate the query-dict so we pass a copy each
        #  time
        for mp in mountpoints:
            result, count =  mp.find(query.copy(), order_by, limit)
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
        return d, len(d), len(results)

    #@utils.sanitize_dbus    
    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='a{sv}as',
             out_signature='aa{sv}u')
    def find(self, query=None, properties=None, **kwargs):
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
        else:
            if 'query' not in kwargs:
                kwargs['query'] = query
        
        include_files = kwargs.pop('include_files', False)
        order_by = kwargs.pop('order_by', [])

        # XXX: this is a workaround, deal properly with n backends
        limit = kwargs.pop('limit', DEFAULT_LIMIT)
        offset = kwargs.pop('offset', 0)

        # distribute the search to all the mountpoints unless a
        # backingstore id set is specified
        # backends may be able to return sorted results, if there is
        # only a single backend in the query we can use pre-sorted
        # results directly
        results, count, results_from = self._multiway_search(kwargs,
                                                             order_by,
                                                             limit, offset)

        
        # ordering is difficult when we are dealing with sets from
        # more than one source. The model is this.
        # order by the primary (first) sort criteria, then do the rest
        # in post processing. This allows use to assemble partially
        # database sorted results from many sources and quickly
        # combine them.
        if results_from > 1:
            if order_by:
                # resolve key names to columns
                if isinstance(order_by, basestring):
                    order_by = [o.strip() for o in order_by.split(',')]

                if not isinstance(order_by, list):
                    logger.debug("bad query, order_by should be a list of property names")                
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

                results = results[offset:limit+offset]
            else:
                results = results.values()
            
        d = []
        c = 0
        for r in results:
            props = r.properties
            props['uid'] = r.id
            props['mountpoint'] = r.backingstore.id
            props['filename'] = ''
            d.append(props)

            if properties:
                for name in props.keys():
                    if name not in properties:
                        del props[name]
            c+= 1
            if limit and c > limit: break
            
        return (d, count)

    def get(self, uid):
        mp = self._resolveMountpoint()
        c = None
        try:
            c = mp.get(uid)
            if c: return c
        except KeyError:
            pass
            
        if not c:
            for mp in self.mountpoints.itervalues():
                try:
                    c = mp.get(uid)
                    if c: break
                except KeyError:
                    continue
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
        props = content.properties
        props['mountpoint'] = content.backingstore.id
        return props

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
    
    def _update_completion_cb(self, async_cb, async_err_cb, content, exc=None):
        logger.debug("_update_completion_cb() called with %r / %r, exc %r" % (async_cb, async_err_cb, exc))
        if exc is not None:
            async_err_cb(exc)
            return

        self.Updated(content.id)
        logger.debug("updated %s" % content.id)
        async_cb()

    #@utils.sanitize_dbus
    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='sa{sv}sb',
             out_signature='',
             async_callbacks=('async_cb', 'async_err_cb'),
             byte_arrays=True)
    def update(self, uid, props, filelike=None, transfer_ownership=False,
            async_cb=None, async_err_cb=None):
        """Record the current state of the object checked out for a
        given uid. If contents have been written to another file for
        example. You must create it
        """
        content = self.get(uid)
        mountpoint = props.pop('mountpoint', None)
        content.backingstore.update_async(uid, props, filelike, can_move=transfer_ownership,
            completion=lambda *args: self._update_completion_cb(async_cb, async_err_cb, content, *args))

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
            
