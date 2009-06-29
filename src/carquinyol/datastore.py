# Copyright (C) 2008, One Laptop Per Child
# Based on code Copyright (C) 2007, ObjectRealms, LLC
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import logging
import uuid
import time
import os
import traceback

import dbus
import gobject

from sugar import mime

from carquinyol import layoutmanager
from carquinyol import migration
from carquinyol.layoutmanager import MAX_QUERY_LIMIT
from carquinyol.metadatastore import MetadataStore
from carquinyol.indexstore import IndexStore
from carquinyol.filestore import FileStore
from carquinyol.optimizer import Optimizer

# the name used by the logger
DS_LOG_CHANNEL = 'org.laptop.sugar.DataStore'

DS_SERVICE = "org.laptop.sugar.DataStore"
DS_DBUS_INTERFACE = "org.laptop.sugar.DataStore"
DS_OBJECT_PATH = "/org/laptop/sugar/DataStore"

logger = logging.getLogger(DS_LOG_CHANNEL)

class DataStore(dbus.service.Object):
    """D-Bus API and logic for connecting all the other components.
    """ 
    def __init__(self, **options):
        bus_name = dbus.service.BusName(DS_SERVICE,
                                        bus=dbus.SessionBus(),
                                        replace_existing=False,
                                        allow_replacement=False)
        dbus.service.Object.__init__(self, bus_name, DS_OBJECT_PATH)

        layout_manager = layoutmanager.get_instance()
        if layout_manager.get_version() == 0:
            migration.migrate_from_0()
            layout_manager.set_version(2)
            layout_manager.index_updated = False
        elif layout_manager.get_version() == 1:
            layout_manager.set_version(2)
            layout_manager.index_updated = False

        self._metadata_store = MetadataStore()

        self._index_store = IndexStore()
        try:
            self._index_store.open_index()
        except Exception:
            logging.error('Failed to open index, will rebuild\n%s' \
                    % traceback.format_exc())
            layout_manager.index_updated = False
            self._index_store.remove_index()
            self._index_store.open_index()

        self._file_store = FileStore()

        if not layout_manager.index_updated:
            logging.debug('Index is not up-to-date, will update')
            self._rebuild_index()

        self._optimizer = Optimizer(self._file_store, self._metadata_store)

    def _rebuild_index(self):
        uids = layoutmanager.get_instance().find_all()
        logging.debug('Going to update the index with uids %r' % uids)
        gobject.idle_add(lambda: self.__rebuild_index_cb(uids),
                            priority=gobject.PRIORITY_LOW)

    def __rebuild_index_cb(self, uids):
        if uids:
            uid = uids.pop()

            logging.debug('Updating entry %r in index. %d to go.' % \
                          (uid, len(uids)))

            if not self._index_store.contains(uid):
                try:
                    props = self._metadata_store.retrieve(uid)
                    self._index_store.store(uid, props)
                except Exception:
                    logging.error('Error processing %r\n%s.' \
                            % (uid, traceback.format_exc()))

        if not uids:
            logging.debug('Finished updating index.')
            layoutmanager.get_instance().index_updated = True
            return False
        else:
            return True

    def _create_completion_cb(self, async_cb, async_err_cb, uid, exc=None):
        logger.debug("_create_completion_cb(%r, %r, %r, %r)" % \
            (async_cb, async_err_cb, uid, exc))
        if exc is not None:
            async_err_cb(exc)
            return

        self.Created(uid)
        self._optimizer.optimize(uid)
        logger.debug("created %s" % uid)
        async_cb(uid)

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='a{sv}sb',
                         out_signature='s',
                         async_callbacks=('async_cb', 'async_err_cb'),
                         byte_arrays=True)
    def create(self, props, file_path, transfer_ownership,
               async_cb, async_err_cb):
        uid = str(uuid.uuid4())
        logging.debug('datastore.create %r' % uid)

        if not props.get('timestamp', ''):
            props['timestamp'] = int(time.time())

        self._metadata_store.store(uid, props)
        self._index_store.store(uid, props)
        self._file_store.store(uid, file_path, transfer_ownership,
                lambda *args: self._create_completion_cb(async_cb,
                                                         async_err_cb,
                                                         uid,
                                                         *args))

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Created(self, uid):
        pass

    def _update_completion_cb(self, async_cb, async_err_cb, uid, exc=None):
        logger.debug("_update_completion_cb() called with %r / %r, exc %r" % \
            (async_cb, async_err_cb, exc))
        if exc is not None:
            async_err_cb(exc)
            return

        self.Updated(uid)
        self._optimizer.optimize(uid)
        logger.debug("updated %s" % uid)
        async_cb()

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='sa{sv}sb',
             out_signature='',
             async_callbacks=('async_cb', 'async_err_cb'),
             byte_arrays=True)
    def update(self, uid, props, file_path, transfer_ownership,
               async_cb, async_err_cb):
        logging.debug('datastore.update %r' % uid)

        if not props.get('timestamp', ''):
            props['timestamp'] = int(time.time())

        self._metadata_store.store(uid, props)
        self._index_store.store(uid, props)

        if os.path.exists(self._file_store.get_file_path(uid)) and \
                (not file_path or os.path.exists(file_path)):
            self._optimizer.remove(uid)
        self._file_store.store(uid, file_path, transfer_ownership,
                lambda *args: self._update_completion_cb(async_cb,
                                                         async_err_cb,
                                                         uid,
                                                         *args))

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Updated(self, uid):
        pass

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='a{sv}as',
             out_signature='aa{sv}u')
    def find(self, query, properties):
        logging.debug('datastore.find %r' % query)
        t = time.time()

        if layoutmanager.get_instance().index_updated:
            try:
                uids, count = self._index_store.find(query)
            except Exception:
                logging.error('Failed to query index, will rebuild\n%s' \
                        % traceback.format_exc())
                layoutmanager.get_instance().index_updated = False
                self._index_store.close_index()
                self._index_store.remove_index()
                self._index_store.open_index()
                self._rebuild_index()

        if not layoutmanager.get_instance().index_updated:
            logging.warning('Index updating, returning all entries')

            uids = layoutmanager.get_instance().find_all()
            count = len(uids)

            offset = query.get('offset', 0)
            limit = query.get('limit', MAX_QUERY_LIMIT)
            uids = uids[offset:offset + limit]

        entries = []
        for uid in uids:
            if os.path.exists(layoutmanager.get_instance().get_entry_path(uid)):
                metadata = self._metadata_store.retrieve(uid, properties)
                entries.append(metadata)
            else:
                logging.debug('Skipping entry %r without metadata dir' % uid)
                count = count - 1
        logger.debug('find(): %r' % (time.time() - t))
        return entries, count

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='s',
             sender_keyword='sender')
    def get_filename(self, uid, sender=None):
        logging.debug('datastore.get_filename %r' % uid)
        user_id = dbus.Bus().get_unix_user(sender)
        extension = self._get_extension(uid)
        return self._file_store.retrieve(uid, user_id, extension)

    def _get_extension(self, uid):
        mime_type = self._metadata_store.get_property(uid, 'mime_type')
        if mime_type is None or not mime_type:
            return ''
        return mime.get_primary_extension(mime_type)

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='s',
                         out_signature='a{sv}')
    def get_properties(self, uid):
        logging.debug('datastore.get_properties %r' % uid)
        metadata = self._metadata_store.retrieve(uid)
        return metadata

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='sa{sv}',
                         out_signature='as')
    def get_uniquevaluesfor(self, propertyname, query=None):
        if propertyname != 'activity':
            raise ValueError('Only ''activity'' is a supported property name')
        if query:
            raise ValueError('The query parameter is not supported')
        if layoutmanager.get_instance().index_updated:
            return self._index_store.get_activities()
        else:
            logging.warning('Index updating, returning an empty list')
            return []

    @dbus.service.method(DS_DBUS_INTERFACE,
             in_signature='s',
             out_signature='')
    def delete(self, uid):
        self._optimizer.remove(uid)

        self._index_store.delete(uid)
        self._file_store.delete(uid)
        self._metadata_store.delete(uid)
        
        entry_path = layoutmanager.get_instance().get_entry_path(uid)
        os.removedirs(entry_path)

        self.Deleted(uid)
        logger.debug("deleted %s" % uid)

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Deleted(self, uid):
        pass

    def stop(self):
        """shutdown the service"""
        self._index_store.close_index()
        self.Stopped()

    @dbus.service.signal(DS_DBUS_INTERFACE)
    def Stopped(self):
        pass

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature="sa{sv}",
                         out_signature='s')
    def mount(self, uri, options=None):
        return ''

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature="",
                         out_signature="aa{sv}")
    def mounts(self):
        return [{'id': 1}]

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature="s",
                         out_signature="")
    def unmount(self, mountpoint_id):
        pass

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="a{sv}")
    def Mounted(self, descriptior):
        pass

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="a{sv}")
    def Unmounted(self, descriptor):
        pass

