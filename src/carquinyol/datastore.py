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

# pylint fails on @debian's arguments
# pylint: disable=C0322

import logging
import uuid
import time
import os
import shutil
import subprocess
import tempfile

import dbus
import dbus.service
from gi.repository import GLib

from sugar3 import mime

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
MIN_INDEX_FREE_BYTES = 1024 * 1024 * 5

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

        migrated, initiated = self._open_layout()

        self._metadata_store = MetadataStore()
        self._file_store = FileStore()
        self._optimizer = Optimizer(self._file_store, self._metadata_store)
        self._index_store = IndexStore()
        self._index_updating = False

        root_path = layoutmanager.get_instance().get_root_path()
        self._cleanflag = os.path.join(root_path, 'ds_clean')

        if initiated:
            logging.debug('Initiate datastore')
            self._rebuild_index()
            self._index_store.flush()
            self._mark_clean()
            return

        if migrated:
            self._rebuild_index()
            self._mark_clean()
            return

        rebuild = False
        stat = os.statvfs(root_path)
        da = stat.f_bavail * stat.f_bsize

        if not self._index_store.index_updated:
            logging.warn('Index is not up-to-date')
            rebuild = True
        elif not os.path.exists(self._cleanflag):
            logging.warn('DS state is not clean')
            rebuild = True
        elif da < MIN_INDEX_FREE_BYTES:
            logging.warn('Disk space tight for index')
            rebuild = True

        if rebuild:
            logging.warn('Trigger index rebuild')
            self._rebuild_index()
        else:
            # fast path
            try:
                self._index_store.open_index()
            except:
                logging.exception('Failed to open index')
                # try...
                self._rebuild_index()

        self._mark_clean()
        return

    def _mark_clean(self):
        try:
            f = open(self._cleanflag, 'w')
            os.fsync(f.fileno())
            f.close()
        except:
            logging.exception("Could not mark the datastore clean")

    def _mark_dirty(self):
        try:
            os.remove(self._cleanflag)
        except:
            pass

    def _open_layout(self):
        """Open layout manager, check version of data store on disk and
        migrate if necessary.

        Returns a pair of booleans. For the first, True if migration was done
        and an index rebuild is required. For the second, True if datastore was
        just initiated.
        """
        layout_manager = layoutmanager.get_instance()

        if layout_manager.is_empty():
            layout_manager.set_version(layoutmanager.CURRENT_LAYOUT_VERSION)
            return False, True

        old_version = layout_manager.get_version()
        if old_version == layoutmanager.CURRENT_LAYOUT_VERSION:
            return False, False

        if old_version == 0:
            migration.migrate_from_0()

        layout_manager.set_version(layoutmanager.CURRENT_LAYOUT_VERSION)
        return True, False

    def _rebuild_index(self):
        """Remove and recreate index."""
        self._index_store.close_index()
        self._index_store.remove_index()

        # rebuild the index in tmpfs to better handle ENOSPC
        temp_index_path = tempfile.mkdtemp(prefix='sugar-datastore-index-')
        logger.debug('Rebuilding index in %s' % temp_index_path)
        self._index_store.open_index(temp_path=temp_index_path)
        self._update_index()
        self._index_store.close_index()

        on_disk = False

        # can we fit the index on disk? get disk usage in bytes...
        index_du = subprocess.check_output(['/usr/bin/du', '-bs',
                                            temp_index_path])
        index_du = int(index_du.split('\t')[0])
        # disk available, in bytes
        stat = os.statvfs(temp_index_path)
        da = stat.f_bavail * stat.f_bsize
        if da > (index_du * 1.2) and da > MIN_INDEX_FREE_BYTES:
            # 1.2 due to 20% room for growth
            logger.debug('Attempting to move tempfs index to disk')
            # move to internal disk
            try:
                index_path = layoutmanager.get_instance().get_index_path()
                if os.path.exists(index_path):
                    shutil.rmtree(index_path)
                shutil.copytree(temp_index_path, index_path)
                shutil.rmtree(temp_index_path)
                on_disk = True
            except Exception:
                logger.exception('Error copying tempfs index to disk,'
                                 'revert to using tempfs index.')
        else:
            logger.warn("Not enough disk space, using tempfs index")

        if on_disk:
            self._index_store.open_index()
        else:
            self._index_store.open_index(temp_path=temp_index_path)

    def _update_index(self):
        """Find entries that are not yet in the index and add them."""
        uids = layoutmanager.get_instance().find_all()
        logging.debug('Going to update the index with object_ids %r',
                      uids)
        self._index_updating = True
        GLib.idle_add(lambda: self.__update_index_cb(uids),
                         priority=GLib.PRIORITY_LOW)

    def __update_index_cb(self, uids):
        if uids:
            uid = uids.pop()

            logging.debug('Updating entry %r in index. %d to go.', uid,
                          len(uids))

            if not self._index_store.contains(uid):
                try:
                    update_metadata = False
                    props = self._metadata_store.retrieve(uid)
                    if 'filesize' not in props:
                        path = self._file_store.get_file_path(uid)
                        if os.path.exists(path):
                            props['filesize'] = os.stat(path).st_size
                            update_metadata = True
                    if 'timestamp' not in props:
                        props['timestamp'] = str(int(time.time()))
                        update_metadata = True
                    if 'creation_time' not in props:
                        if 'ctime' in props:
                            try:
                                props['creation_time'] = time.mktime(
                                    time.strptime(
                                        props['ctime'],
                                        migration.DATE_FORMAT))
                            except (TypeError, ValueError):
                                pass
                        if 'creation_time' not in props:
                            props['creation_time'] = props['timestamp']
                        update_metadata = True
                    if update_metadata:
                        self._metadata_store.store(uid, props)
                    self._index_store.store(uid, props)
                except Exception:
                    logging.exception('Error processing %r', uid)
                    logging.warn('Will attempt to delete corrupt entry %r',
                                 uid)
                    try:
                        # self.delete(uid) only works on well-formed
                        # entries :-/
                        entry_path = \
                            layoutmanager.get_instance().get_entry_path(uid)
                        shutil.rmtree(entry_path)
                    except Exception:
                        logging.exception('Error deleting corrupt entry %r',
                                          uid)

        if not uids:
            self._index_store.flush()
            self._index_updating = False
            logging.debug('Finished updating index.')
            return False
        else:
            return True

    def _create_completion_cb(self, async_cb, async_err_cb, uid, exc=None):
        logger.debug('_create_completion_cb(%r, %r, %r, %r)', async_cb,
                     async_err_cb, uid, exc)
        if exc is not None:
            async_err_cb(exc)
            return

        self.Created(uid)
        self._optimizer.optimize(uid)
        logger.debug('created %s', uid)
        self._mark_clean()
        async_cb(uid)

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='a{sv}sb',
                         out_signature='s',
                         async_callbacks=('async_cb', 'async_err_cb'),
                         byte_arrays=True)
    def create(self, props, file_path, transfer_ownership,
               async_cb, async_err_cb):
        uid = str(uuid.uuid4())
        logging.debug('datastore.create %r', uid)

        self._mark_dirty()

        if not props.get('timestamp', ''):
            props['timestamp'] = int(time.time())

        # FIXME: Support for the deprecated ctime property. Remove in 0.92.
        if 'ctime' in props:
            try:
                props['creation_time'] = time.mktime(time.strptime(
                    migration.DATE_FORMAT, props['ctime']))
            except (TypeError, ValueError):
                pass

        if 'creation_time' not in props:
            props['creation_time'] = props['timestamp']

        if os.path.exists(file_path):
            stat = os.stat(file_path)
            props['filesize'] = stat.st_size
        else:
            props['filesize'] = 0

        self._metadata_store.store(uid, props)
        self._index_store.store(uid, props)
        self._file_store.store(
            uid, file_path, transfer_ownership,
            lambda * args: self._create_completion_cb(async_cb,
                                                      async_err_cb,
                                                      uid, * args))

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Created(self, uid):
        pass

    def _update_completion_cb(self, async_cb, async_err_cb, uid, exc=None):
        logger.debug('_update_completion_cb() called with %r / %r, exc %r',
                     async_cb, async_err_cb, exc)
        if exc is not None:
            async_err_cb(exc)
            return

        self.Updated(uid)
        self._optimizer.optimize(uid)
        logger.debug('updated %s', uid)
        self._mark_clean()
        async_cb()

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='sa{sv}sb',
                         out_signature='',
                         async_callbacks=('async_cb', 'async_err_cb'),
                         byte_arrays=True)
    def update(self, uid, props, file_path, transfer_ownership,
               async_cb, async_err_cb):
        logging.debug('datastore.update %r', uid)

        self._mark_dirty()

        if not props.get('timestamp', ''):
            props['timestamp'] = int(time.time())

        # FIXME: Support for the deprecated ctime property. Remove in 0.92.
        if 'ctime' in props:
            try:
                props['creation_time'] = time.mktime(time.strptime(
                    migration.DATE_FORMAT, props['ctime']))
            except (TypeError, ValueError):
                pass

        if 'creation_time' not in props:
            props['creation_time'] = props['timestamp']

        if file_path:
            # Empty file_path means skipping storage stage, see filestore.py
            # TODO would be more useful to update filesize after real file save
            if os.path.exists(file_path):
                stat = os.stat(file_path)
                props['filesize'] = stat.st_size
            else:
                props['filesize'] = 0

        self._metadata_store.store(uid, props)
        self._index_store.store(uid, props)

        if os.path.exists(self._file_store.get_file_path(uid)) and \
                (not file_path or os.path.exists(file_path)):
            self._optimizer.remove(uid)
        self._file_store.store(
            uid, file_path, transfer_ownership,
            lambda * args: self._update_completion_cb(async_cb,
                                                      async_err_cb,
                                                      uid, * args))

    @dbus.service.signal(DS_DBUS_INTERFACE, signature="s")
    def Updated(self, uid):
        pass

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='a{sv}as',
                         out_signature='aa{sv}u')
    def find(self, query, properties):
        logging.debug('datastore.find %r', query)
        t = time.time()

        if not self._index_updating:
            try:
                uids, count = self._index_store.find(query)
            except Exception:
                logging.exception('Failed to query index, will rebuild')
                self._rebuild_index()

        if self._index_updating:
            logging.warning('Index updating, returning all entries')
            return self._find_all(query, properties)

        entries = []
        for uid in uids:
            entry_path = layoutmanager.get_instance().get_entry_path(uid)
            if not os.path.exists(entry_path):
                logging.warning(
                    'Inconsistency detected, returning all entries')
                self._rebuild_index()
                return self._find_all(query, properties)

            metadata = self._metadata_store.retrieve(uid, properties)
            self._fill_internal_props(metadata, uid, properties)
            entries.append(metadata)

        logger.debug('find(): %r', time.time() - t)

        return entries, count

    def _find_all(self, query, properties):
        uids = layoutmanager.get_instance().find_all()
        count = len(uids)

        offset = query.get('offset', 0)
        limit = query.get('limit', MAX_QUERY_LIMIT)
        uids = uids[offset:offset + limit]

        entries = []
        for uid in uids:
            metadata = self._metadata_store.retrieve(uid, properties)
            self._fill_internal_props(metadata, uid, properties)
            entries.append(metadata)

        return entries, count

    def _fill_internal_props(self, metadata, uid, names=None):
        """Fill in internal / computed properties in metadata

        Properties are only set if they appear in names or if names is
        empty.
        """
        if not names or 'uid' in names:
            metadata['uid'] = uid

        if not names or 'filesize' in names:
            file_path = self._file_store.get_file_path(uid)
            if os.path.exists(file_path):
                stat = os.stat(file_path)
                metadata['filesize'] = str(stat.st_size)
            else:
                metadata['filesize'] = '0'

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='a{sv}',
                         out_signature='as')
    def find_ids(self, query):
        if not self._index_updating:
            try:
                return self._index_store.find(query)[0]
            except Exception:
                logging.error('Failed to query index, will rebuild')
                self._rebuild_index()
        return []

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='s',
                         out_signature='s',
                         sender_keyword='sender')
    def get_filename(self, uid, sender=None):
        logging.debug('datastore.get_filename %r', uid)
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
        logging.debug('datastore.get_properties %r', uid)
        metadata = self._metadata_store.retrieve(uid)
        self._fill_internal_props(metadata, uid)
        return metadata

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='sa{sv}',
                         out_signature='as')
    def get_uniquevaluesfor(self, propertyname, query=None):
        if propertyname != 'activity':
            raise ValueError('Only ''activity'' is a supported property name')
        if query:
            raise ValueError('The query parameter is not supported')
        if not self._index_updating:
            return self._index_store.get_activities()
        else:
            logging.warning('Index updating, returning an empty list')
            return []

    @dbus.service.method(DS_DBUS_INTERFACE,
                         in_signature='s',
                         out_signature='')
    def delete(self, uid):
        self._mark_dirty()
        try:
            entry_path = layoutmanager.get_instance().get_entry_path(uid)
            self._optimizer.remove(uid)
            self._index_store.delete(uid)
            self._file_store.delete(uid)
            self._metadata_store.delete(uid)
            # remove the dirtree
            shutil.rmtree(entry_path)
            try:
                # will remove the hashed dir if nothing else is there
                os.removedirs(os.path.dirname(entry_path))
            except:
                pass
        except:
            logger.exception('Exception deleting entry')
            raise

        self.Deleted(uid)
        logger.debug('deleted %s', uid)
        self._mark_clean()

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
