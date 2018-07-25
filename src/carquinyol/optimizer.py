# Copyright (C) 2008, One Laptop Per Child
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

import os
import errno
import logging
import subprocess

from gi.repository import GLib

from carquinyol import layoutmanager


class Optimizer(object):
    """Optimizes disk space usage by detecting duplicates and sharing storage.
    """

    def __init__(self, file_store, metadata_store):
        self._file_store = file_store
        self._metadata_store = metadata_store
        self._enqueue_checksum_id = None

    def optimize(self, uid):
        """Add an entry to a queue of entries to be checked for duplicates.

        """
        if not os.path.exists(self._file_store.get_file_path(uid)):
            return

        queue_path = layoutmanager.get_instance().get_queue_path()
        open(os.path.join(queue_path, uid), 'w').close()
        logging.debug('optimize %r', os.path.join(queue_path, uid))

        if self._enqueue_checksum_id is None:
            self._enqueue_checksum_id = \
                GLib.idle_add(self._process_entry_cb,
                                 priority=GLib.PRIORITY_LOW)

    def remove(self, uid):
        """Remove any structures left from space optimization

        """
        checksum = self._metadata_store.get_property(uid, 'checksum')
        if checksum is None:
            return

        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        checksum_entry_path = os.path.join(checksum_path, uid)

        if os.path.exists(checksum_entry_path):
            logging.debug('remove %r', checksum_entry_path)
            os.remove(checksum_entry_path)

        if os.path.exists(checksum_path):
            try:
                os.rmdir(checksum_path)
                logging.debug('removed %r', checksum_path)
            except OSError, e:
                if e.errno != errno.ENOTEMPTY:
                    raise

    def _identical_file_already_exists(self, checksum):
        """Check if we already have files with this checksum.

        """
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        return os.path.exists(checksum_path)

    def _get_uid_from_checksum(self, checksum):
        """Get an existing entry which file matches checksum.

        """
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        first_uid = os.listdir(checksum_path)[0]
        return first_uid

    def _create_checksum_dir(self, checksum):
        """Create directory that tracks files with this same checksum.

        """
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        logging.debug('create dir %r', checksum_path)
        os.mkdir(checksum_path)

    def _add_checksum_entry(self, uid, checksum):
        """Create a file in the checksum dir with the uid of the entry

        """
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)

        logging.debug('touch %r', os.path.join(checksum_path, uid))
        open(os.path.join(checksum_path, uid), 'w').close()

    def _already_linked(self, uid, checksum):
        """Check if this entry's file is already a hard link to the checksums
           dir.

        """
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        return os.path.exists(os.path.join(checksum_path, uid))

    def _process_entry_cb(self):
        """Process one item in the checksums queue by calculating its checksum,
           checking if there exist already an identical file, and in that case
           substituting its file with a hard link to that pre-existing file.

        """
        queue_path = layoutmanager.get_instance().get_queue_path()
        queue = os.listdir(queue_path)
        if queue:
            uid = queue[0]
            logging.debug('_process_entry_cb processing %r', uid)

            file_in_entry_path = self._file_store.get_file_path(uid)
            if not os.path.exists(file_in_entry_path):
                logging.info('non-existent entry in queue: %r', uid)
            else:
                checksum = self._calculate_md5sum(file_in_entry_path)
                self._metadata_store.set_property(uid, 'checksum', checksum)

                if self._identical_file_already_exists(checksum):
                    if not self._already_linked(uid, checksum):
                        existing_entry_uid = \
                                self._get_uid_from_checksum(checksum)

                        self._file_store.hard_link_entry(uid,
                                                         existing_entry_uid)

                        self._add_checksum_entry(uid, checksum)
                else:
                    self._create_checksum_dir(checksum)
                    self._add_checksum_entry(uid, checksum)

            os.remove(os.path.join(queue_path, uid))

        if len(queue) <= 1:
            self._enqueue_checksum_id = None
            return False
        else:
            return True

    def _calculate_md5sum(self, path):
        """Calculate the md5 checksum of a given file.

        """
        popen = subprocess.Popen(['md5sum', path], stdout=subprocess.PIPE)
        stdout, __ = popen.communicate()
        return str(stdout).split(' ', 1)[0]
