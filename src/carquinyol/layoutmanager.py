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
import logging

MAX_QUERY_LIMIT = 40960
CURRENT_LAYOUT_VERSION = 6

class LayoutManager(object):
    """Provide the logic about how entries are stored inside the datastore
    directory
    """

    def __init__(self):
        profile = os.environ.get('SUGAR_PROFILE', 'default')
        base_dir = os.path.join(os.path.expanduser('~'), '.sugar', profile)

        self._root_path = os.path.join(base_dir, 'datastore')

        if not os.path.exists(self._root_path):
            os.makedirs(self._root_path)

        self._create_if_needed(self.get_checksums_dir())
        self._create_if_needed(self.get_queue_path())

    def _create_if_needed(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_version(self):
        version_path = os.path.join(self._root_path, 'version')
        version = 0
        if os.path.exists(version_path):
            try:
                version = int(open(version_path, 'r').read())
            except ValueError:
                logging.exception('Can not read layout version')
                version = 0

        return version

    def set_version(self, version):
        version_path = os.path.join(self._root_path, 'version')
        open(version_path, 'w').write(str(version))

    def get_entry_path(self, uid):
        # os.path.join() is just too slow
        return '%s/%s/%s' % (self._root_path, uid[:2], uid)

    def get_data_path(self, uid):
        return '%s/%s/%s/data' % (self._root_path, uid[:2], uid)

    def get_metadata_path(self, uid):
        return '%s/%s/%s/metadata' % (self._root_path, uid[:2], uid)

    def get_root_path(self):
        return self._root_path

    def get_index_path(self):
        return os.path.join(self._root_path, 'index')

    def get_checksums_dir(self):
        return os.path.join(self._root_path, 'checksums')

    def get_queue_path(self):
        return os.path.join(self.get_checksums_dir(), 'queue')

    def find_all(self):
        uids = []
        for f in os.listdir(self._root_path):
            if os.path.isdir(os.path.join(self._root_path, f)) and len(f) == 2:
                for g in os.listdir(os.path.join(self._root_path, f)):
                    if len(g) == 36:
                        uids.append(g)
        return uids

    def is_empty(self):
        """Check if there is any existing entry.

        All data store layout versions are handled. Will err on the safe
        side (i.e. return False if there might be any entry)."""
        if os.path.exists(os.path.join(self._root_path, 'store')):
            # unmigrated 0.82 data store
            return False

        for f in os.listdir(self._root_path):
            if os.path.isdir(os.path.join(self._root_path, f)) and len(f) == 2:
                for g in os.listdir(os.path.join(self._root_path, f)):
                    if len(g) == 36:
                        return False
        return True

_instance = None
def get_instance():
    global _instance
    if _instance is None:
        _instance = LayoutManager()
    return _instance
