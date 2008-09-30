import os

MAX_QUERY_LIMIT = 40960

class LayoutManager(object):
    def __init__(self):
        profile = os.environ.get('SUGAR_PROFILE', 'default')
        base_dir = os.path.join(os.path.expanduser('~'), '.sugar', profile)

        self._root_path = os.path.join(base_dir, 'datastore')

        if not os.path.exists(self._root_path):
            os.makedirs(self._root_path)
            self.set_version(1)

        self._create_if_needed(self.get_checksums_dir())
        self._create_if_needed(self.get_queue_path())

        index_updated_path = os.path.join(self._root_path, 'index_updated')
        self._index_updated = os.path.exists(index_updated_path)

    def _create_if_needed(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_version(self):
        version_path = os.path.join(self._root_path, 'version')
        version = 0
        if os.path.exists(version_path):
            version = int(open(version_path, 'r').read())
        return version

    def set_version(self, version):
        version_path = os.path.join(self._root_path, 'version')
        open(version_path, 'w').write(str(version))

    def get_entry_path(self, uid):
        # os.path.join() is just too slow
        return '%s/%s/%s' % (self._root_path, uid[:2], uid)

    def get_root_path(self):
        return self._root_path

    def get_index_path(self):
        return os.path.join(self._root_path, 'index')

    def get_checksums_dir(self):
        return os.path.join(self._root_path, 'checksums')
 
    def get_queue_path(self):
        return os.path.join(self.get_checksums_dir(), 'queue')

    def _is_index_updated(self):
        return self._index_updated

    def _set_index_updated(self, index_updated):
        if index_updated != self._index_updated:
            self._index_updated = index_updated

            index_updated_path = os.path.join(self._root_path, 'index_updated')
            if os.path.exists(index_updated_path):
                os.remove(index_updated_path)
            else:
                open(index_updated_path, 'w').close()

    index_updated = property(_is_index_updated, _set_index_updated)

    def find_all(self):
        uids = []
        for f in os.listdir(self._root_path):
            if os.path.isdir(os.path.join(self._root_path, f)) and len(f) == 2:
                for g in os.listdir(os.path.join(self._root_path, f)):
                    if len(g) == 36:
                        uids.append(g)
        return uids

_instance = None
def get_instance():
    global _instance
    if _instance is None:
        _instance = LayoutManager()
    return _instance

