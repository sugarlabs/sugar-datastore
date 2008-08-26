import os

class LayoutManager(object):
    def __init__(self):
        profile = os.environ.get('SUGAR_PROFILE', 'default')
        base_dir = os.path.join(os.path.expanduser('~'), '.sugar', profile)

        self._root_path = os.path.join(base_dir, 'datastore2')

        self._create_if_needed(self._root_path)
        self._create_if_needed(self.get_checksums_dir())
        self._create_if_needed(self.get_queue_path())

    def _create_if_needed(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get_entry_path(self, uid):
        return os.path.join(self._root_path, uid[:2], uid)

    def get_index_path(self):
        return os.path.join(self._root_path, 'index')

    def get_checksums_dir(self):
        return os.path.join(self._root_path, 'checksums')
 
    def get_queue_path(self):
        return os.path.join(self.get_checksums_dir(), 'queue')

_instance = None
def get_instance():
    global _instance
    if _instance is None:
        _instance = LayoutManager()
    return _instance

