import os
import logging

try:
    import cjson
    has_cjson = True
except ImportError:
    import simplejson
    has_cjson = False

from olpc.datastore import layoutmanager

MAX_SIZE = 256

class MetadataStore(object):
    def store(self, uid, metadata):
        metadata = metadata.copy()

        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for key, value in metadata.items():
            if isinstance(value, str) and len(value) > MAX_SIZE:
                self._write_external(key, value, dir_path)
                del metadata[key]
            elif isinstance(value, str) and not self._is_unicode(value):
                self._write_external(key, value, dir_path)
                del metadata[key]

        metadata['uid'] = uid
        self._encode(metadata, os.path.join(dir_path, 'metadata'))

    def retrieve(self, uid, properties=None):
        import time
        t = time.time()

        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            raise ValueError('Unknown object: %r' % uid)

        metadata_path = os.path.join(dir_path, 'metadata')
        if os.path.isfile(metadata_path):
            metadata = self._decode(os.path.join(dir_path, 'metadata'))
        else:
            metadata = {}

        if properties:
            for key, value_ in metadata.items():
                if key not in properties:
                    del metadata[key]
        
        extra_metadata_dir = os.path.join(dir_path, 'extra_metadata')
        if os.path.isdir(extra_metadata_dir):
            for key in os.listdir(extra_metadata_dir):
                if properties and key not in properties:
                    continue
                file_path = os.path.join(extra_metadata_dir, key)
                if not os.path.isdir(file_path):
                    # TODO: This class shouldn't know anything about dbus.
                    import dbus
                    metadata[key] = dbus.ByteArray(open(file_path).read())

        logging.debug('retrieve metadata: %r' % (time.time() - t))
        return metadata

    def delete(self, uid):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        metadata_path = os.path.join(dir_path, 'metadata')
        if os.path.isfile(metadata_path):
            os.remove(os.path.join(dir_path, 'metadata'))
        else:
            logging.warning('%s is not a valid path' % metadata_path)

        extra_metadata_path = os.path.join(dir_path, 'extra_metadata')
        if os.path.isdir(extra_metadata_path):
            for key in os.listdir(extra_metadata_path):
                os.remove(os.path.join(extra_metadata_path, key))
            os.rmdir(os.path.join(dir_path, 'extra_metadata'))
        else:
            logging.warning('%s is not a valid path' % extra_metadata_path)

    def _write_external(self, key, value, dir_path):
        extra_metadata_dir = os.path.join(dir_path, 'extra_metadata')
        if not os.path.exists(extra_metadata_dir):
            os.makedirs(extra_metadata_dir)
        f = open(os.path.join(extra_metadata_dir, key), 'w')
        f.write(value)
        f.close()

    def _is_unicode(self, string):
        try:
            string.decode('utf-8')
            return True
        except UnicodeDecodeError:
            return False

    def _encode(self, metadata, file_path):
        if has_cjson:
            f = open(file_path, 'w')
            f.write(cjson.encode(metadata))
            f.close()
        else:
            simplejson.dump(metadata, open(file_path, 'w'))

    def _decode(self, file_path):
        if has_cjson:
            f = open(file_path, 'r')
            metadata = cjson.decode(f.read())
            f.close()
            return metadata
        else:
            return simplejson.load(open(file_path))

