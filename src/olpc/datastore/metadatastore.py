import os
import logging
import urllib

from olpc.datastore import layoutmanager

MAX_SIZE = 256

class MetadataStore(object):
    def store(self, uid, metadata):
        metadata = metadata.copy()

        for key in metadata.keys():
            if ' ' in key:
                raise ValueError('Property names cannot include spaces. '
                                 'Wrong name: %r' % key)

        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for key, value in metadata.items():
            if isinstance(value, str) and \
                    (len(value) > MAX_SIZE or not self._is_unicode(value)):
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
        f = open(file_path, 'w')
        for key, value in metadata.items():
            if value is None:
                value = ''
            f.write('%s %s\n' % (key, urllib.quote(str(value))))
        f.close()

    def _decode(self, file_path):
        f = open(file_path, 'r')
        metadata = {}
        for line in f.readlines():
            key, value = line.split(' ', 1)
            value = value[:-1] # Take out the trailing '\n'
            value = self._cast_for_journal(key, urllib.unquote(value))
            metadata[key] = value
        f.close()
        return metadata

    def _cast_for_journal(self, key, value):
        # Hack because the current journal expects these properties to have some
        # predefined types
        if key in ['timestamp', 'keep']:
            try:
                return int(value)
            except ValueError:
                return value
        else:
            return value

