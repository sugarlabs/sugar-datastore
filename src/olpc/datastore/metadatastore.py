import os
import logging

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
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            raise ValueError('Unknown object: %r' % uid)

        metadata_path = os.path.join(dir_path, 'metadata')
        metadata = self._decode(metadata_path, properties)

        if properties is None or len(properties) != len(metadata):
            extra_metadata_dir = os.path.join(dir_path, 'extra_metadata')
            if os.path.exists(extra_metadata_dir):
                for key in os.listdir(extra_metadata_dir):
                    if properties is not None and key not in properties:
                        continue
                    file_path = os.path.join(extra_metadata_dir, key)
                    if os.path.exists(file_path):
                        # TODO: This class shouldn't know anything about dbus.
                        import dbus
                        metadata[key] = dbus.ByteArray(open(file_path).read())
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
        f.write('v1\n')
        for key, value in metadata.items():
            if not key:
                raise ValueError('Property keys cannot be empty')
            if (' ' in key) or ('\t' in key):
                raise ValueError('Property keys cannot contain tabulators: %r' \
                                 % key)
            if value is None:
                value = ''
            else:
                value = str(value)
            f.write('%s\t%d\t%s\n' % (key, len(value), value))
        f.close()

    def _decode(self, file_path, properties):
        f = open(file_path, 'r')
        version_line = f.readline()
        try:
            version = int(version_line[1:-1])
            if version != 1:
                raise ValueError('Incompatible version %r' % version)
        except:
            logging.error('Invalid version line: %s' % version_line)
            raise

        metadata = {}
        while True:
            line = f.readline()
            if not line:
                break

            key, value_len, value = line.split('\t', 2)
            value_len = int(value_len)

            if len(value) == value_len + 1:
                value = value[:-1] # skip the newline
            else:
                value += f.read(value_len - len(value))
                f.seek(1, 1) # skip the newline

            if properties is None:
                metadata[key] = value
            elif key in properties:
                metadata[key] = value
                if len(properties) == len(metadata):
                    break

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

