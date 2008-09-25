import os
import logging
import errno

from olpc.datastore import layoutmanager

MAX_SIZE = 256

class MetadataStore(object):
    def store(self, uid, metadata):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        metadata_path = os.path.join(dir_path, 'metadata')
        if not os.path.exists(metadata_path):
            os.makedirs(metadata_path)

        metadata['uid'] = uid
        for key, value in metadata.items():
            f = open(os.path.join(metadata_path, key), 'w+')
            try:
                f.write(str(value))
            finally:
                f.close()

    def retrieve(self, uid, properties=None):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            raise ValueError('Unknown object: %r' % uid)

        metadata_path = os.path.join(dir_path, 'metadata')
        metadata = {}
        if properties is None or not properties:
            properties = os.listdir(metadata_path)

        for key in properties:
            property_path = metadata_path + '/' + key
            try:
                value = open(property_path, 'r').read()
            except IOError, e:
                if e.errno != errno.ENOENT:
                    raise
            else:
                if not value:
                    metadata[key] = ''
                else:
                    # TODO: This class shouldn't know anything about dbus.
                    import dbus
                    metadata[key] = dbus.ByteArray(value)
            
        return metadata

    def delete(self, uid):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        metadata_path = os.path.join(dir_path, 'metadata')
        for key in os.listdir(metadata_path):
            os.remove(os.path.join(metadata_path, key))
        os.rmdir(metadata_path)

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

