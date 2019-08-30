import os
import dbus

from carquinyol import layoutmanager
from carquinyol import metadatareader

MAX_SIZE = 256
_INTERNAL_KEYS = ['checksum']


class MetadataStore(object):

    def store(self, uid, metadata):
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        if not os.path.exists(metadata_path):
            os.makedirs(metadata_path)
        else:
            received_keys = list(metadata.keys())
            for key in os.listdir(metadata_path):
                if key not in _INTERNAL_KEYS and key not in received_keys:
                    os.remove(os.path.join(metadata_path, key))

        metadata['uid'] = uid
        for key, value in list(metadata.items()):
            self._set_property(uid, key, value, md_path=metadata_path)

    def _set_property(self, uid, key, value, md_path=False):
        """Set a property in metadata store

        Value datatypes are almost entirely dbus.String, with
        exceptions for certain keys as follows;

        * "timestamp", and "creation_time" of dbus.Int32,
        * "preview" of dbus.ByteArray,
        * "filesize" of int, and
        * "checksum" of str.
        """
        if not md_path:
            md_path = layoutmanager.get_instance().get_metadata_path(uid)
        # Hack to support activities that still pass properties named as
            # for example title:text.
        if ':' in key:
            key = key.split(':', 1)[0]

        changed = True
        fpath = os.path.join(md_path, key)
        tpath = os.path.join(md_path, '.' + key)

        if isinstance(value, int):  # int or dbus.Int32
            value = str(value).encode()
        elif isinstance(value, str):  # str or dbus.String
            value = value.encode()

        # avoid pointless writes; replace atomically
        if os.path.exists(fpath):
            f = open(fpath, 'rb')
            stored_val = f.read()
            f.close()
            if stored_val == value:
                changed = False
        if changed:
            f = open(tpath, 'wb')
            f.write(value)
            f.close()
            os.rename(tpath, fpath)

    def retrieve(self, uid, properties=None):
        """Retrieve metadata for an object from the store.

        Values are read as dbus.ByteArray, then converted to expected
        types.
        """
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)

        if properties is not None:
            properties = [x.encode('utf-8') if isinstance(x, str)
                          else x for x in properties]

        metadata = metadatareader.retrieve(metadata_path, properties)

        # convert from dbus.ByteArray to expected types
        for key, value in metadata.items():
            if key in ['filesize', 'creation_time', 'timestamp']:
                metadata[key] = dbus.Int32(value)
            elif key in ['checksum']:
                metadata[key] = value.decode()
            elif key != 'preview':
                metadata[key] = dbus.String(value.decode())

        return metadata

    def delete(self, uid):
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        for key in os.listdir(metadata_path):
            os.remove(os.path.join(metadata_path, key))
        os.rmdir(metadata_path)

    def get_property(self, uid, key):
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        property_path = os.path.join(metadata_path, key)
        if os.path.exists(property_path):
            return open(property_path, 'r').read()
        else:
            return None

    def set_property(self, uid, key, value):
        self._set_property(uid, key, value)
