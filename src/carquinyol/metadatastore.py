import os

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
        if not md_path:
            md_path = layoutmanager.get_instance().get_metadata_path(uid)
        # Hack to support activities that still pass properties named as
            # for example title:text.
        if ':' in key:
            key = key.split(':', 1)[0]

        changed = True
        fpath = os.path.join(md_path, key)
        tpath = os.path.join(md_path, '.' + key)
        # FIXME: this codepath handles raw image data
        # str() is 8-bit clean right now, but
        # this won't last. We will need more explicit
        # handling of strings, int/floats vs raw data
        if isinstance(value, bytes):
            value = str(value)[2:-1]
        # avoid pointless writes; replace atomically
        if os.path.exists(fpath):
            stored_val = open(fpath, 'rb').read()
            stored_val = stored_val[2:-1]
            if stored_val == value:
                changed = False
        if changed:
            f = open(tpath, 'w')
            f.write(value)
            f.close()
            os.rename(tpath, fpath)

    def retrieve(self, uid, properties=None):
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        if properties is not None:
            properties = [x.encode('utf-8') if isinstance(x,str) else x for x in properties]
        metadata = metadatareader.retrieve(metadata_path, properties)
        for x in metadata:
            metadata[x] = str(metadata[x])[2:-1]
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
