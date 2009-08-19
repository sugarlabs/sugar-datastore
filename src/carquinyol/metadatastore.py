import os

from carquinyol import layoutmanager
from carquinyol import metadatareader

MAX_SIZE = 256


class MetadataStore(object):

    def store(self, uid, metadata):
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        if not os.path.exists(metadata_path):
            os.makedirs(metadata_path)
        else:
            for key in os.listdir(metadata_path):
                os.remove(os.path.join(metadata_path, key))

        metadata['uid'] = uid
        for key, value in metadata.items():

            # Hack to support activities that still pass properties named as
            # for example title:text.
            if ':' in key:
                key = key.split(':', 1)[0]

            f = open(os.path.join(metadata_path, key), 'w')
            try:
                if isinstance(value, unicode):
                    value = value.encode('utf-8')
                elif not isinstance(value, basestring):
                    value = str(value)
                f.write(value)
            finally:
                f.close()

    def retrieve(self, uid, properties=None):
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        return metadatareader.retrieve(metadata_path, properties)

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
        metadata_path = layoutmanager.get_instance().get_metadata_path(uid)
        property_path = os.path.join(metadata_path, key)
        open(property_path, 'w').write(value)
