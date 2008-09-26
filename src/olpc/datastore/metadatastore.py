import os
import logging
import errno

from olpc.datastore import layoutmanager
from olpc.datastore import metadatareader

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
        return metadatareader.retrieve(dir_path, properties)

    def delete(self, uid):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        metadata_path = os.path.join(dir_path, 'metadata')
        for key in os.listdir(metadata_path):
            os.remove(os.path.join(metadata_path, key))
        os.rmdir(metadata_path)

