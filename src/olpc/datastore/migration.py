import os
import logging
import traceback
import sys
import shutil

import cjson

from olpc.datastore import layoutmanager

def migrate_from_0():
    logging.info('Migrating datastore from version 0 to version 1')
    root_path = layoutmanager.get_instance().get_root_path()
    old_root_path = os.path.join(root_path, 'store')
    for f in os.listdir(old_root_path):
        uid, ext = os.path.splitext(f)
        if ext != '.metadata':
            continue

        logging.debug('Migrating entry %r' % uid)
        try:
            _migrate_metadata(root_path, old_root_path, uid)
            _migrate_file(root_path, old_root_path, uid)
            _migrate_preview(root_path, old_root_path, uid)
        except Exception:
            #logging.warning('Failed to migrate entry %r:%s\n' %(uid, 
            #    ''.join(traceback.format_exception(*sys.exc_info()))))
            #
            # In production, we may choose to ignore errors when failing to
            # migrate some entries. But for now, raise them.
            raise

    # Just be paranoid, it's cheap.
    if old_root_path.endswith('datastore/store'):
        shutil.rmtree(old_root_path)

    logging.info('Migration finished')

def _migrate_metadata(root_path, old_root_path, uid):
    dir_path = layoutmanager.get_instance().get_entry_path(uid)
    metadata_path = os.path.join(dir_path, 'metadata')
    os.makedirs(metadata_path)

    old_metadata_path = os.path.join(old_root_path, uid + '.metadata')
    metadata = cjson.decode(open(old_metadata_path, 'r').read())
    for key, value in metadata.items():
        f = open(os.path.join(metadata_path, key), 'w')
        try:
            f.write(str(value))
        finally:
            f.close()

def _migrate_file(root_path, old_root_path, uid):
    if os.path.exists(os.path.join(old_root_path, uid)):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        os.rename(os.path.join(old_root_path, uid),
                  os.path.join(dir_path, 'data'))

def _migrate_preview(root_path, old_root_path, uid):
    dir_path = layoutmanager.get_instance().get_entry_path(uid)
    metadata_path = os.path.join(dir_path, 'metadata')
    os.rename(os.path.join(old_root_path, 'preview', uid),
              os.path.join(metadata_path, 'preview'))

