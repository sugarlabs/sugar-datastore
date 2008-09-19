import os
import errno
import logging

import gobject

from olpc.datastore import layoutmanager

class FileStore(object):
    def __init__(self): 
        self._enqueue_checksum_id = None

        # TODO: add protection against store and retrieve operations on entries
        # that are being processed async.

    def store(self, uid, file_path, transfer_ownership, completion_cb):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        destination_path = os.path.join(dir_path, uid)
        if file_path:
            if not os.path.isfile(file_path):
                raise ValueError('No file at %r' % file_path)
            if transfer_ownership:
                try:
                    logging.debug('FileStore moving from %r to %r' % \
                                  (file_path, destination_path))
                    os.rename(file_path, destination_path)
                    self._enqueue_checksum(uid)
                    completion_cb()
                except OSError, e:
                    if e.errno == errno.EXDEV:
                        self._async_copy(uid, file_path, destination_path,
                                completion_cb)
                    else:
                        raise
            else:
                self._async_copy(uid, file_path, destination_path,
                        completion_cb)
        elif not file_path and os.path.exists(destination_path):
            os.remove(destination_path)
            completion_cb()
        else:
            logging.debug('FileStore: Nothing to do')
            completion_cb()

    def _async_copy(self, uid, file_path, destination_path, completion_cb):
        logging.debug('FileStore copying from %r to %r' % \
                      (file_path, destination_path))
        async_copy = AsyncCopy(file_path, destination_path,
                lambda: self._async_copy_completion_cb(uid, completion_cb))
        async_copy.start()

    def _async_copy_completion_cb(self, uid, completion_cb):
        self._enqueue_checksum(uid)
        completion_cb()

    def _enqueue_checksum(self, uid):
        queue_path = layoutmanager.get_instance().get_queue_path()
        open(os.path.join(queue_path, uid), 'w').close()
        logging.debug('_enqueue_checksum %r' % os.path.join(queue_path, uid))
        if self._enqueue_checksum_id is None:
            self._enqueue_checksum_id = \
                    gobject.idle_add(self._compute_checksum_cb,
                                     priority=gobject.PRIORITY_LOW)

    def _identical_file_already_exists(self, checksum):
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        return os.path.exists(checksum_path)

    def _get_file_from_checksum(self, checksum):
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        first_file_link = os.listdir(checksum_path)[0]
        first_file = os.readlink(os.path.join(checksum_path, first_file_link))
        return first_file

    def _create_checksum_dir(self, checksum):
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        logging.debug('create dir %r' % checksum_path)
        os.mkdir(checksum_path)

    def _add_checksum_entry(self, uid, checksum):
        entry_path = layoutmanager.get_instance().get_entry_path(uid)
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)

        logging.debug('symlink %r -> %r' % (os.path.join(entry_path, uid),
                                            os.path.join(checksum_path, uid)))
        os.symlink(os.path.join(entry_path, uid),
                   os.path.join(checksum_path, uid))

        logging.debug('symlink %r -> %r' % \
                (checksum_path, os.path.join(entry_path, 'checksum')))
        os.symlink(checksum_path, os.path.join(entry_path, 'checksum'))

    def _remove_checksum_entry(self, uid):
        entry_path = layoutmanager.get_instance().get_entry_path(uid)
        checksum = os.readlink(os.path.join(entry_path, 'checksum'))

        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)

        os.remove(os.path.join(checksum_path, uid))
        try:
            os.rmdir(checksum_path)
        except OSError, e:
            if e.errno != errno.ENOTEMPTY:
                raise

        os.remove(os.path.join(entry_path, 'checksum'))

    def _already_linked(self, uid, checksum):
        checksums_dir = layoutmanager.get_instance().get_checksums_dir()
        checksum_path = os.path.join(checksums_dir, checksum)
        return os.path.exists(os.path.join(checksum_path, uid))

    def _compute_checksum_cb(self):
        queue_path = layoutmanager.get_instance().get_queue_path()
        queue = os.listdir(queue_path)
        if queue:
            uid = queue[0]
            logging.debug('_compute_checksum_cb processing %r' % uid)
            entry_path = layoutmanager.get_instance().get_entry_path(uid)
            file_in_entry_path = os.path.join(entry_path, uid)
            checksum = self._calculate_md5sum(os.path.join(entry_path, uid))

            if self._identical_file_already_exists(checksum):
                if not self._already_linked(uid, checksum):
                    logging.debug('delete %r' % file_in_entry_path)
                    os.remove(file_in_entry_path)

                    existing_file = self._get_file_from_checksum(checksum)
                    logging.debug('link %r -> %r' % \
                            (existing_file, file_in_entry_path))
                    os.link(existing_file, file_in_entry_path)

                    self._add_checksum_entry(uid, checksum)
            else:
                self._create_checksum_dir(checksum)
                self._add_checksum_entry(uid, checksum)

            os.remove(os.path.join(queue_path, uid))

        if len(queue) <= 1:
            self._enqueue_checksum_id = None
            return False
        else:
            return True

    def _calculate_md5sum(self, path):
        in_, out = os.popen2(['md5sum', path])
        return out.read().split(' ', 1)[0]

    def retrieve(self, uid, user_id):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        file_path = os.path.join(dir_path, uid)
        if not os.path.exists(file_path):
            return ''

        use_instance_dir = os.path.exists('/etc/olpc-security') and \
                           os.getuid() != user_id
        if use_instance_dir:
            if not user_id:
                raise ValueError("Couldn't determine the current user uid.")
            destination_dir = os.path.join(os.environ['HOME'], 'isolation', '1',
                                           'uid_to_instance_dir', str(user_id))
        else:
            profile = os.environ.get('SUGAR_PROFILE', 'default')
            destination_dir = os.path.join(os.path.expanduser('~'), '.sugar',
                    profile, 'data')
            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir)

        destination_path = os.path.join(destination_dir, uid)

        # Try to make the original file readable. This can fail if the file is
        # in FAT filesystem.
        try:
            os.chmod(file_path, 0604)
        except OSError, e:
            if e.errno != errno.EPERM:
                raise

        # Try to hard link from the original file to the targetpath. This can
        # fail if the file is in a different filesystem. Do a symlink instead.
        try:
            os.link(file_path, destination_path)
        except OSError, e:
            if e.errno == errno.EXDEV:
                os.symlink(file_path, destination_path)
            else:
                raise

        return destination_path

    def delete(self, uid):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        file_path = os.path.join(dir_path, uid)
        if os.path.exists(file_path):
            self._remove_checksum_entry(uid)
            os.remove(file_path)

class AsyncCopy:
    CHUNK_SIZE = 65536

    def __init__(self, src, dest, completion):
        self.src = src
        self.dest = dest
        self.completion = completion
        self.src_fp = -1
        self.dest_fp = -1
        self.written = 0
        self.size = 0

    def _cleanup(self):
        os.close(self.src_fp)
        os.close(self.dest_fp)

    def _copy_block(self, user_data=None):
        try:
            data = os.read(self.src_fp, AsyncCopy.CHUNK_SIZE)
            count = os.write(self.dest_fp, data)
            self.written += len(data)

            # error writing data to file?
            if count < len(data):
                logging.error('AC: Error writing %s -> %s: wrote less than '
                        'expected' % (self.src, self.dest))
                self._cleanup()
                self.completion(RuntimeError(
                        'Error writing data to destination file'))
                return False

            # FIXME: emit progress here

            # done?
            if len(data) < AsyncCopy.CHUNK_SIZE:
                self._cleanup()
                self.completion(None)
                return False
        except Exception, err:
            logging.error("AC: Error copying %s -> %s: %r" % \
                    (self.src, self.dest, err))
            self._cleanup()
            self.completion(err)
            return False

        return True

    def start(self):
        self.src_fp = os.open(self.src, os.O_RDONLY)
        self.dest_fp = os.open(self.dest, os.O_RDWR | os.O_TRUNC | os.O_CREAT,
                0644)

        stat = os.fstat(self.src_fp)
        self.size = stat[6]

        gobject.idle_add(self._copy_block)

