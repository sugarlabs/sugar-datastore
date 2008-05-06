import os
import time
import errno
import logging

import gobject

class FileStore(object):
    def store(self, uid, file_path, transfer_ownership, dir_path, completion_cb):
        destination_path = os.path.join(dir_path, uid)
        if os.path.exists(file_path):
            if transfer_ownership:
                try:
                    os.rename(file_path, destination_path)
                    completion_cb()
                except OSError, e:
                   if e.errno == errno.EXDEV:
                       async_copy = AsyncCopy(file_path, destination_path, completion_cb)
                       async_copy.start()
                   else:
                       raise
            else:
                raise NotImplementedError()
        elif file_path == '' and os.path.exists(destination_path):
            os.remove(destination_path)
            completion_cb()
        else:
            completion_cb()

    def _copy_completion_cb(self, completion_cb, exc=None):
        completion_cb(exc)

    def retrieve(self, uid, dir_path, user_id):
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
            destination_dir = os.path.join(os.path.expanduser('~'), '.sugar', profile,
                                           'data')
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

    def delete(self, uid, dir_path):
        file_path = os.path.join(dir_path, uid)
        if os.path.exists(file_path):
            os.remove(file_path)

class AsyncCopy:
    CHUNK_SIZE=65536

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
                logging.error("AC: Error writing %s -> %s: wrote less than expected" % \
                              (self.src, self.dest))
                self._cleanup()
                self.completion(RuntimeError("Error writing data to destination file"))
                return False

            # FIXME: emit progress here

            # done?
            if len(data) < AsyncCopy.CHUNK_SIZE:
                self._cleanup()
                self.completion(None)
                return False
        except Exception, err:
            logging.error("AC: Error copying %s -> %s: %r" % (self.src, self.dest, err))
            self._cleanup()
            self.completion(err)
            return False

        return True

    def start(self):
        self.src_fp = os.open(self.src, os.O_RDONLY)
        self.dest_fp = os.open(self.dest, os.O_RDWR | os.O_TRUNC | os.O_CREAT, 0644)

        stat = os.fstat(self.src_fp)
        self.size = stat[6]

        self.tstart = time.time()
        sid = gobject.idle_add(self._copy_block)

