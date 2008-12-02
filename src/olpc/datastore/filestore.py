# Copyright (C) 2008, One Laptop Per Child
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import os
import errno
import logging
import tempfile

import gobject

from olpc.datastore import layoutmanager

class FileStore(object):
    """Handle the storage of one file per entry.
    """
    # TODO: add protection against store and retrieve operations on entries
    # that are being processed async.

    def store(self, uid, file_path, transfer_ownership, completion_cb):
        """Store a file for a given entry.
           
        """
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        destination_path = os.path.join(dir_path, 'data')
        if file_path:
            if not os.path.isfile(file_path):
                raise ValueError('No file at %r' % file_path)
            if transfer_ownership:
                try:
                    logging.debug('FileStore moving from %r to %r' % \
                                  (file_path, destination_path))
                    os.rename(file_path, destination_path)
                    completion_cb()
                except OSError, e:
                    if e.errno == errno.EXDEV:
                        self._async_copy(file_path, destination_path,
                                         completion_cb)
                    else:
                        raise
            else:
                self._async_copy(file_path, destination_path, completion_cb)
            """
        TODO: How can we support deleting the file of an entry?
        elif not file_path and os.path.exists(destination_path):
            logging.debug('FileStore: deleting %r' % destination_path)
            os.remove(destination_path)
            completion_cb()
            """
        else:
            logging.debug('FileStore: Nothing to do')
            completion_cb()

    def _async_copy(self, file_path, destination_path, completion_cb):
        """Start copying a file asynchronously.
        
        """
        logging.debug('FileStore copying from %r to %r' % \
                      (file_path, destination_path))
        async_copy = AsyncCopy(file_path, destination_path, completion_cb)
        async_copy.start()

    def retrieve(self, uid, user_id, extension):
        """Place the file associated to a given entry into a directory where the
            user can read it. The caller is reponsible for deleting this file.
        
        """
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        file_path = os.path.join(dir_path, 'data')
        if not os.path.exists(file_path):
            logging.debug('Entry %r doesnt have any file' % uid)
            return ''

        use_instance_dir = os.path.exists('/etc/olpc-security') and \
                           os.getuid() != user_id
        if use_instance_dir:
            if not user_id:
                raise ValueError('Couldnt determine the current user uid.')
            destination_dir = os.path.join(os.environ['HOME'], 'isolation', '1',
                                           'uid_to_instance_dir', str(user_id))
        else:
            profile = os.environ.get('SUGAR_PROFILE', 'default')
            destination_dir = os.path.join(os.path.expanduser('~'), '.sugar',
                    profile, 'data')
            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir)

        if extension is None:
            extension = ''
        elif extension:
            extension = '.' + extension

        destination_path = os.path.join(destination_dir, uid + extension)

        attempt = 1
        while os.path.exists(destination_path):
            if attempt > 10:
                fd_, destination_path = tempfile.mkstemp(prefix=uid,
                                                         suffix=extension,
                                                         dir=destination_dir)
                del fd_
                os.unlink(destination_path)
                break
            else:
                file_name = '%s_%s%s' % (uid, attempt, extension)
                destination_path = os.path.join(destination_dir, file_name)
                attempt += 1

        # Try to hard link from the original file to the targetpath. This can
        # fail if the file is in a different filesystem. Do a symlink instead.
        try:
            os.link(file_path, destination_path)
        except OSError, e:
            if e.errno == errno.EXDEV:
                os.symlink(file_path, destination_path)
            else:
                raise

        # Try to make the original file readable. This can fail if the file is
        # in a FAT filesystem.
        try:
            os.chmod(file_path, 0604)
        except OSError, e:
            if e.errno != errno.EPERM:
                raise

        return destination_path

    def get_file_path(self, uid):
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        return os.path.join(dir_path, 'data')

    def delete(self, uid):
        """Remove the file associated to a given entry.
        
        """
        dir_path = layoutmanager.get_instance().get_entry_path(uid)
        file_path = os.path.join(dir_path, 'data')
        if os.path.exists(file_path):
            os.remove(file_path)

    def hard_link_entry(self, new_uid, existing_uid):
        existing_file = os.path.join(
                layoutmanager.get_instance().get_entry_path(existing_uid),
                'data')
        new_file = os.path.join(
                layoutmanager.get_instance().get_entry_path(new_uid),
                'data')

        logging.debug('removing %r' % new_file)
        os.remove(new_file)

        logging.debug('hard linking %r -> %r' % (new_file, existing_file))
        os.link(existing_file, new_file)

class AsyncCopy(object):
    """Copy a file in chunks in the idle loop.
    
    """
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

