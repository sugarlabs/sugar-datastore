""" 
olpc.datastore.converter
~~~~~~~~~~~~~~~~~~~~
Convert binary formats to unicode text for indexing.

Normally we'd make heavy reliance on 3rd party tools to do
conversion. In the olpc use-case we want to minimize such
dependencies. As such we make a  minimal attempt to extract what text
we can.

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'

from olpc.datastore.utils import Singleton
import logging
import mimetypes
import os
import re
import subprocess
import sys
import tempfile

class Purify(object):
    """Remove some non-printable characters from the output of a
    conversion. This also re-encodes unicode text to utf-8
    """

    BAD_CHARS = re.compile('[\xa0|\x0c|\xc2]+')
    
    def __init__(self, fp):
        self.fp = fp

    def __iter__(self):
        self._fp = iter(self.fp)
        return self
    
    def next(self): return self.filter(self._fp.next())

    def filter(self, line):
        line = self.BAD_CHARS.sub(' ', line)
        if isinstance(line, unicode):
            return line.encode('utf-8')
        # the line should be utf-8 encoded already
        return line

    def read(self):
        data = self.fp.read()
        return self.filter(data)

    def seek(self, *args):
        self.fp.seek(*args)

class subprocessconverter(object):
    """Process a command. Collect the output

    commands will have the following variables available to them for
    substitution. 'source' is required and is the input file.
    'target' is optional, but if its omitted the subprocessconverter
    must supply an implict_target(source) method which returns the
    name of the expected output.

    A file object opened for reading will be returned to be passed to
    the indexer.

    %(source)s
    %(target)s

    pdftotext %(source)s %s(target)s
    """
    def __init__(self, cmd, find_target=None):
        self.raw = cmd
        self.require_target = False
        self.find_target = find_target
        
        if '%(source)s' not in cmd:
            raise ValueError("doesn't handle source")
        if '%(target)s' not in cmd:
            if not callable(find_target):
                raise ValueError("no way of locating conversion target")
            self.require_target = True

    def verify(self):
        """should this converter be used?"""
        return os.path.exists(self.raw.split()[0])
    
    def __call__(self, filename):
        data = {}
        data['source'] = filename
        if self.require_target:
            # XXX: methods that return something bad here
            # will result in the wrong thing being unlinked
            target = data['target'] = self.find_target(filename)
        else:
            target = data['target'] = tempfile.mkstemp()[1]
        cmd = self.raw % data

        try:
            cmd = cmd.split()
            retcode = subprocess.call(cmd)
            if retcode: return None
            #return codecs.open(target, 'r', 'utf-8')
            return Purify(open(target, 'r'))
        finally:
            # we unlink the file as its already been opened for
            # reading
            if os.path.exists(target):
                os.unlink(target)

        
class Converter(object):
    __metaclass__ = Singleton
    def __init__(self):
        # maps both extension -> plugin
        # and mimetype -> plugin
        self._converters = {}
        self.logger = logging.getLogger('org.laptop.sugar.DataStore.converter')
    
    def registerConverter(self, ext_or_mime, plugin):
        if plugin.verify():
            self._converters[ext_or_mime] = plugin

    def __call__(self, filename, encoding=None):
        """Convert filename's content to utf-8 encoded text."""        
        #encoding is passed its the known encoding of the
        #contents. When None is passed the encoding is guessed which
        #can result in unexpected or no output.

        ext = os.path.splitext(filename)[1]
        mt = mimetypes.guess_type(filename, False)
        
        converter = self._converters.get(mt)
        if not converter:
            converter = self._converters.get(ext)
        if converter:
            try: return converter(filename)
            except:
                logging.debug("Binary to Text failed: %s %s %s" %
                              (ext, mt, filename), exc_info=sys.exc_info())
            
        return open(filename, 'r')

# our global instance 
converter = Converter()


# PDF
pdf2txt = subprocessconverter('/usr/bin/pdftotext -nopgbrk -enc UTF-8 %(source)s %(target)s')
converter.registerConverter('.pdf', pdf2txt)


# DOC
def find_by_ext(filename, ext="txt"):
    return "%s.%s" % (os.path.splitext(filename)[0], ext)

doctotext = subprocessconverter('/usr/bin/abiword -t txt %(source)s', find_by_ext)
converter.registerConverter('.doc', doctotext)

# ODT
odt2txt = subprocessconverter('/usr/local/bin/odt2txt --encoding=UTF-8 --output=%(target)s %(source)s')
converter.registerConverter('.odt', odt2txt)

