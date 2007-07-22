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
import codecs
import logging
import os
import subprocess
import sys
import tempfile
import gnomevfs

def guess_mimetype(filename):
    fn = os.path.abspath(filename)
    mimetype = gnomevfs.get_mime_type(fn)
    return mimetype
    
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
            # the stderr capture here will hide glib error messages
            # from converters which shouldn't be generating output anyway
            retcode = subprocess.call(cmd, stderr=subprocess.PIPE)
            if retcode: return None
            return codecs.open(target, 'r', 'utf-8')
        except UnicodeDecodeError:
            # The data was an unknown type but couldn't be understood
            # as text so we don't attempt to index it. This most
            # likely means its just an unknown binary format.
            return None
        finally:
            # we unlink the file as its already been opened for
            # reading
            if os.path.exists(target):
                os.unlink(target)
    
class noop(object):
    def verify(self): return True
    def __call__(self, filename):
        return codecs.open(filename, 'r', 'utf-8')
        
class Converter(object):
    __metaclass__ = Singleton
    def __init__(self):
        # maps both extension -> plugin
        # and mimetype -> plugin
        self._converters = {}
        self._default = None
        self.logger = logging.getLogger('org.laptop.sugar.Indexer')
    
    def registerConverter(self, ext_or_mime, plugin):
        if plugin.verify():
            self._converters[ext_or_mime] = plugin
            if self._default is None: self._default = plugin

    def __call__(self, filename, encoding=None, mimetype=None):
        """Convert filename's content to utf-8 encoded text."""        
        #encoding is passed its the known encoding of the
        #contents. When None is passed the encoding is guessed which
        #can result in unexpected or no output.
        if mimetype: mt = mimetype
        else: mt = guess_mimetype(filename)
        maintype, subtype = mt.split('/',1)

        converter = self._converters.get(mt)
        if not converter:
            converter = self._default
            # it was an image or an unknown application
            if maintype in ['image', 'application', 'audio', 'video'] or \
                   subtype in ['x-trash', 'x-python-bytecode',]:
                converter = None

        if converter:
            try: return converter(filename)
            except:
                logging.debug("Binary to Text failed: %s %s" %
                              (mt, filename), exc_info=sys.exc_info())
            
        return None

# our global instance 
converter = Converter()

# TXT
txt = noop()
converter.registerConverter('.txt', txt)
converter.registerConverter('.html', txt)
converter.registerConverter('text/plain', txt)
converter.registerConverter('text/html', txt)

# PDF
pdf2txt = subprocessconverter('/usr/bin/pdftotext -nopgbrk -enc UTF-8 %(source)s %(target)s')
converter.registerConverter('.pdf', pdf2txt)
converter.registerConverter('application/pdf', pdf2txt)


# DOC
def find_by_ext(filename, ext="txt"):
    return "%s.%s" % (os.path.splitext(filename)[0], ext)

doctotext = subprocessconverter('/usr/bin/abiword -t txt %(source)s', find_by_ext)
converter.registerConverter('.doc', doctotext)
converter.registerConverter('application/msword', doctotext)

# ODT
odt2txt = subprocessconverter('/usr/bin/odt2txt --encoding=UTF-8 --output=%(target)s %(source)s')
converter.registerConverter('.odt', odt2txt)
converter.registerConverter('application/vnd.oasis.opendocument.text', odt2txt)

