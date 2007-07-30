""" 
olpc.datastore.model
~~~~~~~~~~~~~~~~~~~~
The datamodel for the metadata

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'

import datetime
import mimetypes
import os
import time
import warnings
from olpc.datastore.utils import timeparse


# XXX: Open issues
# list properties - Contributors (a, b, c)
#                   difficult to index now
# content state   - searches don't include content deletion flag
#                 - not recording if content is on other storage yet

propertyTypes = {}
_marker = object()

def registerPropertyType(kind, get, set, xapian_sort_type=None,
    defaults=None, for_xapian=None, from_xapain=None):
    propertyTypes[kind] = PropertyImpl(get, set, xapian_sort_type,
                                       defaults, for_xapian=for_xapian, from_xapain=from_xapain)

def propertyByKind(kind): return propertyTypes[kind]

class PropertyImpl(object):
    __slots__ = ('_get', '_set', 'xapian_sort_type', 'defaults', '_for_xapian', '_from_xapian')
    
    def __init__(self, get, set, xapian_sort_type=None, defaults=None,
                 for_xapian=None, from_xapain=None):
        self._get, self._set = get, set
        self.xapian_sort_type = xapian_sort_type
        self.defaults = defaults
        if not for_xapian: for_xapian = self._get
        self._for_xapian = for_xapian
        if not from_xapain: from_xapain = self._set
        self._from_xapian = from_xapain
        
    def get(self, value): return self._get(value)
    def set(self, value): return self._set(value)
    def for_xapian(self, value): return self._for_xapian(value)
    def from_xapian(self, value): return self._from_xapian(value)
    
class Property(object):
    """Light-weight property implementation.
    Handles typed properties via a global registry of type->callbacks

    >>> p = Property(key, value, 'string')
    >>> b = Property(key, value, 'binary')
    """
    def __init__(self, key, value, kind=None):

        self.kind = kind
        if kind not in propertyTypes:
            warnings.warn("Unknown property type: %s on key %s" % \
                          (kind, key), RuntimeWarning)
        else: self._impl = propertyTypes[kind]

        self.key = key
        self.value = value
        
    @classmethod
    def fromstring(cls, key, value=''):
        kind = 'string'
        if ':' in key:
            key, kind = key.split(':', 1)
            # now resolve the kind to a property class
        return cls(key, value, kind)
    

    def __repr__(self):
        return "<%s(%s) %s:%r>" % (self.__class__.__name__,
                                   self.kind,
                                   self.key, self.value)

    def get_value(self): return self._impl.get(self._value)
    def set_value(self, value): self._value = self._impl.set(value)
    value = property(get_value, set_value)

    @property
    def for_xapian(self): return self._impl.for_xapian(self._value)


    def __str__(self): return str(self.value)

class Model(object):
    """Object containing the field/property model used by the
    system"""
    
    def __init__(self):
        self.fields = {}
        self.fieldnames = []

    def copy(self):
        m = Model()
        m.fields = self.fields.copy()
        m.fieldnames = self.fieldnames[:]
        return m

    def fromstring(self, key, value, allowAddition=False):
        """create a property from the key name by looking it up in the
        model."""
        kind = None
        if ':' in key: key, kind = key.split(':', 1)
        added = False
        field = self.fields.get(key)
        if field: mkind = field[1]
        elif allowAddition:
            # create a new field, this will force a change in the
            # model
            # and in turn should add a new field action
            if not kind: kind = "string"
            self.addField(key,kind)
            mkind = kind
            added = True
        else:
            raise KeyError("no field specification for %s" % key)
            
        if kind and mkind:
            if kind != mkind: raise ValueError("""Specified wire
            encoding for property %s was %s, expected %s""" %(key, kind, mkind)) 
        kind = mkind
            
        return Property(key, value, kind), added

    
    def addField(self, key, kind, overrides=None):
        """ Add a field to the model.
        key     -- field name
        kind    -- type by name (registered with registerPropertyType)
        kwargs  -- overrides and additional values to the default
                   arguments supplied by kind
         """
        if key in self.fields:
            raise KeyError("""Another source tried to add %s field to the model""" % key)

        impl = propertyByKind(kind)
        options = impl.defaults.copy()
        if overrides: options.update(overrides)
        if impl.xapian_sort_type:
            if 'type' not in options:
                options['type'] = impl.xapian_sort_type
        
        self.fields[key] = (key, kind, options)
        self.fieldnames.append(key)
        return self
    
    def addFields(self, *args):
        """ List of arguments to addField """
        for arg in args: self.addField(*arg)
        return self        

    def apply(self, indexmanager):
        addField = indexmanager.addField
        for fn in self.fieldnames:
            args = self.fields[fn]
            addField(args[0], **args[2])


# Properties we don't automatically include in properties dict
EXCLUDED_PROPERTIES = ['fulltext', ]

class Content(object):
    """A light weight proxy around Xapian Documents from secore.
    This provides additional methods which are used in the
    backingstore to assist in storage
    """
    __slots__ = ('_doc', '_backingstore', '_file', '_model')
    
    def __init__(self, xapdoc, backingstore=None, model=None):
        self._doc = xapdoc
        self._backingstore = backingstore
        self._file = None
        self._model = model

    def __repr__(self):
        return "<%s %s>" %(self.__class__.__name__,
                           self.properties)
    
    def get_property(self, key, default=_marker):
        result = self._doc.data.get(key, default)
        if result is _marker: raise KeyError(key)
        if isinstance(result, list) and len(result) == 1:
            result = result[0]
        field = self._model.fields.get(key)
        kind = propertyByKind(field[1])
        # Errors here usually property request for a missing field
        return kind.from_xapian(result)
    
        
    @property
    def properties(self):
        d = {}
        for k, v in self.data.iteritems():
            if k in EXCLUDED_PROPERTIES: continue
            
            if isinstance(v, list) and len(v) == 1:
                v = v[0]
            field = self._model.fields.get(k)
            if field:
                kind = propertyByKind(field[1])
                v = kind.from_xapian(v)
            else:
                # do some generic property handling
                if v: v = str(v)
                else: v = ''
            d[k] = v
        return d
    

    def suggestName(self):
        # we look for certain known property names
        # - filename
        # - ext
        # and create a base file name that will be used for the
        # checkout name
        filename = self.get_property('filename', None)
        ext = self.get_property('ext', '')

        if filename:
            # some backingstores keep the full relative path
            filename = os.path.split(filename)[1]
            f, e = os.path.splitext(filename)
            if e: return filename, None
            if ext: return "%s.%s" % (filename, ext), None
        elif ext: return None, ext
        else:
            # try to get an extension from the mimetype if available
            mt = self.get_property('mime_type', None)
            if mt:
                ext = mimetypes.guess_extension(mt)
                # .ksh is a strange ext for plain text
                if ext and ext == '.ksh': ext = '.txt'
                if ext: return None, ext
        return None, None

    def get_file(self):
        if not hasattr(self, "_file") or not self._file or \
               self._file.closed is True:
            target, ext = self.suggestName()
            try:
                targetfile = self.backingstore._targetFile(self.id, target, ext)
                self._file = targetfile
            except OSError:
                self._file = None
        return self._file
    
    def set_file(self, fileobj):
        self._file = fileobj
    file = property(get_file, set_file)

    @property
    def filename(self): return os.path.abspath(self.file.name)

    @property
    def contents(self): return self.file.read()
    
    @property
    def backingstore(self): return self._backingstore
    
    @property
    def id(self): return self._doc.id

    @property
    def data(self): return self._doc.data
    

def noop(value): return value

import re
base64hack = re.compile("(\S{212})")
def base64enc(value): return ' '.join(base64hack.split(value.encode('base64')))
def base64dec(value): return value.replace(' ', '').decode('base64')

DATEFORMAT = "%Y-%m-%dT%H:%M:%S"
def date2string(value): return value.replace(microsecond=0).isoformat()
def string2date(value): return timeparse(value, DATEFORMAT)

def encode_datetime(value):
    # encode datetime to timestamp (float)
    # parse the typelib form to a datetime first
    if isinstance(value, basestring): value = string2date(value)
    return str(time.mktime(value.timetuple()))
    
def decode_datetime(value):
    # convert a float to a local datetime
    return datetime.datetime.fromtimestamp(float(value)).isoformat()

def datedec(value, dateformat=DATEFORMAT):
    return timeparse(value, DATEFORMAT)

def dateenc(value, dateformat=DATEFORMAT):
    if isinstance(value, basestring):
        # XXX: there  is an issue with microseconds not getting parsed
        value = timeparse(value, DATEFORMAT)
    value = value.replace(microsecond=0)
    return value.isoformat()

        

# type, get, set, xapian sort type [string|float|date], defaults
# defaults are the default options to addField in IndexManager
# these can be overridden on model assignment
registerPropertyType('string', noop, noop, 'string', {'store' : True,
                                                      'exact' : True,
                                                      'sortable' : True})

registerPropertyType('text', noop, noop, 'string', {'store' : True,
                                                    'exact' : False,
                                                    'sortable' : False,
                                                    'collapse' : True,
                                                    })

registerPropertyType('binary', noop, noop, None, {'store' : True,
                                                  'exact' : False,
                                                  'sortable' : False})

registerPropertyType('int', str, int, 'float', {'store' : True,
                                                'exact' : True,
                                                'sortable' : True},
                     for_xapian=str)

registerPropertyType('number', str, float, 'float', {'store' : True,
                                                     'exact' : True,
                                                     'sortable' : True})

registerPropertyType('date', dateenc, datedec, 'float', {'store' : True,
                                                         'exact' : True,
                                                         'sortable' : True
                                                         },
                     for_xapian=encode_datetime,
                     from_xapain=decode_datetime)



defaultModel = Model().addFields(    
    ('fulltext', 'text'),
    # vid is version id
    ('vid', 'number'),
    ('checksum', 'string'),
    ('filename', 'string'),
    ('ext', 'string'), # its possible we don't store a filename, but
                       # only an extension we are interested in
    # Title has additional weight 
    ('title', 'text', {'weight' : 2 }),
    ('url', 'string'),
    ('mime_type', 'string'),
    ('author', 'string'),
    ('language', 'string'),
    ('ctime', 'date'),
    ('mtime', 'date'),
    # this will just be a space delimited list of tags
    # indexed with the content
    # I give them high weight as they have user given semantic value.
    ('tags', 'text', {'weight' :3 } ),

    # olpc specific
    ('activity', 'string'),
    ('activity_id', 'string'),
    ('title_set_by_user', 'text'),
    ('keep', 'int'),
    ('icon-color', 'string'),
    ('preview', 'binary'),
    ('buddies', 'text'),
    )

        
