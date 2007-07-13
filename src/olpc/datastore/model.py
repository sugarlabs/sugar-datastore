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

# XXX: Open issues
# list properties - Contributors (a, b, c)
#                   difficult to index now
# content state   - searches don't include content deletion flag
#                 - not recording if content is on other storage yet

propertyTypes = {}
_marker = object()

def registerPropertyType(kind, get, set, xapian_sort_type=None, defaults=None):
    propertyTypes[kind] = PropertyImpl(get, set, xapian_sort_type, defaults)

def propertyByKind(kind): return propertyTypes[kind]

class PropertyImpl(object):
    __slots__ = ('_get', '_set', 'xapian_sort_type', 'defaults')
    
    def __init__(self, get, set, xapian_sort_type=None, defaults=None):
        self._get, self._set = get, set
        self.xapian_sort_type = xapian_sort_type
        self.defaults = defaults
        
    def get(self, value): return self._get(value)
    def set(self, value): return self._set(value)
    
class Property(object):
    """Light-weight property implementation.
    Handles typed properties via a global registry of type->callbacks

    >>> p = Property(key, value, 'string')
    >>> b = Property(key, value, 'binary')
    """
    def __init__(self, key, value, kind=None):
        self.key = key
        self._value = value
        self.kind = kind
        if kind not in propertyTypes:
            warnings.warn("Unknown property type: %s on key %s" % \
                          (kind, key), RuntimeWarning)
        else: self._impl = propertyTypes[kind]

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

class Content(object):
    """A light weight proxy around Xapian Documents from secore.
    This provides additional methods which are used in the
    backingstore to assist in storage
    """
    __slots__ = ('_doc', '_backingstore', '_file')
    
    def __init__(self, xapdoc, backingstore=None):
        self._doc = xapdoc
        self._backingstore = backingstore
        self._file = None

    def __repr__(self):
        return "<%s %s>" %(self.__class__.__name__,
                           self.properties)
    
    def get_property(self, key, default=_marker):
        result = self._doc.data.get(key, default)
        if result is _marker: raise KeyError(key)
        if isinstance(result, list) and len(result) == 1:
            return result[0]
        return result

    @property
    def properties(self):
        d = {}
        for k, v in self.data.iteritems():
            if isinstance(v, list) and len(v) == 1:
                v = v[0]
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
                if ext: return None, ext
        return None, None

    def get_file(self):
        if not hasattr(self, "_file") or self._file.closed is True:
            self.backingstore.get(self.id)
        return self._file
    
    def set_file(self, fileobj):
        self._file = fileobj
    file = property(get_file, set_file)

    @property
    def filename(self): return self.file.name

    @property
    def contents(self): return self.file.read()
    
    @property
    def backingstore(self): return self._backingstore
    
    @property
    def id(self): return self._doc.id

    @property
    def data(self): return self._doc.data
    

## class Buddy(object):
##     """A co-author on content. Information is collected and managed
##     here"""
##     pass



def noop(value): return value

import re
base64hack = re.compile("(\S{212})")
def base64enc(value): return ' '.join(base64hack.split(value.encode('base64')))
def base64dec(value): return value.replace(' ', '').decode('base64')

dateformat = "%Y-%m-%dT%H:%M:%S"
def datedec(value, dateformat=dateformat):
    ti = time.strptime(value, dateformat)
    dt = datetime.datetime(*(ti[:-2]))
    dt = dt.replace(microsecond=0)
    return dt

def dateenc(value, dateformat=dateformat):
    if isinstance(value, basestring):
        # XXX: there  is an issue with microseconds not getting parsed
        ti = time.strptime(value, dateformat)
        value = datetime.datetime(*(ti[:-2]))
    value = value.replace(microsecond=0)
    # XXX: drop time for now, this is a xapian issue
    value = value.date()
    return value.isoformat()

# type, get, set, xapian sort type [string|float|date], defaults
# defaults are the default options to addField in IndexManager
# these can be overridden on model assignment
registerPropertyType('string', noop, noop, 'string', {'store' : True,
                                                      'exact' : True,
                                                      'sortable' : True})

registerPropertyType('text', noop, noop, 'string', {'store' : True,
                                                    'exact' : False,
                                                    'sortable' : False})

registerPropertyType('binary', noop, noop, None, {'store' : True,
                                                  'exact' : False,
                                                  'sortable' : False})

registerPropertyType('int', str, int, 'float', {'store' : True,
                                                'exact' : True,
                                                'sortable' : True})

registerPropertyType('number', str, float, 'float', {'store' : True,
                                                     'exact' : True,
                                                     'sortable' : True})

registerPropertyType('date', dateenc, datedec, 'date', {'store' : True,
                                                        'exact' : True,
                                                        'sortable' : True
                                                        })



defaultModel = Model().addFields(    
    ('fulltext', 'text'),
    # vid is version id
    ('vid', 'number'),
    ('activity', 'string'),
    ('checksum', 'string'),
    ('filename', 'string'),
    # Title has additional weight 
    ('title', 'text', {'weight' : 2 }),
    ('url', 'string'),
    ('mimetype', 'string'),
    ('author', 'string'),
    ('language', 'string'),
    ('ctime', 'date'),
    ('mtime', 'date'),
    # this will just be a space delimited list of tags
    # indexed with the content
    # I give them high weight as they have user given semantic value.
    ('tags', 'text', {'weight' :3 } ),
    )

        
