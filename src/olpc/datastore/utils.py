import dbus

class Singleton(type):
    """A singleton metaclass

    >>> class MyManager(object):
    ...    __metaclass__ = Singleton
    >>> a = MyManager()
    >>> b = MyManager()
    >>> assert a is b
    
    """
    def __init__(cls,name,bases,dic):
        super(Singleton,cls).__init__(name,bases,dic)
        cls._instance=None
    def __call__(cls,*args,**kw):
        if cls._instance is None:
            cls._instance=super(Singleton,cls).__call__(*args,**kw)
        return cls._instance
    
class partial:
    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.pending = args
        self.kwargs = kwargs
        
    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs

        return self.fun(*(self.pending + args), **kw)

def once(method):
    "A decorator that runs a method only once."
    attrname = "_called"
    def decorated(self, *args, **kwargs):
        try:
            return getattr(method, attrname)
        except AttributeError:
            r = method(self, *args, **kwargs)
            setattr(method, attrname, r)
            return r
    return decorated
        


def create_uid():
    # this is linux specific but easily changed
    # Python 2.5 has universal support for this built in
    return open('/proc/sys/kernel/random/uuid', 'r').read()[:-1]


def options_for(dict, prefix, invert=False):
    """return a dict of the filtered properties for keys with prefix.
    prefix will be removed

    If invert is True then only those keys not matching prefix are returned.
    
    >>> assert options_for({'app.a.option' : 1, 'app.b.option' : 2}, 'app.b.')['option'] == 2
    """
    d = {}
    l = len(prefix)
    for k, v in dict.iteritems():
        if k.startswith(prefix):
            if invert is False:d[k[l:]] = v
        elif invert is True:
            d[k] = v
            
    return d
    
    

def _convert(arg):
    # this recursively processes arguments sent over dbus and yields
    # normalized versions
    if isinstance(arg, (dbus.String, dbus.UTF8String)):
        try: return arg.encode('utf-8')
        except: return str(arg)

    if isinstance(arg, (dbus.Dictionary, dict)):
        d = {}
        for k, v in arg.iteritems():
            # here we call str on the lhs making it suitable for
            # passing as keywords args
            d[str(_convert(k))] = _convert(v)
        return d

    if isinstance(arg, dbus.Array):
        a = []
        for item in arg:
            a.append(_convert(item))
        return a
    return arg

    
def sanitize_dbus(method):
    # decorator to produce an alternative version of arguments based on pure Python
    # types.
    def decorator(self, *args, **kwargs):
        n = []
        for arg in args: n.append(_convert(arg))
        kw = _convert(kwargs)
        return method(self, *n, **kw)
    return decorator
