
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
