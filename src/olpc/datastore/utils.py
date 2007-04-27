import inspect

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
    



