""" 
deltastream
~~~~~~~~~~~~~~~~~~~~
A forward or backward stream of delta information used to manage file
versions efficiently

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'


import bsdiff
FULL = 1
PATCH = 2

class DeltaStream(object):
    """Record and Reconstruct objects from a forward diff chain. When diff
    size/distance from the original is over a threshold we record a
    new version in its entirety
    """

    def _record(self, old_fn, new_fn):
        od = open(old_fn, 'r').read()
        nd = open(new_fn, 'r').read()
        
        #XXX: This needs to be done more memory efficiently
        patch = bsdiff.Patch(od, nd)
        # XXX: again, memory inefficient 
        if len(str(patch)) < (len(nd) / 2.0):
            # The patch is larger than some threshold, we want to
            # record a new full version rather than a patch
            return FULL, nd
        else:
            return PATCH, patch

    def record(self, old_fn, new_fn):
        mode, data = self._record(old_fn, new_fn)
        if mode is FULL:
            pass
        elif mode is PATCH:
            pass
        
