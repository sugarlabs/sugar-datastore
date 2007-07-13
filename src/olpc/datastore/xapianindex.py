""" 
xapianindex
~~~~~~~~~~~~~~~~~~~~
maintain indexes on content

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'


from Queue import Queue, Empty
import logging
import re

import threading
import warnings

import secore

from olpc.datastore import model 
from olpc.datastore.converter import converter
from olpc.datastore.utils import create_uid


# Setup Logger
logger = logging.getLogger('org.sugar.datastore.xapianindex')

# Indexer Operations
CREATE = 1
UPDATE = 2
DELETE = 3


class ContentMappingIter(object):
    """An iterator over a set of results from a search.

    """
    def __init__(self, results, backingstore):
        self._results = results
        self._backingstore = backingstore
        self._iter = iter(results)

    def __iter__(self): return self
    
    def next(self):
        searchresult = self._iter.next()
        return model.Content(searchresult, self._backingstore)


class IndexManager(object):
    DEFAULT_DATABASE_NAME = 'index'
    
    def __init__(self, default_language='en'):
        # We will maintain two connections to the database
        # we trigger automatic flushes to the read_index
        # after any write operation        
        self.write_index = None
        self.read_index = None
        self.queue = Queue(0)
        self.indexer_running = False
        self.language = default_language

        self.backingstore = None
        
        self.fields = set()
        
    #
    # Initialization
    def connect(self, repo, **kwargs):
        if self.write_index is not None:
            warnings.warn('''Requested redundant connect to index''',
                          RuntimeWarning)

        self.repo = repo
        self.write_index = secore.IndexerConnection(repo)

        # configure the database according to the model
        datamodel = kwargs.get('model', model.defaultModel)
        datamodel.apply(self)

        # store a reference
        self.datamodel = datamodel
        
        self.read_index = secore.SearchConnection(repo)

        self.flush()        

        # by default we start the indexer now
        self.startIndexer()

    def bind_to(self, backingstore):
        # signal from backingstore that its our parent
        self.backingstore = backingstore

    # flow control
    def flush(self):
        """Called after any database mutation"""
        self.write_index.flush()
        self.read_index.reopen()
    
    def stop(self):
        self.stopIndexer()
        self.write_index.close()
        self.read_index.close()

    # Index thread management
    def startIndexer(self):
        self.indexer_running = True
        self.indexer = threading.Thread(target=self.indexThread)
        self.indexer.setDaemon(True)
        self.indexer.start()
        
    def stopIndexer(self, force=False):
        if not self.indexer_running: return 
        if not force: self.queue.join()
        self.indexer_running = False
        self.indexer.join()

    def enque(self, uid, vid, doc, operation, filestuff=None):
        self.queue.put((uid, vid, doc, operation, filestuff))

    def indexThread(self):
        # process the queue
        # XXX: there is currently no way to remove items from the queue
        # for example if a USB stick is added and quickly removed
        # the mount should however get a stop() call which would
        # request that the indexing finish
        while self.indexer_running:
            # include timeout here to ease shutdown of the thread
            # if this is a non-issue we can simply allow it to block
            try:
                uid, vid, doc, operation, filestuff = self.queue.get(timeout=0.5)
                if operation is DELETE: self.write_index.delete(uid)
                elif operation in (CREATE, UPDATE):
                    # Here we handle the conversion of binary
                    # documents to plain text for indexing. This is
                    # done in the thread to keep things async and
                    # latency lower.
                    if filestuff:
                        filename, mimetype = filestuff
                        fp = converter(filename, mimetype)
                        if fp:
                            doc.fields.append(secore.Field('fulltext',
                                                           fp.read()))
                            
                    if operation is CREATE: self.write_index.add(doc)
                    elif operation is UPDATE: self.write_index.replace(doc)

                else:
                    logger.warning("Unknown indexer operation ( %s: %s)" % \
                                   (uid, operation))
                    continue

                # XXX: this isn't quite true, we haven't called flush
                # yet so the document might not be on disk
                logger.info("Indexed Content %s:%s" % (uid, vid))
                # but we still tell the queue its complete 
                self.queue.task_done()
                
            except Empty:
                pass
##             except:
##                 import traceback
##                 traceback.print_exc()
##                 try: self.write_index.close()
##                 except: pass
##                 try:
##                     self.write_index = secore.IndexerConnection(self.repo)
##                     self.read_index.reopen()
##                 except:
##                     # Shut down the indexer
##                     logger.critical("Indexer Failed, Shutting it down")
##                     self.indexer_running = False
                    
                
                


    def complete_indexing(self):
        """Intentionally block until the indexing is complete. Used
        primarily in testing.
        """
        self.queue.join()
        self.flush()
    
    #
    # Field management
    def addField(self, key, store=True, exact=False, sortable=False,
                 type='string', collapse=False,
                 **kwargs):
        language = kwargs.pop('language', self.language)
        
        xi = self.write_index.add_field_action
        
        if store: xi(key, secore.FieldActions.STORE_CONTENT)
        if exact: xi(key, secore.FieldActions.INDEX_EXACT)
        else:
            # weight -- int 1 or more
            # nopos  -- don't include positional information
            # noprefix -- boolean
            xi(key, secore.FieldActions.INDEX_FREETEXT, language=language, **kwargs)

        if sortable:
            xi(key, secore.FieldActions.SORTABLE, type=type)
        if collapse:
            xi(key, secore.FieldActions.COLLAPSE)

        # track this to find missing field configurations
        self.fields.add(key)

    #
    # Index Functions
    def _mapProperties(self, props):
        """data normalization function, maps dicts of key:kind->value
        to Property objects
        """
        d = {}
        for k,v in props.iteritems():
            p = model.Property.fromstring(k, v)
            d[p.key] = p
        return d

    def index(self, props, filename=None):
        """Index the content of an object.
        Props must contain the following:
            key -> Property()
        """
        props = self._mapProperties(props)
        doc = secore.UnprocessedDocument()
        add = doc.fields.append
        fp = None
        operation = UPDATE

        filestuff = None
        if filename:
            # enque async file processing
            # XXX: to make sure the file is kept around we could keep
            # and open fp?
            mimetype = props.get("mimetype")
            mimetype = mimetype and mimetype.value or 'text/plain'
            filestuff = (filename, mimetype)

        #
        # Version handling
        #
        # we implicitly create new versions of documents the version
        # id should have been set by the higher level system
        uid = props.pop('uid', None)
        vid = props.pop('vid', None)

        if uid: uid = uid.value
        else:
            uid = create_uid()
            operation = CREATE
            
        if vid: vid = str(float(vid.value) + 1.0)
        else: vid = "1.0"
        
        doc.id = uid
        add(secore.Field('vid', vid))
        
        #
        # Property indexing
        for k, prop in props.iteritems():
            value = prop.value

            if k not in self.fields:
                warnings.warn("""Missing field configuration for %s""" % k,
                              RuntimeWarning)
                continue
            
            add(secore.Field(k, value))
            
        # queue the document for processing
        self.enque(uid, vid, doc, operation, filestuff)

        return uid

    def get(self, uid):
        doc = self.read_index.get_document(uid)
        if not doc: raise KeyError(uid)
        return model.Content(doc, self.backingstore)

    def delete(self, uid):
        # does this need queuing?
        # the higher level abstractions have to handle interaction
        # with versioning policy and so on
        self.enque(uid, None, None, DELETE)
        
    #
    # Search
    def search(self, query, start_index=0, end_index=50):
        """search the xapian store.
        query is a string defining the serach in standard web search syntax.

        ie: it contains a set of search terms.  Each search term may be
        preceded by a "+" sign to indicate that the term is required, or a "-"
        to indicate that is is required to be absent.
        """
        ri = self.read_index
        if not query:
            q = self.read_index.query_all()
        elif isinstance(query, dict):
            queries = []
            # each term becomes part of the query join
            for k, v in query.iteritems():
                queries.append(ri.query_field(k, v))
            q = ri.query_composite(ri.OP_AND, queries)
        else:
            q = self.parse_query(query)
            
        results = ri.search(q, start_index, end_index)
        count = results.matches_estimated

        # map the result set to model.Content items
        return ContentMappingIter(results, self.backingstore), count
    

    def get_uniquevaluesfor(self, property):
        # XXX: this is very sketchy code
        # try to get the searchconnection to support this directly
        # this should only apply to EXACT fields
        r = set()
        prefix = self.read_index._field_mappings.get_prefix(property)
        plen = len(prefix)
        termiter = self.read_index._index.allterms(prefix)
        for t in termiter:
            term = t.term
            if len(term) > plen:
                term = term[plen:]
                if term.startswith(':'): term = term[1:]
                r.add(term)

        # r holds the textual representation of the fields value set
        # if the type of field or property needs conversion to a
        # different python type this has to happen now
        descriptor = self.datamodel.fields.get(property)
        if descriptor:
            kind = descriptor[1]
            impl = model.propertyByKind(kind)
            r = set([impl.set(i) for i in r])
        
        return r
                                                         
    def parse_query(self, query):
        # accept standard web query like syntax
        # 'this' -- match this
        # 'this that' -- match this and that in document
        # '"this that"' match the exact pharse 'this that'
        # 'title:foo' match a document whose title contains 'foo'
        # 'title:"A tale of two datastores"' exact title match
        # '-this that' match that w/o this
        ri = self.read_index
        start = 0
        end = len(query)
        nextword = re.compile("(\S+)")
        endquote = re.compile('(")')
        queries = []
        while start < end:
            m = nextword.match(query, start)
            if not m: break
            orig = start
            field = None
            start = m.end() + 1
            word = m.group(1)
            if ':' in word:
                # see if its a field match
                fieldname, w = word.split(':', 1)
                if fieldname in self.fields:
                    field = fieldname
                    
                word = w

            if word.startswith('"'):
                qm = endquote.search(query, start)
                if qm:
                    #XXX: strip quotes or not here
                    #word = query[orig+1:qm.end(1)-1]
                    word = query[orig:qm.end(1)]
                    start = qm.end(1) + 1

            if field:
                queries.append(ri.query_field(field, word))
            else:
                queries.append(ri.query_parse(word))
        q = ri.query_composite(ri.OP_AND, queries)
        return q
