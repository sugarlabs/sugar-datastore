""" 
xapianindex
~~~~~~~~~~~~~~~~~~~~
maintain indexes on content

""" 
from __future__ import with_statement

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'



from Queue import Queue, Empty
import gc
import logging
import re
import sys
import time
import thread
import threading
import warnings

import secore
import xapian as _xapian # we need to modify the QueryParser

from olpc.datastore import model 
from olpc.datastore.converter import converter
from olpc.datastore.utils import create_uid, parse_timestamp_or_float


# Setup Logger
logger = logging.getLogger('org.sugar.datastore.xapianindex')

# Indexer Operations
CREATE = 1
UPDATE = 2
DELETE = 3

# Force a flush every _n_ changes to the db
THRESHOLD = 128

class ReadWriteConnection(secore.indexerconnection.IndexerConnection,
                          secore.searchconnection.SearchConnection):
    # has search methods on a write connection
    pass

class ContentMappingIter(object):
    """An iterator over a set of results from a search.

    """
    def __init__(self, results, backingstore, model):
        self._results = results
        self._backingstore = backingstore
        self._iter = iter(results)
        self._model = model

    def __iter__(self): return self
    
    def next(self):
        searchresult = self._iter.next()
        return model.Content(searchresult, self._backingstore, self._model)


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
        self._write_lock = threading.Lock()
        self.deltact = 0 # delta count
        
    #
    # Initialization
    def connect(self, repo, **kwargs):
        if self.write_index is not None:
            warnings.warn('''Requested redundant connect to index''',
                          RuntimeWarning)

        self.repo = repo
        self.write_index = ReadWriteConnection(repo)

        # configure the database according to the model
        datamodel = kwargs.get('model', model.defaultModel)
        datamodel.apply(self)

        # store a reference
        self.datamodel = datamodel
        
        self.read_index = self.write_index

        self.flush(force=True)        
        # by default we start the indexer now
        self.startIndexer()
        assert self.indexer.isAlive()

                
    def bind_to(self, backingstore):
        # signal from backingstore that its our parent
        self.backingstore = backingstore

    
    def stop(self, force=False):
        self.stopIndexer(force)
        self.write_index.close()
        #self.read_index.close()
        # XXX: work around for xapian not having close() this will
        # change in the future in the meantime we delete the
        # references to the indexers and then force the gc() to run
        # which should inturn trigger the C++ destructor which forces
        # the database shut.
        self.write_index = None
        self.read_index = None
        gc.collect()
        
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
        # should terminate after the current task
        self.indexer.join()

    # flow control
    def flush(self, force=False):
        """Called after any database mutation"""
        self.deltact += 1
        if force or self.deltact > THRESHOLD:
            with self._write_lock:
                self.write_index.flush()
                #self.read_index.reopen()
                self.deltact = 0

    def enque(self, uid, vid, doc, operation, filestuff=None):
        # here we implement the sync/async policy
        # we want to take create/update operations and
        # set theproperties right away, the
        # conversion/fulltext indexing can
        # happen in the thread
        if operation in (CREATE, UPDATE):
            with self._write_lock:
                if operation is CREATE:
                    self.write_index.add(doc)
                    logger.info("created %s:%s" % (uid, vid))
                elif operation is UPDATE:
                    self.write_index.replace(doc)
                    logger.info("updated %s:%s" % (uid, vid))
            self.flush()

            # Disable content indexing for Trial-3.
            # https://dev.laptop.org/ticket/3058
            return
            
            # now change CREATE to UPDATE as we set the
            # properties already
            operation = UPDATE
            if not filestuff:
                # In this case we are done
                return
        elif operation is DELETE:
            # sync deletes
            with self._write_lock:
                self.write_index.delete(uid)
                logger.info("deleted content %s:%s" % (uid,vid))
            self.flush()
            return

        self.queue.put((uid, vid, doc, operation, filestuff))

    def indexThread(self):
        # process the queue
        # XXX: there is currently no way to remove items from the queue
        # for example if a USB stick is added and quickly removed
        # the mount should however get a stop() call which would
        # request that the indexing finish
        # XXX: we can in many cases index, not from the tempfile but
        # from the item in the repo as that will become our immutable
        # copy. Detect those cases and use the internal filename
        # property or backingstore._translatePath to get at it
        while self.indexer_running:
            # include timeout here to ease shutdown of the thread
            # if this is a non-issue we can simply allow it to block
            try:
                data = self.queue.get(True, 0.025)
                uid, vid, doc, operation, filestuff = data
            except Empty:
                #time.sleep(1.0)
                continue

            try:
                with self._write_lock:
                    if operation is UPDATE:
                        # Here we handle the conversion of binary
                        # documents to plain text for indexing. This is
                        # done in the thread to keep things async and
                        # latency lower.
                        # we know that there is filestuff or it
                        # wouldn't have been queued
                        filename, mimetype = filestuff
                        if isinstance(filename, file):
                            filename = filename.name
                        fp = converter(filename, mimetype)
                        if fp:
                            # read in at a fixed block size, try to
                            # conserve memory. If this doesn't work
                            # we can make doc.fields a generator
                            while True:
                                chunk = fp.read(2048)
                                if not chunk: break
                                doc.fields.append(secore.Field('fulltext', chunk))
                                
                            self.write_index.replace(doc)
                            logger.info("update file content %s:%s" % (uid, vid))
                        else:
                            logger.warning("""Conversion process failed for document %s %s""" % (uid, filename))
                    else:
                        logger.warning("Unknown indexer operation ( %s: %s)" % (uid, operation))

                    # tell the queue its complete 
                    self.queue.task_done()

                # we do flush on each record now
                self.flush()
            except:
                logger.exception("Error in indexer")
                

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
        add_anything = False
        for k,v in props.iteritems():
            p, added = self.datamodel.fromstring(k, v,
                                                 allowAddition=True)
            if added is True:
                self.fields.add(p.key)
                add_anything = True
            d[p.key] = p

        if add_anything:
            with self._write_lock:
                self.datamodel.apply(self)
            
        return d

    def index(self, props, filename=None):
        """Index the content of an object.
        Props must contain the following:
            key -> Property()
        """
        operation = UPDATE
        #
        # Version handling
        #
        # we implicitly create new versions of documents the version
        # id should have been set by the higher level system
        uid = props.pop('uid', None)
        vid = props.pop('vid', None)

        if not uid:
            uid = create_uid()
            operation = CREATE
            
        if vid: vid = str(float(vid) + 1.0)
        else: vid = "1.0"
        
        # Property mapping via model
        props = self._mapProperties(props)
        doc = secore.UnprocessedDocument()
        add = doc.fields.append
        fp = None


        filestuff = None
        if filename:
            # enque async file processing
            # XXX: to make sure the file is kept around we could keep
            # and open fp?
            mimetype = props.get("mime_type")
            mimetype = mimetype and mimetype.value or 'text/plain'
            filestuff = (filename, mimetype)

        doc.id = uid
        add(secore.Field('vid', vid))
        
        #
        # Property indexing
        for k, prop in props.iteritems():
            value = prop.for_xapian
            
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
        return model.Content(doc, self.backingstore, self.datamodel)

    def delete(self, uid):
        # does this need queuing?
        # the higher level abstractions have to handle interaction
        # with versioning policy and so on
        self.enque(uid, None, None, DELETE)
        
    #
    # Search
    def search(self, query, start_index=0, end_index=4096, order_by=None):
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
            q = query.pop('query', None)
            if q:
                queries.append(self.parse_query(q))
            if not query and not queries:
                # we emptied it 
                q = self.read_index.query_all()
            else:
                # each term becomes part of the query join
                for k, v in query.iteritems():
                    if isinstance(v, dict):
                        # it might be a range scan
                        # this needs to be factored out
                        # and/or we need client side lib that helps
                        # issue queries because there are type
                        # conversion issues here
                        start = v.pop('start', 0)
                        end = v.pop('end', sys.maxint)
                        start = parse_timestamp_or_float(start)
                        end = parse_timestamp_or_float(end)
                        queries.append(ri.query_range(k, start, end))
                    elif isinstance(v, list):
                        # construct a set of OR queries
                        ors = []
                        for item in v: ors.append(ri.query_field(k, item))
                        queries.append(ri.query_composite(ri.OP_OR, ors))
                    else:
                        queries.append(ri.query_field(k, v))
                        
                q = ri.query_composite(ri.OP_AND, queries)
        else:
            q = self.parse_query(query)

        if order_by and isinstance(order_by, list):
            # secore only handles a single item, not a multilayer sort
            order_by = order_by[0]

        results = ri.search(q, start_index, end_index, sortby=order_by)
        count = results.matches_estimated

        # map the result set to model.Content items
        return ContentMappingIter(results, self.backingstore, self.datamodel), count
    

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

        # limited support for wildcard searches

        qp = _xapian.QueryParser

        flags = (qp.FLAG_LOVEHATE)

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
                    # this is a phrase modify the flags
                    flags |= qp.FLAG_PHRASE
                    start = qm.end(1) + 1

            if field:
                queries.append(ri.query_field(field, word))
            else:
                if word.endswith("*"):
                    flags |= qp.FLAG_WILDCARD
                q = self._query_parse(word, flags)

                queries.append(q)
        q = ri.query_composite(ri.OP_AND, queries)
        return q

    def _query_parse(self, word, flags=0, op=None):
        # while newer secore do pass flags it doesn't allow control
        # over them at the API level. We override here to support
        # adding wildcard searching
        ri = self.read_index
        if op is None: op = ri.OP_AND
        qp = ri._prepare_queryparser(None, None, op)
        try:
            return qp.parse_query(word, flags)
        except _xapian.QueryParserError, e:
            # If we got a parse error, retry without boolean operators (since
            # these are the usual cause of the parse error).
            return qp.parse_query(string, 0)
