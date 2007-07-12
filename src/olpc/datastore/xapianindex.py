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


class IndexManager(object):

    def __init__(self, language='en'):
        # We will maintain two connections to the database
        # we trigger automatic flushes to the read_index
        # after any write operation        
        self.write_index = None
        self.read_index = None
        self.queue = Queue(0)
        self.indexer_running = False
        self.language = language

        self.fields = set()
        
    #
    # Initialization
    def connect(self, repo):
        if self.write_index is not None:
            warnings.warn('''Requested redundant connect''', RuntimeWarning)
            
        self.write_index = secore.IndexerConnection(repo)
        self.setupFields()
        
        self.read_index = secore.SearchConnection(repo)
        
        # by default we start the indexer now
        self.startIndexer()

    def stop(self):
        self.stopIndexer()
        self.write_index.close()
        self.read_index.close()


    def startIndexer(self):
        self.indexer_running = True
        self.indexer = threading.Thread(target=self.indexThread,
                                        name="XapianIndexer")
        self.indexer.setDaemon(True)
        self.indexer.start()
        
    def stopIndexer(self, force=False):
        if not self.indexer_running: return 
        if not force: self.queue.join()
        self.indexer_running = False
        self.indexer.join()

    def enque(self, uid, vid, doc):
        self.queue.put((uid, vid, doc))

    def indexThread(self):
        # process the queue
        while self.indexer_running:
            # include timeout here to ease shutdown of the thread
            # if this is a non-issue we can simply allow it to block
            try:
                uid, vid, doc = self.queue.get(timeout=0.5)
                self.write_index.add(doc)
                self.flush()
                logger.info("Indexed Content %s:%s" % (uid, vid))
                self.queue.task_done()
            except Empty:
                pass
            
    @property
    def working(self):
        """Does the indexer have work"""
        return not self.queue.empty()
    
    def flush(self):
        """Called after any database mutation"""
        self.write_index.flush()
        self.read_index.reopen()

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
        
    def setupFields(self):
        # add standard fields
        # text is content objects information
        self.addField('text', store=False, exact=False)

        # vid is version id
        self.addField('vid', store=True, exact=True, sortable=True, type="float")

        # Title has additional weight 
        self.addField('title', store=True, exact=False, weight=2, sortable=True)

        self.addField('mimetype', store=True, exact=True)
        self.addField('author', store=True, exact=True)
        self.addField('language', store=True, exact=True)


        self.addField('ctime', store=True, exact=True, sortable=True, type='date')
        self.addField('mtime', store=True, exact=True, sortable=True, type='date')
        
    #
    # Index Functions
    def index(self, props, filename=None):
        """Index the content of an object.
        Props must contain the following:
            key -> Property()
        """
        doc = secore.UnprocessedDocument()
        add = doc.fields.append
        
        if filename:
            mimetype = props.get("mimetype")
            mimetype = mimetype and mimetype.value or 'text/plain'
            fp = converter(filename, mimetype)

        #
        # File contents
        if fp:
            # add the (converted) document contents
            add(secore.Field('text', fp.read()))

        #
        # Version handling
        #
        # we implicitly create new versions of documents the version
        # id should have been set by the higher level system
        uid = props.pop('uid', None)
        vid = props.pop('vid', None)

        if uid: uid = uid.value
        else: uid = create_uid()
        if vid: vid = vid.value
        else: vid = "1.0"
        
        doc.id = uid
        add(secore.Field('vid', vid))
        
        #
        # Property indexing
        for k, prop in props.iteritems():
            if isinstance(prop, model.BinaryProperty): continue
            value = prop.value
            if k not in self.fields:
                warnings.warn("""Missing field configuration for %s""" % k,
                             RuntimeWarning)
                continue
            add(secore.Field(k, value))

        # queue the document for processing
        self.enque(uid, vid, doc)

        return uid

    #
    # Search
    def search(self, query, start_index=0, end_index=50):
        """search the xapian store.
        query is a string defining the serach in standard web search syntax.

        ie: it contains a set of search terms.  Each search term may be
        preceded by a "+" sign to indicate that the term is required, or a "-"
        to indicate that is is required to be absent.
        """
        # this will return the [(id, relevance), ...], estimated
        # result count
        ri = self.read_index
        if isinstance(query, dict):
            queries = []
            # each term becomes part of the query join
            for k, v in query.iteritems():
                queries.append(ri.query_field(k, v))
            q = ri.query_composite(ri.OP_AND, queries)
        else:
            q = self.parse_query(query)

            
        results = ri.search(q, start_index, end_index)
        return [r.id for r in results]
            
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
