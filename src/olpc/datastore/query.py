""" 
olpc.datastore.query
~~~~~~~~~~~~~~~~~~~~
manage the metadata index and make it queryable. this in turn will
depend on olpc.datastore.fulltext which indexes the actual content.

""" 

__author__ = 'Benjamin Saller <bcsaller@objectrealms.net>'
__docformat__ = 'restructuredtext'
__copyright__ = 'Copyright ObjectRealms, LLC, 2007'
__license__  = 'The GNU Public License V2+'


from datetime import datetime
from lemur.xapian.sei import DocumentStore, DocumentPiece, SortableValue
from olpc.datastore.converter import converter
from olpc.datastore.model import BackingStoreContentMapping
from olpc.datastore.model import DateProperty
from olpc.datastore.model import Model, Content, Property
from olpc.datastore.utils import create_uid
from sqlalchemy import create_engine, BoundMetaData
from sqlalchemy import select, intersect, and_ 
import atexit
import logging
import os, sys



class QueryManager(object):
    def __init__(self, metadata_uri,
                 language='en',
                 fulltext_repo='fulltext',
                 sync_index=True,
                 use_fulltext=True):
        """
        The metadata_uri is a sqlalchemy connection string used to
        find the database.
        
        Language is the language code used in the fulltext
        engine. This helps improve stemming and so on. In the future
        additional control will be provided.

        This will check keywords for:
               'sync_index' which determines if we use an internal
                             sync index impl or an out of process one
                             via DBus. If the async process is to be
                             used it must be properly configured and
                             available for DBus to spawn.
               
               'fulltext_repo' the full filepath to which the fulltext
                               index data will be stored
               'use_fulltext' when true indexing will be performed

        """
        self.uri = metadata_uri
        self.language = language
        self.content_ext = None

        self._handle_options(fulltext_repo=fulltext_repo,
                             use_fulltext=use_fulltext,
                             sync_index=sync_index)

    def _handle_option(self, options, key):
        if key in options:
            setattr(self, key, options[key])

    def _handle_options(self, **kwargs):
        self._handle_option(kwargs, 'fulltext_repo')
        self._handle_option(kwargs, 'use_fulltext')
        self._handle_option(kwargs, 'sync_index')
        self.sync_index = self.use_fulltext and self.sync_index
        
    def prepare(self, datastore, backingstore, **kwargs):
        """This is called by the datastore with its backingstore and
        querymanager. Its assumed that querymanager is None and we are
        the first in this release
        """
        self._handle_options(**kwargs)
        # XXX: more than on case
        # while there is a one-to-one mapping of backingstores to
        # query managers there can be more than one of these pairs
        # in the whole datastore.        
        self.datastore = datastore
        self.backingstore = backingstore
        # Create the mapping extension that will be used to create
        # content instances
        if self.backingstore:
            self.content_ext = BackingStoreContentMapping(self.backingstore)
        
        self.connect_db()
        self.prepare_db()
        self.connect_model()

        self.connect_fulltext(self.fulltext_repo, self.language,
                              read_only=not self.sync_index)
        return True

    def stop(self):
        pass
        
    # Primary interface
    def create(self, props, filelike=None, include_defaults=True):
        """Props can either be a dict of k,v pairs or a sequence of
        Property objects.

        The advantage of using property objects is that the data can
        by typed. When k/v pairs are used a default string type will
        be chosen.

        When include_defaults is True a default set of properties are
        created on behalf of the Content if they were not provided.

        These include:
             author : ''
             title  : ''
             mime_type : ''
             language : '',
             ctime : '',
             mtime : '',
        """
        s = self.model.session
        c = Content()
        # its important the id be set before other operations
        c.id = create_uid()
        s.save(c)
        
        self._bindProperties(c, props, creating=True, include_defaults=include_defaults)
        s.flush()

        if self.sync_index and filelike:
            self.fulltext_index(c.id, filelike)
        return c
    
    def update(self, content_or_uid, props=None, filelike=None):
        content = self._resolve(content_or_uid)

        if props is not None:
            self._bindProperties(content, props, creating=False)
            self.model.session.flush()
        if self.sync_index and filelike:
            self.fulltext_index(content.id, filelike)

    def _automaticProperties(self):
        now = datetime.now()
        return {
            'mtime' : DateProperty('mtime', now),
            }
    
    def _defaultProperties(self):
        now = datetime.now()
        default = {
            'ctime' : DateProperty('ctime', now),
            'author' : Property('author', '', 'string'),
            'title'  : Property('title', '', 'string'),
            'mime_type' : Property('mime_type', '', 'string'),
            'language' : Property('language', '', 'string'),
        }
        default.update(self._automaticProperties())
        return default

    def _normalizeProps(self, props, creating, include_defaults):
        # return a dict of {name : property}
        if isinstance(props, dict):
            # convert it into a list of properties
            d = {}
            for k,v in props.iteritems():
                p = Property(k, v, 'string')
                d[k] = p
            if creating and include_defaults:
                defaults = self._defaultProperties()
                for k, v in defaults.iteritems():
                    if k not in d: d[k] = v
            props = d
        else:
            d = {}
            for p in props:
                d[p.key] = p
            props = d
        return props
    
    def _bindProperties(self, content, props, creating=False, include_defaults=False):
        """Handle either a dict of properties or a list of property
        objects, binding them to the content instance.
        """
        # for information on include_defaults see create()
        # default properties are only provided when creating is True
        session = self.model.session

        props = self._normalizeProps(props, creating,
                                     include_defaults)
        
        # we should have a dict of property objects
        if creating:
            content.properties.extend(props.values())
        else:
            # if the automatically maintained properties (like mtime)
            # are not set, include them now
            auto = self._automaticProperties()
            auto.update(props)
            props = auto
            # we have to check for the update case
            oldProps = dict([(p.key, p) for p in content.properties])

            for k, p in props.iteritems():
                if k in oldProps:
                    oldProps[k].value = p.value
                    oldProps[k].type  = p.type
                else:    
                    content.properties.append(p)
                    
    def get(self, uid):
        return self.model.session.query(self.model.mappers['content']).get(uid)

    def get_properties(self, content_or_uid, keys):
        c = self._resolve(content_or_uid)
        return self.model.session.query(Property).select_by(self.model.property.c.key.in_(keys),
                                                            content_id=c.id)


    def delete(self, content_or_uid):
        c = self._resolve(content_or_uid)
        s = self.model.session
        s.delete(c)
        s.flush()
        if self.sync_index:
            self.fulltext_unindex(c.id)

        
    def find(self, query=None, **kwargs):
        """
        dates can be search in one of two ways.
        date='YYYY-MM-DD HH:MM:SS'
        date={'start' : 'YYYY-MM-DD HH:MM:SS',
              'end'   : 'YYYY-MM-DD HH:MM:SS'
              }
              where date is either ctime or mtime.
        if start or end is omitted its becomes a simple before/after
        style query. If both are provided its a between query.

        providing the key 'fulltext' will include a full text search
        of content matching its parameters. see fulltext_search for
        additional details.

        To order results by a given property you can specify:
        >>> qm.find(order_by=['author', 'title'])

        Order by must be a list of property names given in the order
        of decreasing precedence.

        If 'limit' is passed it will be the maximum number of results
        to return and 'offset' will be the offset from 0 into the
        result set to return.
        
        """

        # XXX: this will have to be expanded, but in its simplest form
        if not self.sync_index: self.index.reopen()
        
        s = self.model.session
        properties = self.model.tables['properties']
        if not query: query = {}
        query.update(kwargs)
        q = s.query(Content)
        # rewrite the query to reference properties
        # XXX: if there is a 'content' key will will have to search
        # the content using the full text index which will result in a
        # list of id's which must be mapped into the query
        # fulltext_threshold is the minimum acceptable relevance score
        order_by = query.pop('order_by', [])
        limit = query.pop('limit', None)
        offset = query.pop('offset', None)

        # ordering is difficult when we are dealing with sets from
        # more than one source. The model is this.
        # order by the primary (first) sort criteria, then do the rest
        # in post processing. This allows use to assemble partially
        # database sorted results from many sources and quickly
        # combine them.
        if order_by:
            # resolve key names to columns
            if isinstance(order_by, basestring):
                order_by = [o.strip() for o in order_by.split(',')]
                
            if not isinstance(order_by, list):
                logging.debug("bad query, order_by should be a list of property names")                
                order_by = None
                
        if offset: q = q.offset(offset)
        if limit: q = q.limit(limit)
        
        if query:
            properties = self.model.properties
            where = []
            fulltext = query.pop('fulltext', None)
            threshold = query.pop('fulltext_threshold', 60)

            
            
            statement = None
            ft_select = None
            
            if query:
                # daterange support
                # XXX: this is sort of a hack because
                #      - it relies on Manifest typing in sqlite
                #      - value's type is not normalized
                #      - we make special exception based on property name
                # if we need special db handling of dates ctime/mtime
                # will become columns of Content and not properties
                ctime = query.pop('ctime', None)
                mtime = query.pop('mtime', None)
                if ctime or mtime:
                    self._query_dates(ctime, mtime, where)
                for k,v in query.iteritems():
                    where.append(select([properties.c.content_id],
                                        and_( properties.c.key==k,
                                              properties.c.value==v)))
                                 
                statement = intersect(*where)
                statement.distinct=True
                
            if fulltext and self.use_fulltext:
                # perform the full text search and map the id's into
                # the statement for inclusion
                ft_res = self.fulltext_search(fulltext)
                if ft_res:
                    ft_ids = [ft[0] for ft in ft_res if ft[1] >=
                              threshold]
                    
                    if ft_ids:
                        ft_select = select([properties.c.content_id],
                                           properties.c.content_id.in_(*ft_ids))

                if ft_select is None:
                    # the full text query eliminated the possibility
                    # of results by returning nothing under a logical
                    # AND condition, bail now
                    return ([], 0)
                else:
                    if statement is None:
                        statement = ft_select
                        statement.distinct = True
                    else:
                        statement = intersect(statement, ft_select)

            result = statement.execute()
            r = [q.get(i[0]) for i in result]
            r = (r, len(r))
        else:
            r = (q.select(), q.count())

        if order_by:
            # this is a little tricky, these are the partially ordered
            # results. we now generate a sort function based on the
            # complete set of ordering criteria which includes the
            # primary sort criteria as well to keep it stable.
            def comparator(a, b):
                # we only sort on properties so
                for criteria in order_by:
                    mode = 1 # ascending
                    if criteria.startswith('-'):
                        mode = -1
                        criteria = criteria[1:]
                    pa = a.get_property(criteria, None)
                    pb = b.get_property(criteria, None)
                    r = cmp(pa, pb) * mode
                    if r != 0: return r
                return 0
            
            d,c = r

            results = []
            for i in d: results.append(i)
            results.sort(comparator)
            
            r = results ,c 
        return r
    
    # sqla util
    def _resolve(self, content_or_uid):
        if isinstance(content_or_uid, basestring):
            # we need to resolve the object
            content_or_uid = self.model.session.query(Content).get(content_or_uid)
        return content_or_uid

    def _query_dates(self, ctime, mtime, selects):
        if ctime: selects.append(self._query_date('ctime', ctime))
        if mtime: selects.append(self._query_date('mtime', mtime))

    def _query_date(self, key, date):
        properties = self.model.properties
                    
        if isinstance(date, basestring):
            s = select([properties.c.content_id],
                       and_( properties.c.key==key,
                             properties.c.value==date))
        else:
            # its a dict with start/end
            start = date.get('start')
            end = date.get('end')
            if start and end:
                s = select([properties.c.content_id],
                           and_( properties.c.key==key,
                                 properties.c.value.between(start,
                                                            end)))
            elif start:
                s = select([properties.c.content_id],
                           and_( properties.c.key==key,
                                 properties.c.value >=start))
            else:
                s = select([properties.c.content_id],
                           and_( properties.c.key==key,
                                 properties.c.value < end))

        return s

        

        
        
    # fulltext interface
    def fulltext_index(self, uid, fileobj):
        """Index the fileobj relative to uid which should be a
        olpc.datastore.model.Content object's uid. The fileobj can be
        either a pathname or an object implementing the Python file
        ('read') interface.
        """
        pass

    def fulltext_unindex(self, content_id):
        pass

    def fulltext_search(self, *args, **kwargs):
        return []
    
    # lifecycle
    def connect_db(self):
        """Connect to the underlying database. Called implicitly by
        __init__"""
        pass
    
    
    def prepare_db(self):
        """After connecting to the metadata database take any
        initialization steps needed for the environment.

        This is called implicitly by __init__ before the model is
        brought online.
        """
        pass

    def connect_model(self, model):
        """Connect the model. Called with the model passed into
        __init__ after the database has been prepared.
        """
        pass

    def connect_fulltext(self, repo, language, read_only):
        """Connect the full text index"""
        pass
        
    
class SQLiteQueryManager(QueryManager):
    """The default implementation of the query manager. This owns the
    model object and the fulltext object
    """
    def connect_db(self):
        self.db = create_engine(self.uri)
        self.metadata = BoundMetaData(self.db)
        
    def prepare_db(self):
        # Using the sqlite backend we can tune the performance to
        # limit writes as much as possible
        if self.db.name.startswith('sqlite'):
            connection = self.db.connect()
            # cut down of per-activity file locking writes
            connection.execute("PRAGMA locking_mode=EXCLUSIVE")
            # don't demand fsync -- if this is too dangerous
            # we can change it to normal which is still less writey
            # than the default FULL
            connection.execute("PRAGMA synchronous=OFF")
            # temporary tables and indices are kept in memory
            connection.execute("PRAGMA temp_store=MEMORY")
            # XXX: what is the ideal jffs2 page size
            # connection.execute("PRAGMA page_size 4096")

    
    def connect_model(self, model=None):
        if model is None: model = Model()
        # take the model and connect it to us
        model.prepare(self)

        # make sure all the tables and indexes exist
        self.metadata.create_all()
        
        self.model = model


# Full text support
def flatten_unicode(value): return value.encode('utf-8')

class XapianBinaryValue(SortableValue):
    def __init__(self, value, field_name="content"):
        SortableValue.__init__(self, value, field_name)

class XapianFulltext(object):
    def connect_fulltext(self, repo, language='en', read_only=True):
        if not os.path.exists(repo) and read_only is True:
            # create the store 
            index = DocumentStore(repo, language, read_only=False)
            index.close()
            # and abandon it
        self.index = DocumentStore(repo, language, read_only=read_only)
        self.index.registerFlattener(unicode, flatten_unicode)
        atexit.register(self.index.close)
        
    def fulltext_index(self, uid, fileobj):
        """Index the fileobj relative to uid which should be a
        olpc.datastore.model.Content's uid. The fileobj can be either
        a pathname or an object implementing the Python file ('read')
        interface.
        """
        piece = DocumentPiece
        if isinstance(fileobj, basestring):
            # treat it as a pathname
            # use the global converter to try to get text from the file
            fp = converter(fileobj)
            #piece = XapianBinaryValue
        elif hasattr(fileobj, 'read'):
            # this is an off case, we have to assume utf-8 data
            logging.debug("Indexing from readable, not filename")
            fp = fileobj
        else:
            raise ValueError("Not a valid file object")

        if fp is None:
            # for whatever reason we were unable to get the content
            # into an indexable form.
            logging.debug("Unable to index %s %s" % (uid, fileobj))
            return False
        
        return self._ft_index(uid, fp, piece)

    def _ft_index(self, content_id, fp, piece=DocumentPiece):
        try:
            doc = [piece(fp.read())]
            self.index.addDocument(doc, content_id)
            return True
        except:
            logging.debug("fulltext index exception", exc_info=sys.exc_info())
            return False



    def fulltext_search(self, *args, **kwargs):
        """
        perform search(search_string, ) -> [(content_id, relevance), ...]
        
        search_string is a string defining the serach in standard web search
        syntax.

        ie: it contains a set of search terms.  Each search term may be
        preceded by a "+" sign to indicate that the term is required, or a "-"
        to indicate that is is required to be absent.

        If field_name is not None, it is the prefix of a field, which the
        search will be restricted to.
        
        If field_name is None, the search will search all fields by default,
        but search terms may be preceded by a fieldname followed by a colon to
        restrict part of the search to a given field.

        combiner is one of DocumentStore.OP_OR or DocumentStore.OP_AND, and is
        used to indicate the default operator used to combine terms.

        partial is a flag, which should be set to True to enable partial search
        matching, for use when doing interactive searches and we're not sure if
        the user has finished typing the search yet.

        range_restrictions is a RangeRestrictions object, used to restrict the
        search results.

        """
        if len(args) == 1:
            # workaround for api change
            args = (args[0], 0, 10)
            
        res = self.index.performSearch(*args, **kwargs)
        est = max(1, res.estimatedResultCount())
        return res.getResults(0, est)

    def fulltext_similar(self, *content_ids):
        return self.index.findSimilar(content_ids)

    def fulltext_unindex(self, content_id):
        self.index.deleteDocument(content_id)

    def stop(self):
        if self.use_fulltext:
            self.index.close()


class DefaultQueryManager(XapianFulltext, SQLiteQueryManager):
    pass
