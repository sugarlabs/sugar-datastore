import os
import logging
import time
import sys

import xapian
from xapian import WritableDatabase, Document, Enquire, Query, QueryParser

_MAX_LIMIT = 4096

_VALUE_UID = 0
_VALUE_TIMESTAMP = 1
_VALUE_ACTIVITY_ID = 2
_VALUE_MIME_TYPE = 3
_VALUE_ACTIVITY = 4
_VALUE_KEEP = 5

_PREFIX_UID = 'Q'
_PREFIX_ACTIVITY = 'A'

_PROPERTIES_NOT_TO_INDEX = ['timestamp', 'activity_id', 'keep', 'preview']

class IndexStore(object):
    def __init__(self, root_path):
        index_path = os.path.join(root_path, 'index')
        self._database = WritableDatabase(index_path, xapian.DB_CREATE_OR_OPEN)

    def _document_exists(self, uid):
        postings = self._database.postlist(_PREFIX_UID + uid)
        try:
            postlist_item = postings.next()
        except StopIteration:
            return False
        return True

    def store(self, uid, properties):
        document = Document()
        document.add_term(_PREFIX_UID + uid)
        document.add_term(_PREFIX_ACTIVITY + properties['activity'])

        document.add_value(_VALUE_UID, uid)
        document.add_value(_VALUE_TIMESTAMP, str(properties['timestamp']))
        document.add_value(_VALUE_ACTIVITY_ID, properties['activity_id'])
        document.add_value(_VALUE_MIME_TYPE, str(properties['keep']))
        document.add_value(_VALUE_ACTIVITY, properties['activity'])

        term_generator = xapian.TermGenerator()

        # TODO: we should do stemming, but in which language?
        #if language is not None:
        #    term_generator.set_stemmer(_xapian.Stem(language))

        # TODO: we should use a stopper
        #if stop is not None:
        #    stopper = _xapian.SimpleStopper()
        #    for term in stop:
        #        stopper.add (term)
        #    term_generator.set_stopper (stopper)

        term_generator.set_document(document)
        term_generator.index_text_without_positions(
                self._extract_text(properties), 1, '')

        if not self._document_exists(uid):
            self._database.add_document(document)
        else:
            self._database.replace_document(_PREFIX_UID + uid, document)
        self._database.flush()

    def _extract_text(self, properties):
        text = ''
        for key, value in properties.items():
            if key not in _PROPERTIES_NOT_TO_INDEX:
                if text:
                    text += ' '
                text += value
        return text

    def find(self, query):
        enquire = Enquire(self._database)
        enquire.set_query(self._parse_query(query))

        offset = query.get('offset', 0)
        limit = query.get('limit', _MAX_LIMIT)

        # This will assure that the results count is exact.
        check_at_least = offset + limit + 1

        enquire.set_sort_by_value(_VALUE_TIMESTAMP, True)

        query_result = enquire.get_mset(offset, limit, check_at_least)
        total_count = query_result.get_matches_estimated()

        uids = []
        for hit in query_result:
            uids.append(hit.document.get_value(_VALUE_UID))

        return (uids, total_count)

    def _parse_query(self, query_dict):
        logging.debug('_parse_query %r' % query_dict)
        queries = []

        if query_dict.has_key('query'):
            query_parser = QueryParser()
            query_parser.set_database(self._database)
            #query_parser.set_default_op(Query.OP_AND)

            # TODO: we should do stemming, but in which language?
            #query_parser.set_stemmer(_xapian.Stem(lang))
            #query_parser.set_stemming_strategy(qp.STEM_SOME)

            query = query_parser.parse_query(
                    query_dict['query'],
                    QueryParser.FLAG_PHRASE |
                            QueryParser.FLAG_BOOLEAN |
                            QueryParser.FLAG_LOVEHATE |
                            QueryParser.FLAG_WILDCARD,
                    '')

            queries.append(query)

        self._replace_mtime(query_dict)
        if query_dict.has_key('timestamp'):
            start = str(query_dict['timestamp'].pop('start', 0))
            end = str(query_dict['timestamp'].pop('end', sys.maxint))
            query = Query(Query.OP_VALUE_RANGE, _VALUE_TIMESTAMP, start, end)
            queries.append(query)

        if query_dict.has_key('uid'):
            queries.append(Query(_PREFIX_UID + query_dict['uid']))

        if query_dict.has_key('activity'):
            queries.append(Query(_PREFIX_ACTIVITY + query_dict['activity']))

        if not queries:
            queries.append(Query(''))
        
        return Query(Query.OP_AND, queries)

    def _replace_mtime(self, query):
        # TODO: Just a hack for the current journal that filters by mtime
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
        if query.has_key('mtime'):
            mtime_range = query.pop('mtime')

            start = mtime_range['start'][:-7]
            start = time.mktime(time.strptime(start, DATE_FORMAT))

            end = mtime_range['end'][:-7]
            end = time.mktime(time.strptime(end, DATE_FORMAT))

            query['timestamp'] = {'start': start, 'end': end}

    def delete(self, uid):
        self._database.delete_document(_PREFIX_UID + uid)

    def get_activities(self):
        activities = []
        for term in self._database.allterms(_PREFIX_ACTIVITY):
            activities.append(term.term[len(_PREFIX_ACTIVITY):])
        return activities

