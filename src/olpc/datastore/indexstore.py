# Copyright (C) 2008, One Laptop Per Child
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import logging
import time
import os

import gobject
import xapian
from xapian import WritableDatabase, Document, Enquire, Query, QueryParser

from olpc.datastore import layoutmanager
from olpc.datastore.layoutmanager import MAX_QUERY_LIMIT

_VALUE_UID = 0
_VALUE_TIMESTAMP = 1

_PREFIX_UID = 'Q'
_PREFIX_ACTIVITY = 'A'
_PREFIX_ACTIVITY_ID = 'I'
_PREFIX_MIME_TYPE = 'M'

# Force a flush every _n_ changes to the db
_FLUSH_THRESHOLD = 20

# Force a flush after _n_ seconds since the last change to the db
_FLUSH_TIMEOUT = 60

_PROPERTIES_NOT_TO_INDEX = ['timestamp', 'activity_id', 'keep', 'preview']

_MAX_RESULTS = int(2 ** 31 - 1)

class IndexStore(object):
    """Index metadata and provide rich query facilities on it.
    """ 
    def __init__(self):
        self._database = None
        self._flush_timeout = None
        self._pending_writes = 0

    def open_index(self):
        index_path = layoutmanager.get_instance().get_index_path()
        self._database = WritableDatabase(index_path, xapian.DB_CREATE_OR_OPEN)

    def close_index(self):
        self._database.flush()
        self._database = None

    def remove_index(self):
        index_path = layoutmanager.get_instance().get_index_path()
        if not os.path.exists(index_path):
            return
        for f in os.listdir(index_path):
            os.remove(os.path.join(index_path, f))

    def contains(self, uid):
        postings = self._database.postlist(_PREFIX_UID + uid)
        try:
            postlist_item = postings.next()
        except StopIteration:
            return False
        return True

    def store(self, uid, properties):
        document = Document()
        document.add_term(_PREFIX_UID + uid)
        document.add_term(_PREFIX_ACTIVITY + properties.get('activity', ''))
        document.add_term(_PREFIX_MIME_TYPE + properties.get('mime_type', ''))
        document.add_term(_PREFIX_ACTIVITY_ID +
                          properties.get('activity_id', ''))

        document.add_value(_VALUE_UID, uid)
        document.add_value(_VALUE_TIMESTAMP, str(properties['timestamp']))

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

        if not self.contains(uid):
            self._database.add_document(document)
        else:
            self._database.replace_document(_PREFIX_UID + uid, document)
        self._flush()

    def _extract_text(self, properties):
        text = ''
        for key, value in properties.items():
            if key not in _PROPERTIES_NOT_TO_INDEX:
                if text:
                    text += ' '
                if isinstance(value, unicode):
                    value = value.encode('utf-8')
                elif not isinstance(value, basestring):
                    value = str(value)
                text += value
        return text

    def find(self, query):
        enquire = Enquire(self._database)
        enquire.set_query(self._parse_query(query))

        offset = query.get('offset', 0)
        limit = query.get('limit', MAX_QUERY_LIMIT)

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

        query_str = query_dict.pop('query', None)
        if query_str is not None:
            query_parser = QueryParser()
            query_parser.set_database(self._database)
            #query_parser.set_default_op(Query.OP_AND)

            # TODO: we should do stemming, but in which language?
            #query_parser.set_stemmer(_xapian.Stem(lang))
            #query_parser.set_stemming_strategy(qp.STEM_SOME)

            query = query_parser.parse_query(
                    query_str['query'],
                    QueryParser.FLAG_PHRASE |
                            QueryParser.FLAG_BOOLEAN |
                            QueryParser.FLAG_LOVEHATE |
                            QueryParser.FLAG_WILDCARD,
                    '')

            queries.append(query)

        self._replace_mtime(query_dict)
        timestamp = query_dict.pop('timestamp', None)
        if timestamp is not None:
            start = str(timestamp.pop('start', 0))
            end = str(timestamp.pop('end', _MAX_RESULTS))
            query = Query(Query.OP_VALUE_RANGE, _VALUE_TIMESTAMP, start, end)
            queries.append(query)

        uid = query_dict.pop('uid', None)
        if uid is not None:
            queries.append(Query(_PREFIX_UID + uid))

        activity = query_dict.pop('activity', None)
        if activity is not None:
            queries.append(Query(_PREFIX_ACTIVITY + activity))

        activity_id = query_dict.pop('activity_id', None)
        if activity_id is not None:
            query = Query(_PREFIX_ACTIVITY_ID + activity_id)
            queries.append(query)

        mime_type = query_dict.pop('mime_type', None)
        if mime_type is not None:
            mime_queries = []
            for mime_type in mime_type:
                mime_queries.append(Query(_PREFIX_MIME_TYPE + mime_type))
            queries.append(Query(Query.OP_OR, mime_queries))

        if not queries:
            queries.append(Query(''))

        if query_dict:
            logging.warning('Unknown term(s): %r' % query_dict)
        
        return Query(Query.OP_AND, queries)

    def _replace_mtime(self, query):
        # TODO: Just a hack for the current journal that filters by mtime
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
        if query.has_key('mtime'):
            mtime_range = query.pop('mtime')

            start = mtime_range['start'][:-7]
            start = time.mktime(time.strptime(start, DATE_FORMAT))

            end = mtime_range['end'][:-7]
            # FIXME: this will give an unexpected result if the journal is in a
            # different timezone
            end = time.mktime(time.strptime(end, DATE_FORMAT))

            query['timestamp'] = {'start': int(start), 'end': int(end)}

    def delete(self, uid):
        self._database.delete_document(_PREFIX_UID + uid)

    def get_activities(self):
        activities = []
        for term in self._database.allterms(_PREFIX_ACTIVITY):
            activities.append(term.term[len(_PREFIX_ACTIVITY):])
        return activities

    def _flush_timeout_cb(self):
        self._flush(True)
        return False

    def _flush(self, force=False):
        """Called after any database mutation"""
        logging.debug('IndexStore.flush: %r %r' % (force, self._pending_writes))

        if self._flush_timeout is not None:
            gobject.source_remove(self._flush_timeout)
            self._flush_timeout = None

        self._pending_writes += 1
        if force or self._pending_writes > _FLUSH_THRESHOLD:
            self._database.flush()
            self._pending_writes = 0
        else:
            self._flush_timeout = gobject.timeout_add(_FLUSH_TIMEOUT * 1000,
                                                      self._flush_timeout_cb)

