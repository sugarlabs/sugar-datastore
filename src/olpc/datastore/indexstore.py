import os

import xapian
from xapian import WritableDatabase, Document, Enquire, Query

_MAX_LIMIT = 4096

_VALUE_UID = 0
_VALUE_TIMESTAMP = 1
_VALUE_ACTIVITY_ID = 2
_VALUE_MIME_TYPE = 3
_VALUE_ACTIVITY = 4
_VALUE_KEEP = 5

class IndexStore(object):
    def __init__(self, root_path):
        index_path = os.path.join(root_path, 'index')
        self._database = WritableDatabase(index_path, xapian.DB_CREATE_OR_OPEN)

    def _document_exists(self, uid):
        postings = self._database.postlist('Q' + uid)
        try:
            postlist_item = postings.next()
        except StopIteration:
            return False
        return True

    def store(self, uid, properties):
        document = Document()
        document.add_term('Q' + uid)
        document.add_value(_VALUE_UID, uid)
        document.add_value(_VALUE_TIMESTAMP, str(properties['timestamp']))
        document.add_value(_VALUE_ACTIVITY_ID, properties['activity_id'])
        document.add_value(_VALUE_MIME_TYPE, str(properties['keep']))
        document.add_value(_VALUE_ACTIVITY, properties['activity'])

        if not self._document_exists(uid):
            self._database.add_document(document)
        else:
            self._database.replace_document('Q' + uid, document)
        self._database.flush()

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
        queries = []

        if query_dict.has_key('uid'):
            queries.append(Query('Q' + query_dict['uid']))

        #if query_dict.has_key('timestamp'):
        #    queries.append(Query('Q' + query_dict['uid']))

        if not queries:
            queries.append(Query(''))
        
        return Query(Query.OP_AND, queries)

    def delete(self, uid):
        self._database.delete_document('Q' + uid)

