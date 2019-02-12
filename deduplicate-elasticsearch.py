#!/usr/local/bin/python3

# A description and analysis of this code can be found at
# https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/
# Improvements on:
from elasticsearch import Elasticsearch

es = Elasticsearch(["localhost:9200"])
dict_of_duplicate_docs = {}

# The following line defines the fields that will be
# used to determine if a document is a duplicate
keys_to_include_in_hash = ["CAC", "FTSE", "SMI"]

#The index to look for documents
es_doc_index = "stocks"

# dry_run will just print all documents
# else, keep only the first document
dry_run = True

# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hits):
    for item in hits:
        combined_key = ""
        for mykey in keys_to_include_in_hash:
            combined_key += str(item['_source'][mykey])

        _id = item["_id"]

        dict_of_duplicate_docs.setdefault(combined_key.encode('utf-8'), []).append(_id)


# Loop over all documents in the index, and populate the
# dict_of_duplicate_docs data structure.
def scroll_over_all_docs():
    data = es.search(index=es_doc_index, scroll='1m',  body={"query": {"match_all": {}}})

    # Get the scroll ID
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    # Before scroll, process current batch of hits
    populate_dict_of_duplicate_docs(data['hits']['hits'])

    while scroll_size > 0:
        data = es.scroll(scroll_id=sid, scroll='2m')

        # Process current batch of hits
        populate_dict_of_duplicate_docs(data['hits']['hits'])

        # Update the scroll ID
        sid = data['_scroll_id']

        # Get the number of results that returned in the last scroll
        scroll_size = len(data['hits']['hits'])


def loop_over_hashes_and_remove_duplicates(dry_run = True):
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
      if len(array_of_ids) > 1:
        print("********** Duplicate docs hash=%s **********" % hashval)
        # Get the documents that have mapped to the current hasval
        matching_docs = es.mget(index=es_doc_index, doc_type="doc", body={"ids": array_of_ids})

        if dry_run:
            for doc in matching_docs['docs']:
                print("doc=%s\n" % doc)
        else:
            for doc in matching_docs['docs'][1:]:
                print("doc=%s\n" % doc)
                 es.delete(index=es_doc_index,doc_type="doc",id=doc['_id'])


def main(dry_run):
    scroll_over_all_docs()
    loop_over_hashes_and_remove_duplicates(dry_run=dry_run)

main(dry_run)
