import sys, json
from elasticsearch import Elasticsearch
es = Elasticsearch()

PLAATSEN = json.load(open('PLAATSEN.json'))

def dump_at_countries():
    for v in PLAATSEN.values():
      if v['pid'] == '35597':    
        for vv in PLAATSEN.values():
            if vv['pid'] == v['sid']:
                count = len([vvv for vvv in PLAATSEN.values() if vvv['pid'] == vv['sid']])
                print vv['sid'], count, v['term'].encode('utf8'), '~', vv['term'].encode('utf8')

def find_c(lemma):
    q = {
        "bool" : {
            "must" : {
                "match" : { "prefterm" : lemma['term'] }
            },
            "should" : [
                {
                    "term" : { "parents" : "7012149" }
                }
            ],
            "minimum_should_match" : 1,
            "boost" : 1.0
        }
    }
    for x in es.search(index="tgn", body={"query": q})['hits']['hits']:
        print x['_source']['prefterm'], x['_source']['parents']

def find(term):
    q = {"match": {"prefterm": term}}
    for x in es.search(index="tgn", body={"query": q})['hits']['hits']:
        xx = x['_source']
        print xx['prefterm'], xx['sid'], xx['parents']