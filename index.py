import json, sys, sqlite3
from elasticsearch import Elasticsearch

data = {}
es = Elasticsearch()
db = sqlite3.connect('x.sqlite3')
cur = db.cursor()

def build_parents(lemma):
    parent = data.get(lemma.get('pid'))
    while parent and parent['pid'] != '1000000':
        yield parent
        parent = data.get(parent.get('pid'))

def db_parents(anid):
    r = cur.execute('SELECT pid FROM parents WHERE id = ?', (anid,))
    try:
        val = r.next()
        pid = val[0]
        if pid != '1000000':
            yield pid
            for value in db_parents(pid):
                yield value
    except StopIteration:
        pass

if __name__ == '__main__':
    for x in sys.argv[1:]:
        sys.stderr.write('\rLoading: %s               ' % x)
        data.update(json.load(open(x)))

    idx = 0
    for k,v in data.items():
        idx += 1
        sys.stderr.write('\r%s' % (idx/len(data)))
        v['parents'] = list(db_parents(v['sid']))
        es.index(index="tgn", doc_type="lemma", id=int(k), body=v)

# es.search(index="tgn", body={"query": {"match": {"prefterm": ""}}})
# q = {
#     "bool" : {
#         "must" : {
#             "match" : { "prefterm" : "Amsterdam" }
#         },
#         "should" : [
#             {
#                 "term" : { "parents" : "7012149" }
#             }
#         ],
#         "minimum_should_match" : 1,
#         "boost" : 1.0
#     }
# }    
# for x in es.search(index="tgn", body={"query": q})['hits']['hits']:
#   print x['_source']['prefterm'], x['_source']['parents']
