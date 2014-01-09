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