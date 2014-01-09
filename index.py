import json, sys, sqlite3, traceback
from elasticsearch import Elasticsearch

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
        data = json.load(open(x))
        idx = 0
        for k,v in data.items():
            idx += 1
            sys.stderr.write('\r%s%%' % (float(idx)/len(data)*100))
            v['parents'] = list(db_parents(v['sid']))
            try:
                lat = float(v['lat'])
                lon = float(v['long'])
                v['location'] = {'lat': lat, 'lon': lon}
                del v['lat']
                del v['long']
            except:
                continue
            es.index(index="tgn", doc_type="lemma", id=int(k), body=v)