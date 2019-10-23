from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(hosts='http://localhost:9200')

index_name = "test-moby-reduced"

moby_reduced = {
    15: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 15',
        'text': '''"In that day, the Lord with his sore, and great, and strong sword,
shall punish Leviathan the piercing serpent, even Leviathan that
crooked serpent; and he shall slay the dragon that is in the sea."
--ISAIAH''',
        'timestamp': datetime.now(),
    },
    16: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 16',
        'text': '''"And what thing soever besides cometh within the chaos of this
monster's mouth, be it beast, boat, or stone, down it goes all
incontinently that foul great swallow of his, and perisheth in the
bottomless gulf of his paunch." --HOLLAND'S PLUTARCH'S MORALS.''',
        'timestamp': datetime.now(),
    },
    17: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 17',
        'text': '''"The Indian Sea breedeth the most and the biggest fishes that are:
among which the Whales and Whirlpooles called Balaene, take up as
much in length as four acres or arpens of land." --HOLLAND'S PLINY.''',
        'timestamp': datetime.now(),
    },
    18: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 18',
        'text': '''"Scarcely had we proceeded two days on the sea, when about sunrise a
great many Whales and other monsters of the sea, appeared.  Among the
former, one was of a most monstrous size. ...  This came towards us,
open-mouthed, raising the waves on all sides, and beating the sea
before him into a foam." --TOOKE'S LUCIAN.  "THE TRUE HISTORY."''',
        'timestamp': datetime.now(),
    },
    19: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 19',
        'text': '''"He visited this country also with a view of catching horse-whales,
which had bones of very great value for their teeth, of which he
brought some to the king. ...  The best whales were catched in his
own country, of which some were forty-eight, some fifty yards long.
He said that he was one of six who had killed sixty in two days."
--OTHER OR OCTHER'S VERBAL NARRATIVE TAKEN DOWN FROM HIS MOUTH BY
KING ALFRED, A.D. 890.''',
        'timestamp': datetime.now(),
    }
}

for item in moby_reduced:
    res = es.index(index=index_name, doc_type='paragraph', id=item, body=moby_reduced[item])
    print(res['result'])

# res = es.index(index="test-index", doc_type='tweet', id=17, body=doc)

# res = es.get(index=index_name, doc_type='paragraph', id=19)
# print(res['_source'])

es.indices.refresh(index=index_name)

# res = es.search(index=index_name, body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total']['value'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

res = es.search(index=index_name, body={"query": {"match": {"text": "sea whales"}}})

for hit in res['hits']['hits']:
    print([hit['_score'], hit['_id']])

print("SECOND QUERY")

res = es.search(index=index_name, body={"query": {"match": {"text": "the"}}})

for hit in res['hits']['hits']:
    print([hit['_score'], hit['_id']])
