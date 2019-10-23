from datetime import datetime
from arango import ArangoClient

index_name = "test_moby_reduced"

moby_reduced = {
    15: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 15',
        'text': '''"In that day, the Lord with his sore, and great, and strong sword,
shall punish Leviathan the piercing serpent, even Leviathan that
crooked serpent; and he shall slay the dragon that is in the sea."
--ISAIAH''',
        'timestamp': str(datetime.now()),
    },
    16: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 16',
        'text': '''"And what thing soever besides cometh within the chaos of this
monster's mouth, be it beast, boat, or stone, down it goes all
incontinently that foul great swallow of his, and perisheth in the
bottomless gulf of his paunch." --HOLLAND'S PLUTARCH'S MORALS.''',
        'timestamp': str(datetime.now()),
    },
    17: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 17',
        'text': '''"The Indian Sea breedeth the most and the biggest fishes that are:
among which the Whales and Whirlpooles called Balaene, take up as
much in length as four acres or arpens of land." --HOLLAND'S PLINY.''',
        'timestamp': str(datetime.now()),
    },
    18: {
        'author': 'Herman Melville',
        'docno': 'Chapter 0, Paragraph 18',
        'text': '''"Scarcely had we proceeded two days on the sea, when about sunrise a
great many Whales and other monsters of the sea, appeared.  Among the
former, one was of a most monstrous size. ...  This came towards us,
open-mouthed, raising the waves on all sides, and beating the sea
before him into a foam." --TOOKE'S LUCIAN.  "THE TRUE HISTORY."''',
        'timestamp': str(datetime.now()),
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
        'timestamp': str(datetime.now()),
    }
}


# Initialize the ArangoDB client.
client = ArangoClient(hosts='http://localhost:8529')

# Connect to "_system" database as root user.
# This returns an API wrapper for "_system" database.
sys_db = client.db('_system', username=None, password=None)

# Create a new database named "test" if it does not exist.
if not sys_db.has_database(index_name):
    sys_db.create_database(index_name)

# Connect to "test" database as root user.
# This returns an API wrapper for "test" database.
db = client.db(index_name, username=None, password=None)


# Create a new collection named "students" if it does not exist.
# This returns an API wrapper for "students" collection.
if db.has_collection(index_name):
    students = db.collection(index_name)
else:
    students = db.create_collection(index_name)

# # Add a hash index to the collection.
# students.add_hash_index(fields=['name'], unique=False)


# Retrieve list of views.
view_list = db.views()

print(view_list)
# Create a view.

if not view_list:
    db.create_view(
        name=str('v_' + index_name),
        view_type='arangosearch',
        properties={
            'cleanupIntervalStep': 0,
            'consolidationIntervalMsec': 0,
            'links': {
                index_name: {
                    "analyzers" : [
                        "text_en" 
                    ],
                    "includeAllFields" : True
                }
            }
        }
    )


# Truncate the collection.
# students.truncate()

# # Insert new documents into the collection.
# students.insert({'name': 'jane', 'age': 19, 'text': 'WHAT motherfreaking nice guys. Nós speakamos english. E você? Do you speaka? Hein meu jovem?'})
# students.insert({'name': 'josh', 'age': 18, 'text': 'Mais uma vez testando o arangodb, meus jovens. Uma delícia esse abacate.'})
# students.insert({'name': 'jake', 'age': 21, 'text': 'Bora testar isso daqui né jovens. Bora ver se dá certo essa consulta marota.'})
if not view_list:
    for item in moby_reduced:
        res = students.insert(moby_reduced[item])
        print(res)
    
aqlquery = "FOR d IN v_" + index_name + " SEARCH ANALYZER(d.text IN TOKENS('sea whales', 'text_en'), 'text_en') SORT BM25(d, 1.2, 0.75) DESC LET myScore = BM25(d, 1.2, 0.75) RETURN { doc: d, score: myScore }"

# Execute an AQL query. This returns a result cursor.
# cursor = db.aql.execute("FOR doc IN v_studentes SEARCH ANALYZER(doc.text IN TOKENS('meus delícia bora', 'identity'), 'identity') SORT BM25(doc, 1.2, 0.75) DESC RETURN doc")
#  
# aqlquery = "FOR d IN v_studentes SEARCH ANALYZER(d.text IN TOKENS('what', 'text_en'), 'text_en') SORT BM25(d, 1.2, 0.75) DESC LET myScore = BM25(d, 1.2, 0.75) RETURN { doc: d, score: myScore }"
# validate = db.aql.validate(aqlquery,)
# print(validate)
cursor = db.aql.execute(aqlquery, count=True)
# cursor = db.aql.explain("FOR doc IN students RETURN doc")
# Iterate through the result cursor
# student_keys = [doc['_key'] for doc in cursor]

# Iterate through the cursor to retrieve the documents.
# student_names = [document['name'] for document in cursor]
# print(cursor)
for item in cursor.batch():
    print(item['doc']['docno'], item['score'])
# print(cursor.count())

# print(student_keys)
# for documents in cursor:
#     print(documents)
#     print("VERY NICE")
# print("NOT NICE")




# from pyArango.connection import *

# conn = Connection(arangoURL='http://localhost:8529')

# db = conn["testttt"]
# aql = "FOR doc IN v_studentes RETURN doc"
# bindVars = {'name': 'Tesla-3'}
# # by setting rawResults to True you'll get dictionaries instead of Document objects, useful if you want to result to set of fields for example
# queryResult = db.AQLQuery(aql, rawResults=False, batchSize=10)
# # document = queryResult[0]
# print(queryResult)
# # print(document)