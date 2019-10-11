from arango import ArangoClient

# Initialize the ArangoDB client.
client = ArangoClient(hosts='http://localhost:8529')

# Connect to "_system" database as root user.
# This returns an API wrapper for "_system" database.
sys_db = client.db('_system', username=None, password=None)

# Create a new database named "test" if it does not exist.
if not sys_db.has_database('testttt'):
    sys_db.create_database('testttt')

# Connect to "test" database as root user.
# This returns an API wrapper for "test" database.
db = client.db('testttt', username=None, password=None)
db_view = db.view('v_studentes')
print(db.views())

# Create a new collection named "students" if it does not exist.
# This returns an API wrapper for "students" collection.
if db.has_collection('students'):
    students = db.collection('students')
else:
    students = db.create_collection('students')

# # Add a hash index to the collection.
# students.add_hash_index(fields=['name'], unique=False)

# Truncate the collection.
# students.truncate()

# # Insert new documents into the collection.
# students.insert({'name': 'jane', 'age': 19, 'text': 'WHAT motherfreaking nice guys. Nós speakamos english. E você? Do you speaka? Hein meu jovem?'})
# students.insert({'name': 'josh', 'age': 18, 'text': 'Mais uma vez testando o arangodb, meus jovens. Uma delícia esse abacate.'})
# students.insert({'name': 'jake', 'age': 21, 'text': 'Bora testar isso daqui né jovens. Bora ver se dá certo essa consulta marota.'})

# Execute an AQL query. This returns a result cursor.
# cursor = db.aql.execute("FOR doc IN v_studentes SEARCH ANALYZER(doc.text IN TOKENS('meus delícia bora', 'identity'), 'identity') SORT BM25(doc, 1.2, 0.75) DESC RETURN doc")
#  
aqlquery = "FOR d IN v_studentes SEARCH ANALYZER(d.text IN TOKENS('what', 'text_en'), 'text_en') SORT BM25(d, 1.2, 0.75) DESC LET myScore = BM25(d, 1.2, 0.75) RETURN { doc: d, score: myScore }"
# validate = db.aql.validate(aqlquery,)
# print(validate)
cursor = db.aql.execute(aqlquery, count=True)
# cursor = db.aql.explain("FOR doc IN students RETURN doc")
# Iterate through the result cursor
# student_keys = [doc['_key'] for doc in cursor]

# Iterate through the cursor to retrieve the documents.
# student_names = [document['name'] for document in cursor]
print(cursor)
print(cursor.batch())
print(cursor.count())

# print(student_keys)
# for documents in cursor:
#     print(documents)
#     print("VERY NICE")
# print("NOT NICE")




from pyArango.connection import *

conn = Connection(arangoURL='http://localhost:8529')

db = conn["testttt"]
aql = "FOR doc IN v_studentes RETURN doc"
bindVars = {'name': 'Tesla-3'}
# by setting rawResults to True you'll get dictionaries instead of Document objects, useful if you want to result to set of fields for example
queryResult = db.AQLQuery(aql, rawResults=False, batchSize=10)
# document = queryResult[0]
print(queryResult)
# print(document)