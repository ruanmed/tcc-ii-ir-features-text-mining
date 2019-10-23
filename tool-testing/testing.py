import time
import subprocess
from datetime import datetime
from arango import ArangoClient
from elasticsearch import Elasticsearch



class IndexToolManager:
    '''
    A class used to manage the database indexation tools used in this research.
    Provides functions to index a database with ArangoDB, Elasticsearch and Zettair,
    using the BM25 IR function implemented in each of those tools.
    Also makes it possible to query the indexed database using BM25.

    Attributes
    ----------
    indexName : str
        a string to refer to the current working data set
    
    bm25_b : float
        BM25 b parameter to adjust the document length compensation

    bm25_k1 : float
        BM25 k1 parameter to adjust the term-frequency weight

    bm25_k3 : float
        BM25 k3 parameter to adjust the term-frequency weight in the query (used for long queries)

    Methods
    -------
    initializeArango()
       Initializes ArangoDB, connect to a client, creates/connects to collection and view.

    '''

    def __init__(self, indexName = 'default_index', bm25_b = 0.75, bm25_k1 = 1.2, bm25_k3 = 0.0):
        self.indexName = indexName
        self.bm25_b = bm25_b
        self.bm25_k1 = bm25_k1
        self.bm25_k3 = bm25_k3

        self.initializeArango()
        self.initializeElastic()

    def initializeArango(self):
        # Initialize the ArangoDB client.
        self.arangoClient = ArangoClient(hosts='http://localhost:8529')

        # Connect to "_system" database as root user.
        # This returns an API wrapper for "_system" database.
        sys_db = self.arangoClient.db('_system', username=None, password=None)

        index_name = self.indexName
        # Create a new database named "test" if it does not exist.
        if not sys_db.has_database(index_name):
            sys_db.create_database(index_name)
                
        # Connect to "test" database as root user.
        # This returns an API wrapper for "test" database.
        self.arangoDb = self.arangoClient.db(index_name, username=None, password=None)

        db = self.arangoDb
        # Create a new collection named "students" if it does not exist.
        # This returns an API wrapper for "students" collection.
        if db.has_collection(index_name):
            self.arangoCollection = db.collection(index_name)
        else:
            self.arangoCollection = db.create_collection(index_name)

        # Retrieve list of views.
        view_list = db.views()

        # Creates the view used by the Analyzer to Search and use BM25
        self.arangoViewName = str('v_' + index_name)
        if not view_list:
            db.create_view(
                name=self.arangoViewName,
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

    def insertArango(self, itemKey, itemBody):

        document = {'_key': itemKey }
        document.update(itemBody)
        self.arangoCollection.insert(document)
    
    def insertDocumentArango(self, document):
        '''
        Inserts a document in the ArangoDB 'indexName' collection.

        Parameters
        ----------
        document : dict
            Document, might contain a '_key' or '_id' value, 
            e.g.: '_key' : ' document1',  or '_id' : 'collection_name/document1'
        '''

        self.arangoCollection.insert(document)

    def bulkListGeneratorArango(self, bulkItems):
        '''
        Generates bulk documents ready to import to the ArangoDB collection.

        Parameters
        ----------
        bulkItems : list
            Bulk items to be processed, must contain an 'id' field.
        '''

        documentList = []
        tempdict = bulkItems.copy()
        for item in tempdict:
            document = {
                        '_key': item.pop('id'),
                        **item
            }
            documentList.append(document)

        return documentList

    def bulkImportArango(self, documentList):
        '''
        Bulk import to ArangoDB collection.

        Parameters
        ----------
        documentList : list of dicts
            List of documents to be inserted in the ArangoDB collection.
            Every document must have an '_key' field.
            e.g. of document list: 
                [{ '_key': 'document1', 'field1' : 'value1', 'field2' : 'value2'}, 
                { '_key': 'document2', 'field1' : 'value4', 'field2' : 'value5'}]
        '''

        self.arangoCollection.import_bulk(documentList)

    def initializeElastic(self):
        # Initialize the Elasticsearch client.
        self.elasticClient = Elasticsearch(hosts='http://localhost:9200')

        self.elasticDocumentType = '_doc'

    def insertElastic(self, itemKey, itemBody):        
        '''
        Inserts a document in a Elasticsearch database.

        Parameters
        ----------
        itemKey : str or number
            Document identifier

        itemBody : dict
            Document body/data.
        '''

        self.elasticClient.index(index=self.indexName, doc_type=self.elasticDocumentType, id=itemKey, body=itemBody)

    def bulkInsertGeneratorElastic(self, bulkItems):
        '''
        Generates a bulk body of insert Elasticsearch operations.

        Parameters
        ----------
        bulkItems : list
            Bulk items to be processed, must contain an 'id' field.
        '''

        bulkBody = []
        tempdict = bulkItems.copy()
        # item['_id'] = item.pop('id')
        for item in tempdict:
            action = [
                { 'index' :
                    {
                        "_index": self.indexName,
                        "_id": item.pop('id')
                    }
                },
                item
            ]
            bulkBody.extend(action)

        return bulkBody           

    def bulkElastic(self, bulkBody):
        '''
        Bulk Elasticsearch operations.

        Parameters
        ----------
        bulkBody : list or str with operations separated by newlines ('\n')
            Bulk operations to be executed, already in the format and order to be executed.
            All operations must have an '_id' in their metadata field.
            e.g. of index operation over 'index_name' index: 
                [{ 'index': {'_index': 'index_name', '_id' : 'document_id'}, {'field1' : 'value1'}]
        '''

        self.elasticClient.bulk(index=self.indexName, body=bulkBody)

    def queryElastic(self, query):
        result = self.elasticClient.search(index=self.indexName, body={"query": {"match": {"text": str(query)}}})
        for hit in result['hits']['hits']:
            print([hit['_score'], hit['_id']])

testdic = { 'key1':'value2', 'key3':'value3'}

print(testdic['key1'])

testTool = IndexToolManager(indexName='default_index')

bulkBody = testTool.bulkInsertGeneratorElastic([{'id':'23232', 'text': 'hueheuheu'}, {'id':'12345678', 'text': 'hmmmmmm'}])

print(bulkBody)