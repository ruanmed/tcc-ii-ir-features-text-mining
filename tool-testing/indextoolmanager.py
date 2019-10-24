import os
import time
import timeit
import pprint
import subprocess
from datetime import datetime
from arango import ArangoClient
from elasticsearch import Elasticsearch

import xml.etree.ElementTree as ET

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

    def get_text_from_child(self, tag):
        '''
        Recursive function to get full text from XML elements with tags.

        Parameters
        ----------
        tag : XML ElementTree element
            Element
        '''

        text = ' '
        if tag.text != None:
            text = str(text + tag.text)
        count = 0
        for child in tag:
            count = count + 1
            text = str(text + self.get_text_from_child(child))
        return text
    
    def get_documents_DB_AUTHORPROF(self, documents_xml_folder = 'db_authorprof/en/', truth_txt = 'db_authorprof/truth.txt'):
        '''
        Generates a list with all documents from DB_AUTHORPROF formatted files.

        Parameters
        ----------
        documents_xml_folder : str
            Folder that contains the XML files from the authors' documents (twits),
            must follow the DB_AUTHORPROF task XML format.

        truth_txt : str
            Truth TXT file with authors' classifications of gender { female | male },
            must follow the DB_AUTHORPROF task TXT format.
        '''
        documents = []

        lines = []
        separator = ':::'

        # Open the truth file
        with open(truth_txt) as f:
            lines = f.read().splitlines()

        # Iterates over the lines, and reads each author's XML file adding the documents to the list
        for line in lines:
            author_id, gender = line.split(separator)
            author_xml = documents_xml_folder + author_id + '.xml'

            # Open the author XML file
            tree_author = ET.parse(str(author_xml))
            root_author = tree_author.getroot()

            number = 1
            for child in root_author.iter():
                document = { 'id' : str(author_id + '-'+ str(number)), 'gender' : str(gender),
                             'text' : child.text
                }
                number = number + 1
                documents.append(document)

        return documents

    def get_documents_DB_BOTGENDER(self, documents_xml_folder = 'db_botgender/en/', truth_txt = 'db_authorprof/truth.txt'):
        '''
        Generates a list with all documents from DB_BOTGENDER formatted files.

        Parameters
        ----------
        documents_xml_folder : str
            Folder that contains the XML files from the authors' documents (twits),
            must follow the DB_BOTGENDER task XML format.

        truth_txt : str
            Truth TXT file with authors' classifications of kind {bot | human} and gender { bot | female | male },
            must follow the DB_BOTGENDER task TXT format.
        '''
        documents = []

        lines = []
        separator = ':::'

        # Open the truth file
        with open(truth_txt) as f:
            lines = f.read().splitlines()

        # Iterates over the lines, and reads each author's XML file adding the documents to the list
        for line in lines:
            author_id, kind, gender = line.split(separator)
            author_xml = documents_xml_folder + author_id + '.xml'

            # Open the author XML file
            tree_author = ET.parse(str(author_xml))
            root_author = tree_author.getroot()

            number = 1
            for child in root_author.iter():
                document = { 'id' : str(author_id + '-'+ str(number)), 'kind' : str(kind), 'gender' : str(gender),
                             'text' : child.text
                }
                number = number + 1
                documents.append(document)

        return documents

    def get_documents_DB_HYPERPARTISAN(self, articles_xml = 'db_hyperpartisan/articles.xml', ground_truth_xml = 'db_hyperpartisan/ground_truth.xml'):
        '''
        Generates a list with all documents from DB_HYPERPARTISAN formatted files.

        Parameters
        ----------
        articles_xml : str
            Articles XML file name, the file must have articles surrounded by <article> tags,
            must follow the DB_HYPERPARTISAN task XML format.

        ground_truth_xml : str
            Articles ground truth XML file with articles surrounded by <article> tags,
            must follow the DB_HYPERPARTISAN task XML format.
        '''

        documents = []
        # Openning the XML files
        tree_articles = ET.parse(str(articles_xml))
        root_articles = tree_articles.getroot()
        tree_ground_truth = ET.parse(str(ground_truth_xml))
        root_ground_truth = tree_ground_truth.getroot()

        for a_child, g_child in zip(root_articles, root_ground_truth):
            document = {**a_child.attrib, **g_child.attrib, 'text' : str(self.get_text_from_child(a_child))}
            documents.append(document)
        return documents

    def initializeArango(self):
        '''
        Initialize ArangoDB with the specific parameters used by the repository,
        also sets it up to the research, creating the collection and view needed.

        Parameters
        ----------
        none :
            none
        '''

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
        '''
        Inserts a document in the ArangoDB 'indexName' collection.

        Parameters
        ----------
        itemKey : str or number
            Document identifier

        itemBody : dict
            Document body/data.
        '''

        document = {'_key': itemKey }
        document.update(itemBody)
        self.arangoCollection.insert(document)
    
    def insertDocumentArango(self, document):
        '''
        Inserts a document in the ArangoDB 'indexName' collection.

        Parameters
        ----------
        document : dict
            Document to be inserted, might contain a '_key' or '_id' value, 
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

    def queryArango(self, query):
        aqlquery = f"FOR d IN {str(self.arangoViewName)} SEARCH ANALYZER(d.text IN TOKENS('{str(query)}', 'text_en'), 'text_en') SORT BM25(d, {float(self.bm25_k1)}, {float(self.bm25_b)}) DESC LET myScore = BM25(d, {float(self.bm25_k1)}, {float(self.bm25_b)}) RETURN {{ doc: d, score: myScore }}"
        cursor = self.arangoDb.aql.execute(aqlquery, count=True)
        
        for item in cursor.batch():
            print(item['doc']['docno'], item['score'])

    def initializeElastic(self):
        '''
        Initialize Elasticsearch with the specific parameters used by the repository,
        setting it up to the research.

        Parameters
        ----------
        none :
            none
        '''
        # Initialize the Elasticsearch client.
        self.elasticClient = Elasticsearch(hosts='http://localhost:9200')

        self.elasticDocumentType = '_doc'

    def insertElastic(self, itemKey, itemBody):        
        '''
        Inserts a document in the Elasticsearch database.

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


# bulkBody = testTool.bulkInsertGeneratorElastic([{'id':'23232', 'text': 'hueheuheu'}, {'id':'12345678', 'text': 'hmmmmmm'}])

# print(bulkBody)

# pp = pprint.PrettyPrinter(indent=4)
# # print(items[0].childNodes)

# t_index = timeit.timeit(index_DB_HYPERPARTISAN_TOOL_ARANGO, setup="gc.enable()", number=1)

# # pp.pprint(testTool.get_documents_DB_HYPERPARTISAN(articles_xml, ground_truth_xml))
# print(testTool.get_documents_DB_BOTGENDER('db_botgender/en/','truth.txt'))