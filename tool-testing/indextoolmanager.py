# import os
import time
# import timeit
# import pprint
import math
import subprocess
# from datetime import datetime
from arango import ArangoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers as ElasticsearchHelpers

import xml.etree.ElementTree as ET

import pandas as pd
import json
from shlex import quote

default_db_names = {
    'authorprof': 'authorprof',
    'botgender': 'botgender',
    'hyperpartisan': 'hyperpartisan',
    'hyperpartisan_split_42': 'hyperpartisan_split_42'
}


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

    top_k : int
        Number of results to be retrieved when querying the database

    Methods
    -------
    initializeArango()
       Initializes ArangoDB, connect to a client, creates/connects to collection and view.

    '''

    def __init__(self, indexName='default_index',
                 bm25_b=0.75, bm25_k1=1.2, bm25_k3=0.0, top_k=100):
        self.indexName = indexName
        self.bm25_b = float(bm25_b)
        self.bm25_k1 = float(bm25_k1)
        self.bm25_k3 = float(bm25_k3)
        self.numberResults = int(top_k)
        self.root_path = "/home/ruan/Documentos/git/tcc-ii-ir-features-text-mining/tool-testing/"

        self.zettair_query_process = None

        self.initializeArango()
        self.initializeElastic()

        self.resultsIndexName = 'tcc_results'
        body = {
            "settings": {
                "number_of_shards": 1,
            }
        }
        if not self.elasticClient.indices.exists(index=self.resultsIndexName):
            self.elasticClient.indices.create(
                index=self.resultsIndexName, body=body)

        # Create a new database named "test" if it does not exist.
        if not self.arango_sys_db .has_database(self.resultsIndexName):
            self.arango_sys_db .create_database(self.resultsIndexName)

        # Connect to "test" database as root user.
        # This returns an API wrapper for "test" database.
        self.arangoResultsDb = self.arangoClient.db(
            self.resultsIndexName, username=None, password=None)

        db = self.arangoResultsDb
        # Create a new collection named "students" if it does not exist.
        # This returns an API wrapper for "students" collection.
        if db.has_collection(self.resultsIndexName):
            self.arangoResultsCollection = db.collection(self.resultsIndexName)
        else:
            self.arangoResultsCollection = db.create_collection(
                self.resultsIndexName)

    def get_parameters(self):
        parameters = {
            'indexName': str(self.indexName),
            'bm25_b': str(self.bm25_b),
            'bm25_k1': str(self.bm25_k1),
            'bm25_k3': str(self.bm25_k3),
            'top_k': str(self.numberResults),
        }

        return parameters

    def clean_current(self):
        self.delete_all([str(self.indexName)])

    def clean_default(self):
        default_list = []

        for item in default_db_names:
            default_list.append(str(item))
            default_list.append(str(item)+'_bulk')

        self.delete_all(default_list)

    def delete_all(self, index_list):
        '''
        Deletes the databases/indexes from all tools.

        Parameters
        ----------
        index_list : list
            String list of the database/indexes names.
        '''

        self.arango_delete(index_list)
        self.elastic_delete(index_list)

    def log_result(self, itemKey, itemBody):
        '''
        Inserts a document in the Elasticsearch database.

        Parameters
        ----------
        itemKey : str or number
            Document identifier

        itemBody : dict
            Document body/data.
        '''

        self.elasticClient.index(
            index=self.resultsIndexName, doc_type=self.elasticDocumentType,
            id=itemKey, body=itemBody)

        document = {'_key': itemKey}
        document.update(itemBody)
        self.arangoResultsCollection.insert(document)

    def get_text_from_child(self, tag):
        '''
        Recursive function to get full text from XML elements with tags.

        Parameters
        ----------
        tag : XML ElementTree element
            Element
        '''

        text = ' '
        if tag.text is not None:
            text = str(text + tag.text)
        count = 0
        for child in tag:
            count = count + 1
            text = str(text + self.get_text_from_child(child))
        return text

    def get_documents(self, db='authorprof',
                      documents_xml_folder='db_authorprof/en/',
                      truth_txt='db_authorprof/truth.txt',
                      append_class_to_id=False):
        '''
        Generates a list with all documents from db formatted files.

        Parameters
        ----------
        db : str
            Database name.

        documents_xml_folder : str
            Folder that contains the XML files from the authors' documents (twits),
            must follow the DB_AUTHORPROF task XML format.

        truth_txt : str
            Truth TXT file with authors' classifications of gender { female | male },
            must follow the DB_AUTHORPROF task TXT format.
        '''
        if (db == 'authorprof'):
            return self.get_documents_DB_AUTHORPROF(documents_xml_folder, truth_txt, append_class_to_id)
        if (db == 'botgender'):
            return self.get_documents_DB_BOTGENDER(documents_xml_folder, truth_txt, append_class_to_id)
        if (db == 'hyperpartisan'):
            return self.get_documents_DB_HYPERPARTISAN(documents_xml_folder, truth_txt, append_class_to_id)
        if (db == 'hyperpartisan_split_42'):
            return self.get_documents_DB_HYPERPARTISAN_split(documents_xml_folder, truth_txt, append_class_to_id)

        return []

    def get_documents_DB_AUTHORPROF(self,
                                    documents_xml_folder='db_authorprof/en/',
                                    truth_txt='db_authorprof/truth.txt',
                                    append_class_to_id=False):
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
            tree_author = ET.parse(str(author_xml),
                                   parser=ET.XMLParser(encoding="utf-8"))
            root_author = tree_author.getroot()

            number = 1
            for child in root_author[0]:
                document = {'id': str(author_id + '-' + str(number)),
                            'author_id': str(author_id),
                            'gender': str(gender), 'class': str(gender),
                            'text': child.text
                            }
                if append_class_to_id:
                    document['id'] += str(':' + str(document['class']))
                number = number + 1
                documents.append(document)

        return documents

    def get_documents_DB_BOTGENDER(self,
                                   documents_xml_folder='db_botgender/en/',
                                   truth_txt='db_authorprof/truth.txt',
                                   append_class_to_id=False):
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
            tree_author = ET.parse(str(author_xml),
                                   parser=ET.XMLParser(encoding="utf-8"))
            root_author = tree_author.getroot()

            number = 1
            for child in root_author[0]:
                document = {'id': str(author_id + '-' + str(number)),
                            'author_id': str(author_id),
                            'kind': str(kind), 'gender': str(gender),
                            'text': child.text, 'class': str(kind),
                            }
                if append_class_to_id:
                    document['id'] += str(':' + str(document['class']))
                number = number + 1
                documents.append(document)

        return documents

    def get_documents_DB_HYPERPARTISAN(self,
                                       articles_xml='db_hyperpartisan/articles.xml',
                                       ground_truth_xml='db_hyperpartisan/ground_truth.xml',
                                       append_class_to_id=False):
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
        tree_articles = ET.parse(str(articles_xml),
                                 parser=ET.XMLParser(encoding="utf-8"))
        root_articles = tree_articles.getroot()
        tree_ground_truth = ET.parse(str(ground_truth_xml))
        root_ground_truth = tree_ground_truth.getroot()

        for a_child, g_child in zip(root_articles, root_ground_truth):
            document = {**a_child.attrib, **g_child.attrib,
                        'text': str(self.get_text_from_child(a_child)),
                        'class': str(g_child.get('hyperpartisan')),
                        }
            if append_class_to_id:
                document['id'] += str(':' + str(document['class']))
            documents.append(document)
        return documents

    def get_documents_DB_HYPERPARTISAN_split(self,
                                             articles_xml='db_hyperpartisan/articles.xml',
                                             ground_truth_xml='db_hyperpartisan/ground_truth.xml',
                                             append_class_to_id=False):
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

        df = pd.read_csv('db_hyperpartisan/train_set.csv', dtype=str)

        documents = []
        # Openning the XML files
        tree_articles = ET.parse(str(articles_xml),
                                 parser=ET.XMLParser(encoding="utf-8"))
        root_articles = tree_articles.getroot()
        tree_ground_truth = ET.parse(str(ground_truth_xml))
        root_ground_truth = tree_ground_truth.getroot()

        for a_child, g_child in zip(root_articles, root_ground_truth):
            document = {**a_child.attrib, **g_child.attrib,
                        'text': str(self.get_text_from_child(a_child)),
                        'class': str(g_child.get('hyperpartisan')),
                        }
            if (df['0'].str.contains(document['id']).any()):
                if append_class_to_id:
                    document['id'] += str(':' + str(document['class']))
                documents.append(document)
        return documents

    def calc_IR(self, result_df, positive_class='true'):
        '''
        Calculates IR attributes suggested in the research:
            CLASS_0_BM25_AVG
            CLASS_0_BM25_COUNT
            CLASS_0_BM25_SUM
            CLASS_1_BM25_AVG
            CLASS_1_BM25_COUNT
            CLASS_1_BM25_SUM
        and returns them as a dictionary.

        Parameters
        ----------
        result_df : DataFrame
            A query result dataframe produced by the query methods.
            Must have the columns:
                * score
                * class

        positive_class : str
            Specifies which 'class' is the positive class.
        '''
        df = result_df.copy()
        CLASS_0 = df.loc[(df['class'] != positive_class)]['score']
        CLASS_1 = df.loc[(df['class'] == positive_class)]['score']
        attrib_IR = {
            'CLASS_0_BM25_AVG': (0 if math.isnan(CLASS_0.mean())
                                 else CLASS_0.mean()),
            'CLASS_0_BM25_COUNT': CLASS_0.count(),
            'CLASS_0_BM25_SUM': CLASS_0.sum(),
            'CLASS_1_BM25_AVG': (0 if math.isnan(CLASS_1.mean())
                                 else CLASS_1.mean()),
            'CLASS_1_BM25_COUNT': CLASS_1.count(),
            'CLASS_1_BM25_SUM': CLASS_1.sum(),
        }
        return attrib_IR

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
        self.arango_sys_db = self.arangoClient.db(
            '_system', username=None, password=None)

        index_name = self.indexName
        # Create a new database named "test" if it does not exist.
        if not self.arango_sys_db .has_database(index_name):
            self.arango_sys_db .create_database(index_name)

        # Connect to "test" database as root user.
        # This returns an API wrapper for "test" database.
        self.arangoDb = self.arangoClient.db(
            index_name, username=None, password=None)

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
                    'writebufferSizeMax': 0,
                    'links': {
                        index_name: {
                            "analyzers": [
                                "text_en"
                            ],
                            "includeAllFields": True,
                            "storeValues": 'id'
                        }
                    }
                }
            )

        # Configure AQL query cache properties
        db.aql.cache.configure(mode='off', max_results=100000)

    def arango_delete(self, databases):
        '''
        Deletes the databases from ArangoDB.

        Parameters
        ----------
        databases : list
            String list of the database names.
        '''

        for db in databases:
            # Delete database named 'db' if it does exist.
            if self.arango_sys_db.has_database(str(db)):
                self.arango_sys_db.delete_database(str(db))

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

        document = {'_key': itemKey}
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
                [{'_key': 'document1', 'field1': 'value1', 'field2': 'value2'},
                {'_key': 'document2', 'field1': 'value4', 'field2': 'value5'}]
        '''

        self.arangoCollection.import_bulk(documentList)

    def arango_query(self, query, ignore_first_result=False):
        '''
        Query ArangoDB view and returns a Pandas DataFrame with the results.

        Parameters
        ----------
        query : str
            Text to be queried to the view using BM25 analyzer.
        '''
        initial = time.time()
        escaped_query = str(query).replace('\\', '')
        escaped_query = str(escaped_query).replace("'", "\\\'")

        nResults = int(self.numberResults)
        if ignore_first_result:
            nResults += 1
        aqlquery = (f"FOR d IN {str(self.arangoViewName)} SEARCH "
                    + f"ANALYZER(d.text IN TOKENS('{escaped_query}'"
                    + f", 'text_en'), 'text_en') "
                    + f"SORT BM25(d, {self.bm25_k1}, {self.bm25_b}) "
                    + f"DESC LIMIT {nResults} "
                    + f"LET sco = BM25(d, {self.bm25_k1}, "
                    + f"{self.bm25_b}) RETURN {{ doc: d, score: sco }}")
        # print(aqlquery)
        cursor = self.arangoDb.aql.execute(query=aqlquery,
                                           count=True,
                                           batch_size=self.numberResults,
                                           optimizer_rules=['+all'],
                                           cache=True)
        item_list = []
        # print(1, time.time()-initial)
        initial = time.time()
        for item in cursor.batch():
            # print(item)
            item_list.append([item['score'], item['doc']['_id'].split('/')[-1],
                              item['doc']['class']])
        # print(2, time.time()-initial)
        if ignore_first_result and (len(item_list) > 0):
            item_list.pop(0)
        return pd.DataFrame(item_list, columns=['score', 'id', 'class'])

    def arango_get_document(self, key):
        '''
        Get a document from ArangoDB database, returns the document.

        Parameters
        ----------
        key : str
            Document key.
        '''
        result = self.arangoCollection.get(str(key))
        return result

    def arango_get_IR_variables(self, query, positive_class='true', ignore_first_result=False):
        '''
         Query ArangoDB view and returns a dict with the IR variables.

        Parameters
        ----------
        query : str
            Text to be queried to the view using BM25 analyzer.
        '''
        result_df = self.arango_query(query, ignore_first_result=ignore_first_result)

        return self.calc_IR(result_df=result_df, positive_class=positive_class)

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
        body = {
            "settings": {
                "number_of_shards": 1,
                "index": {
                    "similarity": {
                        "default": {
                            "type": "BM25",
                            "b": self.bm25_b,
                            "k1": self.bm25_k1
                        }
                    }
                }
            }
        }
        if not self.elasticClient.indices.exists(index=self.indexName):
            self.elasticClient.indices.create(index=self.indexName, body=body)

    def elastic_delete(self, indices):
        '''
        Deletes complete indices from Elasticsearch.

        Parameters
        ----------
        indices : list
            String list of the indices names.
        '''

        for index in indices:
            # Delete indice named 'index' if it does exist.
            if self.elasticClient.indices.exists(index=str(index)):
                self.elasticClient.indices.delete(index=str(index))

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

        self.elasticClient.index(
            index=self.indexName, doc_type=self.elasticDocumentType,
            id=itemKey, body=itemBody)

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
                {'index':
                    {
                        "_index": self.indexName,
                        "_id": item.pop('id')
                    }
                 },
                item
            ]
            bulkBody.extend(action)

        return bulkBody

    def bulkHelperInsertGeneratorElastic(self, bulkItems):
        '''
        Generates a bulk body of insert Elasticsearch operations.

        Parameters
        ----------
        bulkItems : list
            Bulk items to be processed, must contain an 'id' field.
        '''

        # item['_id'] = item.pop('id')
        for item in bulkItems:
            item['_index'] = self.indexName
            item['_id'] = item.pop('id')
            item['_type'] = self.elasticDocumentType

        return bulkItems

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

    def bulkHelperElastic(self, bulkHelperActions):
        '''
        Bulk Helper Elasticsearch operations.

        Parameters
        ----------
        bulkBody : list or str with operations separated by newlines ('\n')
            Bulk operations to be executed, already in the format and order to be executed.
            All operations must have an '_id' in their metadata field.
            e.g. of index operation over 'index_name' index:
                [{ 'index': {'_index': 'index_name', '_id' : 'document_id'}, {'field1' : 'value1'}]
        '''

        # print(len(bulkHelperActions))
        # print(bulkHelperActions[0])
        r = ElasticsearchHelpers.bulk(
            client=self.elasticClient, actions=bulkHelperActions,
            index=self.indexName,  # thread_count=6,
            chunk_size=500, max_chunk_bytes=1000*1024*1024)
        # print(r)

    def refreshElastic(self):
        '''
        Refresh Elasticsearch indices.

        Parameters
        ----------
        none : none
        '''
        self.elasticClient.indices.refresh(index=self.indexName)

    def elastic_query(self, query, ignore_first_result=False):
        '''
        Query Elasticsearch index, returns a Pandas DataFrame with the results.

        Parameters
        ----------
        query : str
            Text to be queried to the index using BM25 similarity
            implemented by Elasticsearch.
        '''
        # escaped_query = str(query).replace('\\', '')
        # escaped_query = str(query).replace('"', '\\\"')
        # escaped_query = json.JSONEncoder.encode(query)
        escaped_query = str(query).replace("'", " ")
        # escaped_query = str(query).replace("\\", "\'")
        # escaped_query = query
        # print('text\n\n\n\n\nHERE')
        # print(escaped_query)        
        nResults = int(self.numberResults)
        if ignore_first_result:
            nResults += 1
        result = self.elasticClient.search(index=self.indexName,
                                           body={
                                               "query": {"match": {
                                                   "text": escaped_query}
                                               }
                                           },
                                           size=nResults)
        hit_list = []
        for hit in result['hits']['hits']:
            hit_list.append(
                [hit['_score'], hit['_id'], hit['_source']['class']])
        if ignore_first_result and (len(hit_list) > 0):
            hit_list.pop(0)
        return pd.DataFrame(hit_list, columns=['score', 'id', 'class'])

    def elastic_get_document(self, id):
        '''
        Get a document from a Elasticsearch index, returns the document.

        Parameters
        ----------
        id : str
            Document id.
        '''
        result = self.elasticClient.get_source(index=self.indexName,
                                               id=str(id))
        return result

    def elastic_get_IR_variables(self, query, positive_class='true', ignore_first_result=False):
        '''
        Query Elasticsearch index, returns a dict with the IR variables.

        Parameters
        ----------
        query : str
            Text to be queried to the index using BM25 similarity
            implemented by Elasticsearch.
        '''
        result_df = self.elastic_query(query, ignore_first_result=ignore_first_result)

        return self.calc_IR(result_df=result_df, positive_class=positive_class)

    def initializeZettair(self):
        print('')

    def saveToTrecFileZettair(self, bulkItems):
        filename = str(self.indexName) + '.txt'
        f = open(filename, "w+")

        for d in bulkItems:
            f.write(f'<DOC>\n<DOCNO>{d["id"]}</DOCNO>\n{d["text"]}\n</DOC>\n')
        f.close()

    def zettair_index(self):
        trecfile = str(self.indexName) + '.txt'
        cmd = f'zet -i -f {self.indexName} -t TREC --big-and-fast {trecfile}'
        res = subprocess.run(cmd, shell=True, universal_newlines=True,
                             check=True, capture_output=True)
        # p = subprocess.Popen(['zet', '--index',  '--filename',
        #                   self.indexName, '-t', 'TREC',
        #                   '--big-and-fast', str(trecfile)],
        #                  stdin=subprocess.PIPE,
        #                  stdout=subprocess.PIPE,
        #                  stderr=subprocess.PIPE)
        # p.terminate()
        print(res)

    def zettair_query(self, query, interactive=True, ignore_first_result=False):
        '''
        Query Zettair index, returns a Pandas DataFrame with the results.

        Parameters
        ----------
        query : str
            Text to be queried to the index using BM25 metric.
        '''
        escaped_query = str(query).replace('\\', '')
        escaped_query = str(escaped_query).replace('"', ' ')
        escaped_query = str(escaped_query).replace('`', '\\`')
        nResults = int(self.numberResults)
        if ignore_first_result:
            nResults += 1
        if (self.zettair_query_process is None):
            self.zettair_query_process = subprocess.Popen(['zet',  '-f',
                                                           self.root_path + self.indexName,
                                                           '-n', str(nResults),
                                                           '--okapi', f'--b={self.bm25_b}',
                                                           f'--k1={self.bm25_k1}',
                                                           f'--k3={self.bm25_k3}',
                                                           '--summary=none',
                                                           '--big-and-fast'],
                                                          stdin=subprocess.PIPE,
                                                          stdout=subprocess.PIPE,
                                                          stderr=subprocess.PIPE)
        # print(escaped_query)
        # p.terminate()
        out = ''
        lines = []
        if interactive:
            escaped_query = str(escaped_query).replace("'", " ")
            escaped_query = "'" + escaped_query + "'"
            # out, err = self.zettair_query_process.communicate(
            #     escaped_query.encode('utf-8'))
            # out = out.decode('utf-8')
            # print(out.decode('utf-8'))
            escaped_query = str(escaped_query).replace('\n', ' ')
            # print(escaped_query)
            self.zettair_query_process.stdin.write(
                escaped_query.encode('utf-8') + b'\n')
            # self.zettair_query_process.stdin.write(escaped_query+'\n')
            self.zettair_query_process.stdin.flush()
            # print(escaped_query)
            fl = self.zettair_query_process.stdout.readline()
            while len(fl.decode('utf-8').split()) > 7:
                # print(fl)
                fl = self.zettair_query_process.stdout.readline()
            # print(fl.decode('utf-8').split('>'))
            lines.append(fl.decode('utf-8').split('>')[1])
            while fl != b'\n' and fl != b'> \n':
                fl = self.zettair_query_process.stdout.readline()
                self.zettair_query_process.stdout.flush()
                if not self.zettair_query_process.poll() is None:
                    print('POOL\n', self.zettair_query_process.poll())
                    err = self.zettair_query_process.stderr.readline()
                    if err != "":
                        print('ERROR\n', err)
                lines.append(fl.decode('utf-8'))
            # for line in iter(self.zettair_query_process.stdout.readline, b'\n'):
            #     lines.append(line.decode('utf-8'))
            #     self.zettair_query_process.stdout.flush()
            #     # print(line)
            # print('END')
        else:
            escaped_query = '"' + escaped_query + '"'
            cmd = f'zet -f {self.root_path}{self.indexName} -n {str(nResults)} --okapi ' + \
                f'--b={self.bm25_b} --k1={self.bm25_k1} --k3={self.bm25_k3} ' + \
                f'--summary=none --big-and-fast {escaped_query}'
            res = subprocess.run(cmd, shell=True, universal_newlines=True,
                                 check=True,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out = res.stdout

            # Process Zettair query result
            # linesx = out.split('>')[1].splitlines()
            linesx = out.splitlines()
            # linesx = (line for line in linesx if line)    # Non-blank lines
            for line in linesx:
                if line:
                    lines.append(line)
                else:  # breaks after first blank line, next line is the summary
                    break
        # Iterates over the lines, extracts the id and score
        res_list = []
        len_lines = len(lines)
        # if not (len_lines <= 2 and lines[0] == ' '):
        if (len_lines > 2 and (not lines[0] == ' ')):
            for line in lines:
                line_split = line.split()
                if (len(line_split) >= 4):
                    stuff = line_split[1].split(':')
                    cur_id = stuff[0]
                    # print(f'stuff: {stuff}')
                    if (len(stuff) > 1):
                        cl = stuff[1]
                    else:
                        cl = self.elastic_get_document(str(cur_id))['class']
                    score = line_split[3].split(',')[0]
                    # cl = 'true'
                    res_list.append(
                        [float(score), cur_id, cl])
        if ignore_first_result and (len(res_list) > 0):
            res_list.pop(0)
        return pd.DataFrame(res_list, columns=['score', 'id', 'class'])

    def zettair_get_IR_variables(self, query, positive_class='true', interactive=True, ignore_first_result=False):
        '''
        Query Zettair index, returns a dict with the IR variables.

        Parameters
        ----------
        query : str
            Text to be queried to the index using BM25 metric.
        '''
        result_df = self.zettair_query(query, interactive, ignore_first_result=ignore_first_result)

        return self.calc_IR(result_df=result_df, positive_class=positive_class)

    def zettair_delete(self, index_name):
        '''
        Deletes the databases from ArangoDB.

        Parameters
        ----------
        index_name : str
            Name used for the the Zettair index files to be deleted.
        '''

        # for db in databases:
        #     # Delete database named 'db' if it does exist.
        #     if self.arango_sys_db.has_database(str(db)):
        #         self.arango_sys_db.delete_database(str(db))

# bulkBody = testTool.bulkInsertGeneratorElastic([{'id':'23232', 'text': 'hueheuheu'}, {'id':'12345678', 'text': 'hmmmmmm'}])

# print(bulkBody)

# pp = pprint.PrettyPrinter(indent=4)
# # print(items[0].childNodes)

# t_index = timeit.timeit(index_DB_HYPERPARTISAN_TOOL_ARANGO, setup="gc.enable()", number=1)

# # pp.pprint(testTool.get_documents_DB_HYPERPARTISAN(articles_xml, ground_truth_xml))
# print(testTool.get_documents_DB_BOTGENDER('db_botgender/en/','truth.txt'))
