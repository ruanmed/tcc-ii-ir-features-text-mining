# import os
# import time
# import timeit
# import pprint
import math
import subprocess
# from datetime import datetime
from arango import ArangoClient
from elasticsearch import Elasticsearch

import xml.etree.ElementTree as ET

import pandas as pd


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

    def __init__(self, indexName='default_index',
                 bm25_b=0.75, bm25_k1=1.2, bm25_k3=0.0):
        self.indexName = indexName
        self.bm25_b = float(bm25_b)
        self.bm25_k1 = float(bm25_k1)
        self.bm25_k3 = float(bm25_k3)
        self.numberResults = 1000

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
        if tag.text is not None:
            text = str(text + tag.text)
        count = 0
        for child in tag:
            count = count + 1
            text = str(text + self.get_text_from_child(child))
        return text

    def get_documents_DB_AUTHORPROF(self,
                                    documents_xml_folder='db_authorprof/en/',
                                    truth_txt='db_authorprof/truth.txt'):
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
                document = {'id': str(author_id + '-' + str(number)),
                            'gender': str(gender), 'class': str(gender),
                            'text': child.text
                            }
                number = number + 1
                documents.append(document)

        return documents

    def get_documents_DB_BOTGENDER(self,
                                   documents_xml_folder='db_botgender/en/',
                                   truth_txt='db_authorprof/truth.txt'):
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
                document = {'id': str(author_id + '-' + str(number)),
                            'kind': str(kind), 'gender': str(gender),
                            'text': child.text, 'class': str(kind),
                            }
                number = number + 1
                documents.append(document)

        return documents

    def get_documents_DB_HYPERPARTISAN(self,
                                       articles_xml='db_hyperpartisan/articles.xml',
                                       ground_truth_xml='db_hyperpartisan/ground_truth.xml'):
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
            document = {**a_child.attrib, **g_child.attrib,
                        'text': str(self.get_text_from_child(a_child)),
                        'class': str(g_child.get('hyperpartisan')),
                        }
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
        sys_db = self.arangoClient.db('_system', username=None, password=None)

        index_name = self.indexName
        # Create a new database named "test" if it does not exist.
        if not sys_db.has_database(index_name):
            sys_db.create_database(index_name)

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
                    'links': {
                        index_name: {
                            "analyzers": [
                                "text_en"
                            ],
                            "includeAllFields": True
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

    def queryArango(self, query):
        '''
        Query ArangoDB view and returns a Pandas DataFrame with the results.

        Parameters
        ----------
        query : str
            Text to be queried to the view using BM25 analyzer.
        '''
        aqlquery = (f"FOR d IN {str(self.arangoViewName)} SEARCH "
                    + f"ANALYZER(d.text IN TOKENS('{str(query)}'"
                    + f", 'text_en'), 'text_en') "
                    + f"SORT BM25(d, {self.bm25_k1}, {self.bm25_b}) "
                    + f"DESC LET sco = BM25(d, {self.bm25_k1}, "
                    + f"{self.bm25_b}) RETURN {{ doc: d, score: sco }}")
        cursor = self.arangoDb.aql.execute(aqlquery, count=True)
        item_list = []
        for item in cursor.batch():
            # print(item)
            item_list.append([item['score'], item['doc']['_id'].split('/')[-1],
                              item['doc']['class']])
        return pd.DataFrame(item_list, columns=['score', 'id', 'class'])

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
            index=self.indexName, doc_type=self.elasticDocumentType, id=itemKey, body=itemBody)

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

    def refreshElastic(self):
        '''
        Refresh Elasticsearch indices.

        Parameters
        ----------
        none : none
        '''
        self.elasticClient.indices.refresh(index=self.indexName)

    def queryElastic(self, query):
        '''
        Query Elasticsearch index, returns a Pandas DataFrame with the results.

        Parameters
        ----------
        query : str
            Text to be queried to the index using BM25 similarity
            implemented by Elasticsearch.
        '''
        result = self.elasticClient.search(index=self.indexName, body={
            "query": {"match": {"text": str(query)}}})
        hit_list = []
        for hit in result['hits']['hits']:
            hit_list.append(
                [hit['_score'], hit['_id'], hit['_source']['class']])
        return pd.DataFrame(hit_list, columns=['score', 'id', 'class'])

    def initializeZettair(self):
        print('')

    def saveToTrecFileZettair(self, bulkItems):
        filename = str(self.indexName) + '.txt'
        f = open(filename, "w+")

        for d in bulkItems:
            f.write(f'<DOC>\n<DOCNO>{d["id"]}</DOCNO>\n{d["text"]}\n</DOC>\n')
        f.close()

    def indexZettair(self):
        trecfile = str(self.indexName) + '.txt'
        cmd = f'zet -i -f {self.indexName} -t TREC --big-and-fast {trecfile}'
        res = subprocess.run(cmd, shell=False, universal_newlines=True,
                             check=True, capture_output=True)
        # p = subprocess.Popen(['zet', '--index',  '--filename',
        #                   self.indexName, '-t', 'TREC',
        #                   '--big-and-fast', str(trecfile)],
        #                  stdin=subprocess.PIPE,
        #                  stdout=subprocess.PIPE,
        #                  stderr=subprocess.PIPE)
        # p.terminate()
        print(res)

    def queryZettair(self, query):
        '''
        Query Zettair index, returns a Pandas DataFrame with the results.

        Parameters
        ----------
        query : str
            Text to be queried to the index using BM25 metric.
        '''
        p = subprocess.Popen(['zet',  '-f', self.indexName,
                              '-n', str(int(self.numberResults)),
                              '--okapi', f'--b={self.bm25_b}',
                              f'--k1={self.bm25_k1}',
                              f'--k3={self.bm25_k3}',
                              '--summary=none', '--big-and-fast'],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate(query.encode('utf-8'))
        p.terminate()
        # print(out.decode('utf-8'))
        # print(err)

        lines = []
        # Process Zettair query result
        linesx = out.decode("utf-8").split('>')[1].splitlines()
        # linesx = (line for line in linesx if line)    # Non-blank lines
        for line in linesx:
            if line:
                lines.append(line)
            else:  # breaks after first blank line, next line is the summary
                break
        # Iterates over the lines, extracts the id and score
        res_list = []
        for line in lines:
            cur_id = line.split()[1]
            score = line.split()[3].split(',')[0]
            cl = 'true'
            res_list.append(
                [score, cur_id, cl])
        return pd.DataFrame(res_list, columns=['score', 'id', 'class'])

# bulkBody = testTool.bulkInsertGeneratorElastic([{'id':'23232', 'text': 'hueheuheu'}, {'id':'12345678', 'text': 'hmmmmmm'}])

# print(bulkBody)

# pp = pprint.PrettyPrinter(indent=4)
# # print(items[0].childNodes)

# t_index = timeit.timeit(index_DB_HYPERPARTISAN_TOOL_ARANGO, setup="gc.enable()", number=1)

# # pp.pprint(testTool.get_documents_DB_HYPERPARTISAN(articles_xml, ground_truth_xml))
# print(testTool.get_documents_DB_BOTGENDER('db_botgender/en/','truth.txt'))
