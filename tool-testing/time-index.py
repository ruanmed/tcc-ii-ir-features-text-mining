# from keras.preprocessing import sequence
import numpy as np
import ast
from indextoolmanager import IndexToolManager
import time
import datetime
# import timeit
import pprint
import pandas as pd

db = {
    'authorprof_en': {
        'xml_folder': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/text/',
        'truth_txt': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/en.txt'
    },
    'botgender_en': {
        'xml_folder': 'db_authorprof/pan19-author-profiling-training-2019-02-18/en/text/',
        'truth_txt': 'db_authorprof/pan19-author-profiling-training-2019-02-18/en/truth.txt'
    },
    'hyperpartisan': {
        'xml_folder': 'db_authorprof/articles-training-byarticle-20181122.xml',
        'truth_txt': 'db_authorprof/ground-truth-training-byarticle-20181122.xml'
    },
    'authorprof': {
        'xml_folder': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/text/',
        'truth_txt': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/truth.txt'
    },
    'botgender': {
        'xml_folder': 'db_authorprof/pan19-author-profiling-training-2019-02-18/en/text/',
        'truth_txt': 'db_authorprof/pan19-author-profiling-training-2019-02-18/en/truth.txt'
    },
}
authorprof_db_name = 'authorprof'
botgender_db_name = 'botgender'
hyperpartisan_db_name = 'hyperpartisan'

authorprof_xml_folder = 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/text/'
authorprof_truth_txt = 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/en.txt'
botgender_xml_folder = 'db_authorprof/pan19-author-profiling-training-2019-02-18/en/text/'
botgender_truth_txt = 'db_authorprof/pan19-author-profiling-training-2019-02-18/en/truth.txt'
hyperpartisan_articles_xml = 'db_authorprof/articles-training-byarticle-20181122.xml'
hyperpartisan_ground_truth_xml = 'db_authorprof/ground-truth-training-byarticle-20181122.xml'


# authorprof_xml_folder = 'db_authorprof/en/'
# authorprof_truth_txt = 'db_authorprof/truth.txt'
# botgender_xml_folder = 'db_botgender/en/'
# botgender_truth_txt = 'db_botgender/truth.txt'
# hyperpartisan_articles_xml = 'db_hyperpartisan/articles.xml'
# hyperpartisan_ground_truth_xml = 'db_hyperpartisan/ground_truth.xml'

# DB_AUTHORPROF

def index_DB_AUTHORPROF_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=authorprof_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_AUTHORPROF ', end - start)

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    print('TIME bulkListGeneratorArango ', end - start)

    start = time.time()
    for doc in documentList:
        testTool.insertDocumentArango(doc)
    end = time.time()
    print('TIME for-loop insertDocumentArango ', end - start)

    final = time.time()

    print('TIME_INDEX index_DB_AUTHORPROF_TOOL_ARANGO: ', final - initial)


def index_bulk_DB_AUTHORPROF_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(authorprof_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_AUTHORPROF ', end - start)

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    print('TIME bulkListGeneratorArango ', end - start)

    start = time.time()
    testTool.bulkImportArango(documentList)
    end = time.time()
    print('TIME bulkImportArango ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_AUTHORPROF_TOOL_ARANGO', final - initial)


def index_DB_AUTHORPROF_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=authorprof_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_AUTHORPROF ', end - start)

    start = time.time()
    for doc in bulk:
        testTool.insertElastic(doc.pop('id'), doc)
    end = time.time()
    print('TIME for-loop insertElastic ', end - start)

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    print('TIME refreshElastic ', end - start)

    final = time.time()

    print('TIME_INDEX index_DB_AUTHORPROF_TOOL_ELASTIC: ', final - initial)


def index_bulk_DB_AUTHORPROF_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(authorprof_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_AUTHORPROF ', end - start)

    start = time.time()
    bulkBody = testTool.bulkInsertGeneratorElastic(bulk)
    end = time.time()
    print('TIME bulkInsertGeneratorElastic ', end - start)

    start = time.time()
    testTool.bulkElastic(bulkBody)
    end = time.time()
    print('TIME bulkElastic ', end - start)

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    print('TIME refreshElastic ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_AUTHORPROF_TOOL_ELASTIC', final - initial)


def index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(authorprof_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_AUTHORPROF ', end - start)

    start = time.time()
    testTool.saveToTrecFileZettair(bulk)
    end = time.time()
    print('TIME saveToTrecFileZettair ', end - start)

    start = time.time()
    testTool.indexZettair()
    end = time.time()
    print('TIME indexZettair ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR: ', final - initial)


# DB_BOTGENDER

def index_DB_BOTGENDER_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=botgender_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_BOTGENDER ', end - start)

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    print('TIME bulkListGeneratorArango ', end - start)

    start = time.time()
    for doc in documentList:
        testTool.insertDocumentArango(doc)
    end = time.time()
    print('TIME for-loop insertDocumentArango ', end - start)

    final = time.time()

    print('TIME_INDEX index_DB_BOTGENDER_TOOL_ARANGO', final - initial)


def index_bulk_DB_BOTGENDER_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(botgender_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_BOTGENDER ', end - start)

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    print('TIME bulkListGeneratorArango ', end - start)

    start = time.time()
    testTool.bulkImportArango(documentList)
    end = time.time()
    print('TIME bulkImportArango ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_BOTGENDER_TOOL_ARANGO: ', final - initial)


def index_DB_BOTGENDER_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=botgender_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_BOTGENDER ', end - start)

    start = time.time()
    for doc in bulk:
        testTool.insertElastic(doc.pop('id'), doc)
    end = time.time()
    print('TIME for-loop insertElastic ', end - start)

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    print('TIME refreshElastic ', end - start)

    final = time.time()

    print('TIME_INDEX index_DB_BOTGENDER_TOOL_ELASTIC', final - initial)


def index_bulk_DB_BOTGENDER_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(botgender_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_BOTGENDER ', end - start)

    start = time.time()
    bulkBody = testTool.bulkInsertGeneratorElastic(bulk)
    end = time.time()
    print('TIME bulkInsertGeneratorElastic ', end - start)

    start = time.time()
    testTool.bulkElastic(bulkBody)
    end = time.time()
    print('TIME bulkElastic ', end - start)

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    print('TIME refreshElastic ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_BOTGENDER_TOOL_ELASTIC: ', final - initial)


def index_bulk_DB_BOTGENDER_TOOL_ZETTAIR():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(botgender_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    print('TIME get_documents_DB_BOTGENDER ', end - start)

    start = time.time()
    testTool.saveToTrecFileZettair(bulk)
    end = time.time()
    print('TIME saveToTrecFileZettair ', end - start)

    start = time.time()
    testTool.indexZettair()
    end = time.time()
    print('TIME indexZettair ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_BOTGENDER_TOOL_ZETTAIR', final - initial)


# DB_HYPERPARTISAN

def index_DB_HYPERPARTISAN_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=hyperpartisan_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    print('TIME get_documents_DB_HYPERPARTISAN ', end - start)

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    print('TIME bulkListGeneratorArango ', end - start)

    start = time.time()
    for doc in documentList:
        testTool.insertDocumentArango(doc)
    end = time.time()
    print('TIME for-loop insertDocumentArango ', end - start)

    final = time.time()

    print('TIME_INDEX index_DB_HYPERPARTISAN_TOOL_ARANGO: ', final - initial)


def index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    print('TIME get_documents_DB_HYPERPARTISAN ', end - start)

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    print('TIME bulkListGeneratorArango ', end - start)

    start = time.time()
    testTool.bulkImportArango(documentList)
    end = time.time()
    print('TIME bulkImportArango ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO', final - initial)


def index_DB_HYPERPARTISAN_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=hyperpartisan_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    print('TIME get_documents_DB_HYPERPARTISAN ', end - start)

    start = time.time()
    for doc in bulk:
        testTool.insertElastic(doc.pop('id'), doc)
    end = time.time()
    print('TIME for-loop insertElastic ', end - start)

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    print('TIME refreshElastic ', end - start)

    final = time.time()

    print('TIME_INDEX index_DB_HYPERPARTISAN_TOOL_ELASTIC: ', final - initial)


def index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    print('TIME get_documents_DB_HYPERPARTISAN ', end - start)

    start = time.time()
    bulkBody = testTool.bulkInsertGeneratorElastic(bulk)
    end = time.time()
    print('TIME bulkInsertGeneratorElastic ', end - start)

    start = time.time()
    testTool.bulkElastic(bulkBody)
    end = time.time()
    print('TIME bulkElastic ', end - start)

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    print('TIME refreshElastic ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC: ', final - initial)


def index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    print('TIME get_documents_DB_HYPERPARTISAN ', end - start)

    start = time.time()
    testTool.saveToTrecFileZettair(bulk)
    end = time.time()
    print('TIME saveToTrecFileZettair ', end - start)

    start = time.time()
    testTool.indexZettair()
    end = time.time()
    print('TIME indexZettair ', end - start)

    final = time.time()

    print('TIME_INDEX index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR: ', final - initial)

print('TIME_INDEX - Starting')
print(datetime.datetime.now())

initial = time.time()

print('CLEANING DATABASES')
testTool = IndexToolManager(indexName=authorprof_db_name)
testTool.clean_default()
final = time.time()
print('CLEANING FINISHED: ', final - initial)

print('')
index_DB_AUTHORPROF_TOOL_ARANGO()
print('')
index_bulk_DB_AUTHORPROF_TOOL_ARANGO()
print('')
index_DB_AUTHORPROF_TOOL_ELASTIC()
print('')
index_bulk_DB_AUTHORPROF_TOOL_ELASTIC()
print('')
index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR()
print('')

index_DB_BOTGENDER_TOOL_ARANGO()
print('')
index_bulk_DB_BOTGENDER_TOOL_ARANGO()
print('')
index_DB_BOTGENDER_TOOL_ELASTIC()
print('')
index_bulk_DB_BOTGENDER_TOOL_ELASTIC()
print('')
index_bulk_DB_BOTGENDER_TOOL_ZETTAIR()
print('')

index_DB_HYPERPARTISAN_TOOL_ARANGO()
print('')
index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO()
print('')
index_DB_HYPERPARTISAN_TOOL_ELASTIC()
print('')
index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC()
print('')
index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR()
print('')

print(datetime.datetime.now())
print('TIME_INDEX - Finished')

# index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO()

# index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC()

# index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR()


# index_DB_HYPERPARTISAN_TOOL_ARANGO()

# index_DB_HYPERPARTISAN_TOOL_ELASTIC()

# pp = pprint.PrettyPrinter(indent=4)

# testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

# query = 'the'
# # print(testTool.queryArango(query))
# # result_df = testTool.queryElastic(query)
# # pp.pprint(testTool.calc_IR(result_df, 'true'))
# # print(testTool.queryElastic(query))
# # print(testTool.queryZettair(query))

# X = []
# with open('train.elmo.mini.tsv', 'rb') as inf:
#     for line in inf:
#         gzip_fields = line.decode('utf-8').split('\t')
#         gzip_id = gzip_fields[0]
#         gzip_label = gzip_fields[1]
#         elmo_embd_str = gzip_fields[4].strip()
#         elmo_embd_list = ast.literal_eval(elmo_embd_str)
#         elmo_embd_array = np.array(elmo_embd_list)
#         pp.pprint(elmo_embd_array.shape)
#         pp.pprint(type(elmo_embd_array))
#         padded_seq = sequence.pad_sequences(
#             [elmo_embd_array], maxlen=200, dtype='float32')[0]
#         # pp.pprint(padded_seq.shape)
#         X.append(padded_seq)

# pp.pprint(X[0].shape)
