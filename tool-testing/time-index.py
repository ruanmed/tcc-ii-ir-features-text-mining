from keras.preprocessing import sequence
import numpy as np
import ast
from indextoolmanager import IndexToolManager
import time
import datetime
# import timeit
import pprint
import pandas as pd
import logging

variable_name = 'TIME_INDEX'
datestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# set up logging to file - see previous section for more details
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=f'logs/{datestr}-{variable_name}_debug.log',
                    filemode='w')
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger(variable_name).addHandler(console)

# Now, we can log to the root logger, or any other logger. First the root...
logging.info('Jackdaws love my big sphinx of quartz.')

# Now, define a couple of other loggers which might represent areas in your
# application:

mylogger = logging.getLogger(variable_name)
handler1 = logging.FileHandler(f'logs/{datestr}-{variable_name}.log')
handler1.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler1.setFormatter(formatter)
mylogger.addHandler(handler1)

db_files = {
    'authorprof_en': {
        'xml_folder': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/text/',
        'truth_txt': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/en.txt'
    },
    'botgender_en': {
        'xml_folder': 'db_botgender/pan19-author-profiling-training-2019-02-18/en/text/',
        'truth_txt': 'db_botgender/pan19-author-profiling-training-2019-02-18/en/truth.txt'
    },
    'hyperpartisan': {
        'xml_folder': 'db_hyperpartisan/articles-training-byarticle-20181122.xml',
        'truth_txt': 'db_hyperpartisan/ground-truth-training-byarticle-20181122.xml'
    },
    'authorprof': {
        'xml_folder': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/text/',
        'truth_txt': 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/en.txt'
    },
    'botgender': {
        'xml_folder': 'db_botgender/pan19-author-profiling-training-2019-02-18/en/text/',
        'truth_txt': 'db_botgender/pan19-author-profiling-training-2019-02-18/en/truth.txt'
    },
    'hyperpartisan_split_42': {
        'xml_folder': 'db_hyperpartisan/articles-training-byarticle-20181122.xml',
        'truth_txt': 'db_hyperpartisan/ground-truth-training-byarticle-20181122.xml'
    },
}
dbs = ['authorprof', 'botgender', 'hyperpartisan', 'hyperpartisan_split_42']
# dbs = []
authorprof_db_name = 'authorprof'
botgender_db_name = 'botgender'
hyperpartisan_db_name = 'hyperpartisan'

authorprof_xml_folder = 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/text/'
authorprof_truth_txt = 'db_authorprof/pan18-author-profiling-training-2018-02-27/en/en.txt'
botgender_xml_folder = 'db_botgender/pan19-author-profiling-training-2019-02-18/en/text/'
botgender_truth_txt = 'db_botgender/pan19-author-profiling-training-2019-02-18/en/truth.txt'
hyperpartisan_articles_xml = 'db_hyperpartisan/articles-training-byarticle-20181122.xml'
hyperpartisan_ground_truth_xml = 'db_hyperpartisan/ground-truth-training-byarticle-20181122.xml'


# authorprof_xml_folder = 'db_authorprof/en/'
# authorprof_truth_txt = 'db_authorprof/truth.txt'
# botgender_xml_folder = 'db_botgender/en/'
# botgender_truth_txt = 'db_botgender/truth.txt'
# hyperpartisan_articles_xml = 'db_hyperpartisan/articles.xml'
# hyperpartisan_ground_truth_xml = 'db_hyperpartisan/ground_truth.xml'

# DB_AUTHORPROF
def index(idx_type='normal', db='authorprof', tool='arango', db_name='authorprof', exp_id='unamed'):
    mylogger.info('')
    mylogger.info(f'INDEX TYPE: {idx_type}')
    mylogger.info(f'DB: {db}')
    mylogger.info(f'TOOL: {tool}')
    mylogger.info(f'DB NAME: {db_name}')
    initial = time.time()

    testTool = IndexToolManager(indexName=db_name)

    start = time.time()
    append_class_to_id = False
    if (tool == 'zettair'):
        append_class_to_id = True
    bulk = testTool.get_documents(db,
                                  db_files[db]['xml_folder'],
                                  db_files[db]['truth_txt'],
                                  append_class_to_id)
    end = time.time()
    mylogger.info(f'get_documents {end - start}')
    mylogger.info(f'TOTAL documents {len(bulk)}')

    start = time.time()
    if (tool == 'arango'):
        documentList = testTool.bulkListGeneratorArango(bulk)
        end = time.time()
        mylogger.info(f'bulkListGeneratorArango {end - start}')
        if (idx_type == 'normal'):
            start = time.time()
            for doc in documentList:
                testTool.insertDocumentArango(doc)
            end = time.time()
            mylogger.info(f'for-loop insertDocumentArango {end - start}')
        if (idx_type == 'bulk'):
            start = time.time()
            testTool.bulkImportArango(documentList)
            end = time.time()
            mylogger.info(f'bulkImportArango {end - start}')

    if (tool == 'elastic'):
        if (idx_type == 'normal'):
            start = time.time()
            for doc in bulk:
                testTool.insertElastic(doc.pop('id'), doc)
            end = time.time()
            mylogger.info(f'for-loop insertElastic {end - start}')

        if (idx_type == 'bulk'):
            start = time.time()
            bulkBody = testTool.bulkHelperInsertGeneratorElastic(bulk)
            end = time.time()
            mylogger.info(f'bulkHelperInsertGeneratorElastic {end - start}')

            start = time.time()
            testTool.bulkHelperElastic(bulkBody)
            end = time.time()
            mylogger.info(f'bulkHelperElastic {end - start}')

        start = time.time()
        testTool.refreshElastic()
        end = time.time()
        mylogger.info(f'refreshElastic {end - start}')

    if (tool == 'zettair'):
        start = time.time()
        testTool.saveToTrecFileZettair(bulk)
        end = time.time()
        mylogger.info(f'saveToTrecFileZettair {end - start}')

        start = time.time()
        testTool.zettair_index()
        end = time.time()
        mylogger.info(f'zettair_index {end - start}')

    final = time.time()
    result_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    testTool.log_result(result_id, {
        'exp_id': exp_id,
        'variable': 'TIME_INDEX',
        'index_type': idx_type,
        'db': db,
        'tool': tool,
        'db_name': db_name,
        'value': str((final - initial)),
    })
    mylogger.info(f'index TOTAL TIME: {final - initial}')


def index_DB_AUTHORPROF_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=authorprof_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_AUTHORPROF {end - start}')
    mylogger.info(f'TOTAL documents DB_AUTHORPROF {len(bulk)}')

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    mylogger.info(f'bulkListGeneratorArango {end - start}')

    start = time.time()
    for doc in documentList:
        testTool.insertDocumentArango(doc)
    end = time.time()
    mylogger.info(f'for-loop insertDocumentArango {end - start}')

    final = time.time()

    mylogger.info(f'index_DB_AUTHORPROF_TOOL_ARANGO: {final - initial}')


def index_bulk_DB_AUTHORPROF_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(authorprof_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_AUTHORPROF {end - start}')
    mylogger.info(f'TOTAL documents DB_AUTHORPROF {len(bulk)}')

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    mylogger.info(f'bulkListGeneratorArango {end - start}')

    start = time.time()
    testTool.bulkImportArango(documentList)
    end = time.time()
    mylogger.info(f'bulkImportArango {end - start}')

    final = time.time()

    mylogger.info(f'index_bulk_DB_AUTHORPROF_TOOL_ARANGO: {final - initial}')


def index_DB_AUTHORPROF_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=authorprof_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_AUTHORPROF {end - start}')
    mylogger.info(f'TOTAL documents DB_AUTHORPROF {len(bulk)}')

    start = time.time()
    for doc in bulk:
        testTool.insertElastic(doc.pop('id'), doc)
    end = time.time()
    mylogger.info(f'for-loop insertElastic {end - start}')

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    mylogger.info(f'refreshElastic {end - start}')

    final = time.time()

    mylogger.info(f'index_DB_AUTHORPROF_TOOL_ELASTIC: {final - initial}')


def index_bulk_DB_AUTHORPROF_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(authorprof_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_AUTHORPROF {end - start}')
    mylogger.info(f'TOTAL documents DB_AUTHORPROF {len(bulk)}')

    start = time.time()
    bulkBody = testTool.bulkInsertGeneratorElastic(bulk)
    end = time.time()
    mylogger.info(f'bulkInsertGeneratorElastic {end - start}')

    start = time.time()
    testTool.bulkElastic(bulkBody)
    end = time.time()
    mylogger.info(f'bulkElastic {end - start}')

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    mylogger.info(f'refreshElastic {end - start}')

    final = time.time()

    mylogger.info(f'index_bulk_DB_AUTHORPROF_TOOL_ELASTIC: {final - initial}')


def index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(authorprof_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_AUTHORPROF(
        authorprof_xml_folder, authorprof_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_AUTHORPROF {end - start}')
    mylogger.info(f'TOTAL documents DB_AUTHORPROF {len(bulk)}')

    start = time.time()
    testTool.saveToTrecFileZettair(bulk)
    end = time.time()
    mylogger.info(f'saveToTrecFileZettair {end - start}')

    start = time.time()
    testTool.zettair_index()
    end = time.time()
    mylogger.info(f'zettair_index {end - start}')

    final = time.time()

    mylogger.info(f'index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR: {final - initial}')


# DB_BOTGENDER

def index_DB_BOTGENDER_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=botgender_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_BOTGENDER {end - start}')
    mylogger.info(f'TOTAL documents DB_BOTGENDER  {len(bulk)}')

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    mylogger.info(f'bulkListGeneratorArango {end - start}')

    start = time.time()
    for doc in documentList:
        testTool.insertDocumentArango(doc)
    end = time.time()
    mylogger.info(f'for-loop insertDocumentArango {end - start}')

    final = time.time()

    mylogger.info(f'index_DB_BOTGENDER_TOOL_ARANGO: {final - initial}')


def index_bulk_DB_BOTGENDER_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(botgender_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_BOTGENDER {end - start}')
    mylogger.info(f'TOTAL documents DB_BOTGENDER  {len(bulk)}')

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    mylogger.info(f'bulkListGeneratorArango {end - start}')

    start = time.time()
    testTool.bulkImportArango(documentList)
    end = time.time()
    mylogger.info(f'bulkImportArango {end - start}')

    final = time.time()

    mylogger.info(f'index_bulk_DB_BOTGENDER_TOOL_ARANGO: {final - initial}')


def index_DB_BOTGENDER_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=botgender_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_BOTGENDER {end - start}')
    mylogger.info(f'TOTAL documents DB_BOTGENDER  {len(bulk)}')

    start = time.time()
    for doc in bulk:
        testTool.insertElastic(doc.pop('id'), doc)
    end = time.time()
    mylogger.info(f'for-loop insertElastic {end - start}')

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    mylogger.info(f'refreshElastic {end - start}')

    final = time.time()

    mylogger.info(f'index_DB_BOTGENDER_TOOL_ELASTIC{final - initial}')


def index_bulk_DB_BOTGENDER_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(botgender_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_BOTGENDER {end - start}')
    mylogger.info(f'TOTAL documents DB_BOTGENDER  {len(bulk)}')

    # start = time.time()
    # bulkBody = testTool.bulkInsertGeneratorElastic(bulk)
    # end = time.time()
    # mylogger.info(f'bulkInsertGeneratorElastic {end - start}')

    start = time.time()
    bulkBody = testTool.bulkHelperInsertGeneratorElastic(bulk)
    end = time.time()
    mylogger.info(f'bulkHelperInsertGeneratorElastic {end - start}')

    # start = time.time()
    # testTool.bulkElastic(bulkBody)
    # end = time.time()
    # mylogger.info(f'bulkElastic {end - start}')

    start = time.time()
    testTool.bulkHelperElastic(bulkBody)
    end = time.time()
    mylogger.info(f'bulkHelperElastic {end - start}')

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    mylogger.info(f'refreshElastic {end - start}')

    final = time.time()

    mylogger.info(f'index_bulk_DB_BOTGENDER_TOOL_ELASTIC: {final - initial}')


def index_bulk_DB_BOTGENDER_TOOL_ZETTAIR():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(botgender_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_BOTGENDER(
        botgender_xml_folder, botgender_truth_txt)
    end = time.time()
    mylogger.info(f'get_documents_DB_BOTGENDER {end - start}')
    mylogger.info(f'TOTAL documents DB_BOTGENDER  {len(bulk)}')

    start = time.time()
    testTool.saveToTrecFileZettair(bulk)
    end = time.time()
    mylogger.info(f'saveToTrecFileZettair {end - start}')

    start = time.time()
    testTool.zettair_index()
    end = time.time()
    mylogger.info(f'zettair_index {end - start}')

    final = time.time()

    mylogger.info(f'index_bulk_DB_BOTGENDER_TOOL_ZETTAIR: {final - initial}')


# DB_HYPERPARTISAN

def index_DB_HYPERPARTISAN_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=hyperpartisan_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    mylogger.info(f'get_documents_DB_HYPERPARTISAN {end - start}')
    mylogger.info(f'TOTAL documents DB_HYPERPARTISAN  {len(bulk)}')

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    mylogger.info(f'bulkListGeneratorArango {end - start}')

    start = time.time()
    for doc in documentList:
        testTool.insertDocumentArango(doc)
    end = time.time()
    mylogger.info(f'for-loop insertDocumentArango {end - start}')

    final = time.time()

    mylogger.info(f'index_DB_HYPERPARTISAN_TOOL_ARANGO: {final - initial}')


def index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    mylogger.info(f'get_documents_DB_HYPERPARTISAN {end - start}')
    mylogger.info(f'TOTAL documents DB_HYPERPARTISAN  {len(bulk)}')

    start = time.time()
    documentList = testTool.bulkListGeneratorArango(bulk)
    end = time.time()
    mylogger.info(f'bulkListGeneratorArango {end - start}')

    start = time.time()
    testTool.bulkImportArango(documentList)
    end = time.time()
    mylogger.info(f'bulkImportArango {end - start}')

    final = time.time()

    mylogger.info(
        f'index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO: {final - initial}')


def index_DB_HYPERPARTISAN_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=hyperpartisan_db_name)

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    mylogger.info(f'get_documents_DB_HYPERPARTISAN {end - start}')
    mylogger.info(f'TOTAL documents DB_HYPERPARTISAN  {len(bulk)}')

    start = time.time()
    for doc in bulk:
        testTool.insertElastic(doc.pop('id'), doc)
    end = time.time()
    mylogger.info(f'for-loop insertElastic {end - start}')

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    mylogger.info(f'refreshElastic {end - start}')

    final = time.time()

    mylogger.info(f'index_DB_HYPERPARTISAN_TOOL_ELASTIC: {final - initial}')


def index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    mylogger.info(f'get_documents_DB_HYPERPARTISAN {end - start}')
    mylogger.info(f'TOTAL documents DB_HYPERPARTISAN  {len(bulk)}')

    start = time.time()
    bulkBody = testTool.bulkInsertGeneratorElastic(bulk)
    end = time.time()
    mylogger.info(f'bulkInsertGeneratorElastic {end - start}')

    start = time.time()
    testTool.bulkElastic(bulkBody)
    end = time.time()
    mylogger.info(f'bulkElastic {end - start}')

    start = time.time()
    testTool.refreshElastic()
    end = time.time()
    mylogger.info(f'refreshElastic {end - start}')

    final = time.time()

    mylogger.info(
        f'index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC: {final - initial}')


def index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR():
    initial = time.time()

    testTool = IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        hyperpartisan_articles_xml, hyperpartisan_ground_truth_xml)
    end = time.time()
    mylogger.info(f'get_documents_DB_HYPERPARTISAN {end - start}')
    mylogger.info(f'TOTAL documents DB_HYPERPARTISAN  {len(bulk)}')

    start = time.time()
    testTool.saveToTrecFileZettair(bulk)
    end = time.time()
    mylogger.info(f'saveToTrecFileZettair {end - start}')

    start = time.time()
    testTool.zettair_index()
    end = time.time()
    mylogger.info(f'zettair_index {end - start}')

    final = time.time()

    mylogger.info(
        f'index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR: {final - initial}')


def measure_TIME_INDEX():

    mylogger.info('START OF TIME_INDEX MEASUREMENTS')
    mylogger.info(str(datetime.datetime.now()))

    initial = time.time()

    mylogger.info('CLEANING DATABASES')
    testTool = IndexToolManager(indexName=authorprof_db_name)
    testTool.clean_default()
    final = time.time()
    mylogger.info(f'CLEANING FINISHED: {final - initial}')
    mylogger.info('')
    mylogger.info('DB_AUTHORPROF')
    mylogger.info('')
    index_DB_AUTHORPROF_TOOL_ARANGO()
    mylogger.info('')
    index_bulk_DB_AUTHORPROF_TOOL_ARANGO()
    mylogger.info('')
    index_DB_AUTHORPROF_TOOL_ELASTIC()
    mylogger.info('')
    index_bulk_DB_AUTHORPROF_TOOL_ELASTIC()
    mylogger.info('')
    index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR()
    mylogger.info('')

    mylogger.info('')
    mylogger.info('DB_BOTGENDER')
    mylogger.info('')
    index_DB_BOTGENDER_TOOL_ARANGO()
    mylogger.info('')
    index_bulk_DB_BOTGENDER_TOOL_ARANGO()
    mylogger.info('')
    index_DB_BOTGENDER_TOOL_ELASTIC()
    mylogger.info('')
    index_bulk_DB_BOTGENDER_TOOL_ELASTIC()
    mylogger.info('')
    index_bulk_DB_BOTGENDER_TOOL_ZETTAIR()
    mylogger.info('')

    mylogger.info('')
    mylogger.info('DB_HYPERPARTISAN')
    mylogger.info('')
    index_DB_HYPERPARTISAN_TOOL_ARANGO()
    mylogger.info('')
    index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO()
    mylogger.info('')
    index_DB_HYPERPARTISAN_TOOL_ELASTIC()
    mylogger.info('')
    index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC()
    mylogger.info('')
    index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR()
    mylogger.info('')

    mylogger.info(str(datetime.datetime.now()))
    mylogger.info('END OF TIME_INDEX MEASUREMENTS')


def measure_TIME_INDEX2(normal=False, clean=False):
    mylogger.info('START OF TIME_INDEX MEASUREMENTS')
    exp_id = str(datetime.datetime.now())
    mylogger.info(exp_id)

    initial = time.time()

    if (clean):
        mylogger.info('CLEANING DATABASES')
        testTool = IndexToolManager(indexName=authorprof_db_name)
        testTool.clean_default()
        final = time.time()
        mylogger.info(f'CLEANING FINISHED: {final - initial}')

    # tools = ['arango', 'elastic', 'zettair']
    tools = ['zettair']
    dbs = ['authorprof', 'botgender', 'hyperpartisan', 'hyperpartisan_split_42']
    # dbs = ['authorprof']

    for db in dbs:
        mylogger.info('')
        mylogger.info('DB_' + db)
        for tool in tools:
            if (normal and tool != 'zettair'):
                index(idx_type='normal', db=db, tool=tool,
                      db_name=db, exp_id=exp_id)
            index(idx_type='bulk', db=db, tool=tool,
                  db_name=str(db+'_bulk'), exp_id=exp_id)

    mylogger.info(str(datetime.datetime.now()))
    mylogger.info('END OF TIME_INDEX MEASUREMENTS')


# measure_TIME_INDEX2(True, True)
# measure_TIME_INDEX2(False, False)

# pp=pprint.PrettyPrinter(indent=4)

# testTool=IndexToolManager(indexName=str(hyperpartisan_db_name+'_bulk'))

# query='Is the chaos of 2017 a catharsis -- a necessary and long overdue purge of dangerous and neglected pathologies? Will the bedlam within the United States descend into more nihilism, or offer a remedy to the status quo that had divided and nearly bankrupted the country? Is the problem too much democracy, as the volatile and fickle mob runs roughshod over establishment experts and experienced bureaucrats? Or is the crisis too little democracy, as populists strive to dethrone a scandal-plagued, anti-democratic, incompetent and overrated entrenched elite? Neither traditional political party has any answers. Democrats are being overwhelmed by the identity politics and socialism of progressives. Republicans are torn asunder between upstart populist nationalists and the calcified establishment status quo. Yet for all the social instability and media hysteria, life in the United States quietly seems to be getting better. The economy is growing. Unemployment and inflation remain low. The stock market and middle-class incomes are up. Business and consumer confidence are high. Corporate profits are up. Energy production has expanded. The border with Mexico is being enforced. Is the instability less a symptom that America is falling apart and more a sign that the loud conventional wisdom of the past -- about the benefits of a globalized economy, the insignificance of national borders and the importance of identity politics -- is drawing to a close, along with the careers of those who profited from it? In the past, any crisis that did not destroy the United States ended up making it stronger. But for now, the fight grows over which is more toxic -- the chronic statist malady that was eating away the country, or the new populist medicine deemed necessary to cure it. (C) 2017 TRIBUNE CONTENT AGENCY, LLC. Victor Davis Hanson is a classicist and historian at the Hoover Institution, Stanford University. His latest book is  The Savior Generals'
# print(testTool.arango_query(query))
# result_df = testTool.arango_query(query)
# text = testTool.elastic_get_document('0000000')['text']
# print(testTool.elastic_query(query))
# pp.pprint(testTool.elastic_get_IR_variables(text, 'true'))
# text = testTool.arango_get_document('0000001')['text']
# pp.pprint(testTool.arango_get_IR_variables(text, 'true'))
# print(testTool.elastic_query(query))
# print(testTool.zettair_query(query))

# X=[]
# with open('train.elmo.mini.tsv', 'rb') as inf:
#     for line in inf:
#         gzip_fields=line.decode('utf-8').split('\t')
#         gzip_id=gzip_fields[0]
#         gzip_label=gzip_fields[1]
#         elmo_embd_str=gzip_fields[4].strip()
#         elmo_embd_list=ast.literal_eval(elmo_embd_str)
#         elmo_embd_array=np.array(elmo_embd_list)
#         # pp.pprint(elmo_embd_array.shape)
#         # pp.pprint(type(elmo_embd_array))
#         padded_seq=sequence.pad_sequences(
#             [elmo_embd_array], maxlen=200, dtype='float32')[0]
#         # pp.pprint(padded_seq.shape)

#         text=testTool.arango_get_document(str(gzip_id))['text']
#         # text = testTool.elastic_get_document('0000001')['text']
#         ir_variables=testTool.arango_get_IR_variables(text, 'true')

#         # pp.pprint(np.full((1, 1024), ir_variables['CLASS_0_BM25_AVG']).shape)
#         padded_seq=np.concatenate((padded_seq, np.full(
#             (1, 1024), ir_variables['CLASS_0_BM25_AVG'])))
#         padded_seq=np.concatenate((padded_seq, np.full(
#             (1, 1024), ir_variables['CLASS_0_BM25_COUNT'])))
#         padded_seq=np.concatenate((padded_seq, np.full(
#             (1, 1024), ir_variables['CLASS_0_BM25_SUM'])))
#         padded_seq=np.concatenate((padded_seq, np.full(
#             (1, 1024), ir_variables['CLASS_1_BM25_AVG'])))
#         padded_seq=np.concatenate((padded_seq, np.full(
#             (1, 1024), ir_variables['CLASS_1_BM25_COUNT'])))
#         padded_seq=np.concatenate((padded_seq, np.full(
#             (1, 1024), ir_variables['CLASS_1_BM25_SUM'])))

#         X.append(padded_seq)

# pp.pprint(X[0].shape)
