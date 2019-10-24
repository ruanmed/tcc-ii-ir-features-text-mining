from indextoolmanager import IndexToolManager
import time
# import timeit
import pprint

authorprof_xml_folder = 'db_authorprof/en/'
authorprof_truth_txt = 'db_authorprof/truth.txt'
botgender_xml_folder = 'db_botgender/en/'
botgender_truth_txt = 'db_botgender/truth.txt'
articles_xml = 'db_hyperpartisan/articles.xml'
ground_truth_xml = 'db_hyperpartisan/ground_truth.xml'

# DB_AUTHORPROF


def index_DB_AUTHORPROF_TOOL_ARANGO():
    testTool = IndexToolManager(indexName='authorprof')

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


def index_bulk_DB_AUTHORPROF_TOOL_ARANGO():
    testTool = IndexToolManager(indexName='authorprof_bulk')

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


def index_DB_AUTHORPROF_TOOL_ELASTIC():
    testTool = IndexToolManager(indexName='authorprof')

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


def index_bulk_DB_AUTHORPROF_TOOL_ELASTIC():
    testTool = IndexToolManager(indexName='authorprof_bulk')

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


def index_bulk_DB_AUTHORPROF_TOOL_ZETTAIR():
    testTool = IndexToolManager(indexName='authorprof_bulk')

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


# DB_BOTGENDER

def index_DB_BOTGENDER_TOOL_ARANGO():
    testTool = IndexToolManager(indexName='botgender')

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


def index_bulk_DB_BOTGENDER_TOOL_ARANGO():
    testTool = IndexToolManager(indexName='botgender_bulk')

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


def index_DB_BOTGENDER_TOOL_ELASTIC():
    testTool = IndexToolManager(indexName='botgender')

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


def index_bulk_DB_BOTGENDER_TOOL_ELASTIC():
    testTool = IndexToolManager(indexName='botgender_bulk')

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


def index_bulk_DB_BOTGENDER_TOOL_ZETTAIR():
    testTool = IndexToolManager(indexName='botgender_bulk')

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


# DB_HYPERPARTISAN

def index_DB_HYPERPARTISAN_TOOL_ARANGO():
    testTool = IndexToolManager(indexName='hyperpartisan')

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        articles_xml, ground_truth_xml)
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


def index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO():
    testTool = IndexToolManager(indexName='hyperpartisan_bulk')

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        articles_xml, ground_truth_xml)
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


def index_DB_HYPERPARTISAN_TOOL_ELASTIC():
    testTool = IndexToolManager(indexName='hyperpartisan')

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        articles_xml, ground_truth_xml)
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


def index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC():
    testTool = IndexToolManager(indexName='hyperpartisan_bulk')

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        articles_xml, ground_truth_xml)
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


def index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR():
    testTool = IndexToolManager(indexName='hyperpartisan_bulk')

    start = time.time()
    bulk = testTool.get_documents_DB_HYPERPARTISAN(
        articles_xml, ground_truth_xml)
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


# index_bulk_DB_HYPERPARTISAN_TOOL_ARANGO()

# index_bulk_DB_HYPERPARTISAN_TOOL_ELASTIC()

# index_bulk_DB_HYPERPARTISAN_TOOL_ZETTAIR()


# index_DB_HYPERPARTISAN_TOOL_ARANGO()

# index_DB_HYPERPARTISAN_TOOL_ELASTIC()

pp = pprint.PrettyPrinter(indent=4)

testTool = IndexToolManager(indexName='hyperpartisan_bulk')

query = 'the'
# print(testTool.queryArango(query))
# result_df = testTool.queryElastic(query)
# pp.pprint(testTool.calc_IR(result_df, 'true'))
# print(testTool.queryElastic(query))
print(testTool.queryZettair(query))
