import ast
from sklearn.preprocessing import LabelEncoder
from keras.preprocessing import sequence
import numpy as np
from keras.models import load_model
from optparse import OptionParser
from keras.layers import Average, Input
from keras.models import Model

import os
import sys
import inspect
import pandas as pd
import datetime
import logging
import pickle
import time
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
# sys.path.insert(0, parentdir)
sys.path.insert(0, '/home/ruan/Documentos/git/tcc-ii-ir-features-text-mining/tool-testing/')
from indextoolmanager import IndexToolManager

variable_name = 'CLF'
datestr = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# set up logging to file - see previous section for more details
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=f'logs/C2-S1-{datestr}-{variable_name}_debug.log',
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
handler1 = logging.FileHandler(f'logs/C2-S1-{datestr}-{variable_name}.log')
handler1.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler1.setFormatter(formatter)
mylogger.addHandler(handler1)

exp_dict = {}
with open('saved_models/exp.pkl', 'rb') as f:
    exp_dict = pickle.load(f)

add_ir_variables = exp_dict['add_ir_variables']
tool = exp_dict['tool']
hyperpartisan_db_name = exp_dict['db_name']
hyperpartisan_orig_db_name = 'hyperpartisan_bulk'
db = exp_dict['db']
db_name = exp_dict['db_name']

# exp_id = str(datetime.datetime.now())
exp_id = exp_dict['exp_id']
saved_model1 = ''
saved_model2 = ''
saved_model3 = ''
test_input = ''
print(exp_dict)
# exit()
testTool = IndexToolManager(
    indexName=str(hyperpartisan_db_name), top_k=exp_dict['ir_top_k'])

testToolOrig = IndexToolManager(
    indexName=str(hyperpartisan_orig_db_name), top_k=exp_dict['ir_top_k'])

def toEvaluationFormat(all_doc_ids, all_prediction):
    n_right_predictions = 0
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    evaluationFormatList = []
    for i in range(len(all_doc_ids)):
        current_doc_id = all_doc_ids[i]
        current_prob = all_prediction[i][0]
        current_doc_real_class = testToolOrig.arango_get_document(str(current_doc_id))[
            'class']
        #current_prob = all_prediction[i]
        if current_prob > 0.5:
            current_pred = 'true'
        else:
            current_prob = 1 - current_prob
            current_pred = 'false'

        if (current_pred == current_doc_real_class):
            n_right_predictions += 1
            if (current_pred == 'true'):
                tp += 1
            elif (current_pred == 'false'):
                tn += 1
        else:
            if (current_pred == 'true'):
                fp += 1
            elif (current_pred == 'false'):
                fn += 1
        evaluationFormat = str(current_doc_id) + ' ' + \
            str(current_pred) + ' ' + str(current_prob) + '\n'
        evaluationFormatList.append(evaluationFormat)
    accuracy = n_right_predictions/len(all_doc_ids)
    f1 = 2.0*tp/(2.0*tp + fp + fn)
    mylogger.info(f'n_right_predictions: {n_right_predictions}')
    mylogger.info(f'Accuracy: {accuracy}')
    mylogger.info(f'TP: {tp} \tFN: {fn}')
    mylogger.info(f'FP: {fp} \tTN: {tn}')
    mylogger.info(f'F1: {f1}')

    result_info = {
        # 'db': db,
        # 'tool': tool,
        # 'db_name': db_name,
        # 'add_ir_variables': 'true' if add_ir_variables else 'false',
        # 'solution_number': exp_dict['solution_number'],
        # 'solution_name': exp_dict['solution_name'],
        # 'ir_top_k': exp_dict['ir_top_k'],
        # 'ignore_first_result': exp_dict['ignore_first_result'],
        # 'random_seed': exp_dict['random_seed'],
        ** exp_dict,
        'saved_model1': saved_model1,
        'saved_model2': saved_model2,
        'saved_model3': saved_model3,
        'test_input': test_input,
        'TP': str(tp),
        'FP': str(fp),
        'TN': str(tn),
        'FN': str(fn),
        'accuracy': str(accuracy),
        'f1': str(f1),
    }
    result_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    testTool.log_result(result_id, {
        'variable': 'CLF_ACC',
        ** testTool.get_parameters(),
        ** result_info,
        'value': str(accuracy),
    })
    result_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    testTool.log_result(result_id, {
        'variable': 'CLF_F1',
        ** testTool.get_parameters(),
        ** result_info,
        'value': str(f1),
    })
    return evaluationFormatList


def load_data(data_path, max_len=200, add_ir_variables=False):
    data = []
    l = []
    ids = []
    i = 0
    l_encoder = LabelEncoder()
    time_query_list = []
    time_query = 0.0
    with open(data_path, 'rb') as inf:
        for line in inf:
            gzip_fields = line.decode('utf-8').split('\t')
            gzip_id = gzip_fields[0]

            gzip_label = gzip_fields[1]
            elmo_embd_str = gzip_fields[4].strip()
            elmo_embd_list = ast.literal_eval(elmo_embd_str)
            elmo_embd_array = np.array(elmo_embd_list)
            padded_seq = sequence.pad_sequences(
                [elmo_embd_array], maxlen=max_len, dtype='float32')[0]

            if (add_ir_variables):
                ir_variables = {}
                initial = None
                final = None
                if (tool == 'arango'):
                    text = testToolOrig.arango_get_document(str(gzip_id))[
                        'text']
                    initial = time.time()
                    ir_variables = testTool.arango_get_IR_variables(
                        text, 'true')
                    final = time.time()
                elif (tool == 'elastic'):
                    text = testToolOrig.elastic_get_document(str(gzip_id))[
                        'text']
                    initial = time.time()
                    ir_variables = testTool.elastic_get_IR_variables(
                        text, 'true')
                    final = time.time()
                elif (tool == 'zettair'):
                    text = testToolOrig.arango_get_document(str(gzip_id))[
                        'text']
                    initial = time.time()
                    ir_variables = testTool.zettair_get_IR_variables(
                        text, 'true', interactive=False)
                    final = time.time()
                time_query_list.append(float(final-initial))
                padded_seq = np.concatenate((padded_seq, np.full(
                    (1, 1024), ir_variables['CLASS_0_BM25_AVG'])))
                padded_seq = np.concatenate((padded_seq, np.full(
                    (1, 1024), ir_variables['CLASS_0_BM25_COUNT'])))
                padded_seq = np.concatenate((padded_seq, np.full(
                    (1, 1024), ir_variables['CLASS_0_BM25_SUM'])))
                padded_seq = np.concatenate((padded_seq, np.full(
                    (1, 1024), ir_variables['CLASS_1_BM25_AVG'])))
                padded_seq = np.concatenate((padded_seq, np.full(
                    (1, 1024), ir_variables['CLASS_1_BM25_COUNT'])))
                padded_seq = np.concatenate((padded_seq, np.full(
                    (1, 1024), ir_variables['CLASS_1_BM25_SUM'])))
                # padded_seq = np.concatenate((padded_seq, np.full(
                #     (1, 1024), gzip_id)))
            data.append(padded_seq)
            l.append(gzip_label)
            ids.append(gzip_id)
            i += 1
            # print(i)
    label = l_encoder.fit_transform(l)
    result_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    time_query = np.mean(time_query_list)
    testTool.log_result(result_id, {
        'exp_id': exp_id,
        'variable': 'TIME_QUERY',
        ** testTool.get_parameters(),
        ** exp_dict,
        'execution_type': 'testing',
        'test_input': test_input,
        'number_queries': str(len(time_query_list)),
        'value': str(time_query),
    })
    return np.array(data), np.array(label), np.array(ids)


def ensemble(models, model_input):
    outputs = [model(model_input) for model in models]
    y = Average()(outputs)

    model = Model(model_input, y, name='ensemble')

    return model


def save_test_split_elmo(data_path, max_len=200):
    df = pd.read_csv('data/test_set.csv', dtype=str)
    with open('work/test-split.elmo.tsv', 'wb') as fo:
        with open(data_path, 'rb') as inf:
            for line in inf:
                gzip_fields = line.decode('utf-8').split('\t')
                gzip_id = gzip_fields[0]
                if (df['0'].str.contains(str(gzip_id)).any()):
                    fo.write(line)
    return


def save_train_split_elmo(data_path, max_len=200):
    df = pd.read_csv('data/train_set.csv', dtype=str)
    with open('work/train-split.elmo.tsv', 'wb') as fo:
        with open(data_path, 'rb') as inf:
            for line in inf:
                gzip_fields = line.decode('utf-8').split('\t')
                gzip_id = gzip_fields[0]
                if (df['0'].str.contains(str(gzip_id)).any()):
                    fo.write(line)
    return


mylogger.info('START OF CLF TESTING')
mylogger.info(exp_id)

mylogger.info(f'DB: {exp_dict["db"]}')
mylogger.info(f'TOOL: {exp_dict["tool"]}')
mylogger.info(f'DB NAME: {exp_dict["db_name"]}')
mylogger.info(f'add_ir_variables: {add_ir_variables}')

parser = OptionParser()
parser.add_option("--inputTSV", help="load saved cache", type=str)
parser.add_option("--output", help="load saved cache", type=str)
parser.add_option("--saved_model1", help="load saved cache", type=str)
parser.add_option("--saved_model2", help="load saved cache", type=str)
parser.add_option("--saved_model3", help="load saved cache", type=str)

options, arguments = parser.parse_args()

max_len = 200
embed_size = 1024
seed = 7

x_data, y_data, doc_id = load_data(options.inputTSV, max_len=max_len,
                                   add_ir_variables=add_ir_variables)

mylogger.info(f'x_data shape: {x_data.shape}')
mylogger.info(f'y_data shape: {y_data.shape}')
mylogger.info(f'doc_id shape: {doc_id.shape}')

model1 = load_model(options.saved_model1)
model1.name = 'model1'
model2 = load_model(options.saved_model2)
model2.name = 'model2'
model3 = load_model(options.saved_model3)
model3.name = 'model3'

saved_model1 = str(options.saved_model1)
saved_model2 = str(options.saved_model2)
saved_model3 = str(options.saved_model3)

test_input = str(options.inputTSV)

models = [model1, model2, model3]

mylogger.info(models[0].input_shape[1:])
model_input = Input(shape=models[0].input_shape[1:], dtype='float32')

ensemble_models = ensemble(models, model_input)

pred = ensemble_models.predict(x_data)

all_pred = toEvaluationFormat(doc_id, pred)
with open(options.output, 'w') as fo:
    for item in all_pred:
        fo.write(item)

mylogger.info(str(datetime.datetime.now()))
mylogger.info('END OF CLF TESTING')
