import tensorflow as tf
import numpy as np
import random
import os
random_seed = 42
seed_value = random_seed

# 1. Set `PYTHONHASHSEED` environment variable at a fixed value
os.environ['PYTHONHASHSEED'] = str(seed_value)

# 2. Set `python` built-in pseudo-random generator at a fixed value
random.seed(seed_value)

# 3. Set `numpy` pseudo-random generator at a fixed value
np.random.seed(seed_value)

# 4. Set `tensorflow` pseudo-random generator at a fixed value
tf.set_random_seed(seed_value)

session_conf = tf.ConfigProto(
    intra_op_parallelism_threads=1, inter_op_parallelism_threads=1)
sess = tf.Session(graph=tf.get_default_graph(), config=session_conf)

from keras import backend as K
K.set_session(sess)

import time
import pickle
import datetime
import logging
import pandas as pd
import inspect
import sys
import ast
from keras.preprocessing import sequence
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedKFold
from keras.models import Model
from keras.callbacks import ModelCheckpoint
from argparse import ArgumentParser
from keras.layers import Input, Flatten, Dense, Activation, Average
from keras.layers import Concatenate, Dropout, Conv1D, MaxPooling1D, BatchNormalization

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
from indextoolmanager import IndexToolManager

variable_name = 'TRAIN_CLF'
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

add_ir_variables = True
ignore_first_result = True
exp_id = str(datetime.datetime.now())
tool = 'arango'
hyperpartisan_db_name = 'hyperpartisan_split_42_bulk'


exp_dict = {
    'exp_id': exp_id,
    'tool': tool,
    'db': 'hyperpartisan',
    'db_name': hyperpartisan_db_name,
    'add_ir_variables': add_ir_variables,
    'solution_number': '1',
    'solution_name': '1_bertha',
    'ir_top_k': 100,
    'ignore_first_result': ignore_first_result,
    'random_seed': random_seed,
    'train_input': '',
    'train_epochs': '',
}

testTool = IndexToolManager(
    indexName=str(hyperpartisan_db_name), top_k=exp_dict['ir_top_k'])


def load_elmo(path, max_len=200, add_ir_variables=False):
    '''
    load ELMo embedding from tsv file.
    :param path: tsv file path.
    :param to_pickle: Convert elmo embeddings to .npy file, avoid read and pad every time.
    :return: elmo embedding and its label.
    '''
    X = []
    label = []
    ids = []
    i = 0
    l_encoder = LabelEncoder()
    time_query_list = []
    time_query = 0.0
    with open(path, 'rb') as inf:
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
                    text = testTool.arango_get_document(str(gzip_id))[
                        'text']
                    initial = time.time()
                    ir_variables = testTool.arango_get_IR_variables(
                        text, 'true', ignore_first_result=ignore_first_result)
                    final = time.time()
                elif (tool == 'elastic'):
                    text = testTool.arango_get_document(str(gzip_id))[
                        'text']
                    initial = time.time()
                    ir_variables = testTool.elastic_get_IR_variables(
                        text, 'true', ignore_first_result=ignore_first_result)
                    final = time.time()
                elif (tool == 'zettair'):
                    text = testTool.arango_get_document(str(gzip_id))[
                        'text']
                    initial = time.time()
                    ir_variables = testTool.zettair_get_IR_variables(
                        text, 'true', interactive=False,
                        ignore_first_result=ignore_first_result)
                    final = time.time()
                # print(ir_variables)
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
            X.append(padded_seq)
            label.append(gzip_label)
            ids.append(gzip_id)
            i += 1
            # print(i)
    Y = l_encoder.fit_transform(label)

    result_id = datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f")
    time_query = np.mean(time_query_list)
    # testTool.log_result(result_id, {
    #     'exp_id': exp_id,
    #     'variable': 'TIME_QUERY',
    #     ** testTool.get_parameters(),
    #     'db': exp_dict['db'],
    #     'tool': tool,
    #     'db_name': exp_dict['db_name'],
    #     'add_ir_variables': 'true' if add_ir_variables else 'false',
    #     'solution_number': exp_dict['solution_number'],
    #     'solution_name': exp_dict['solution_name'],
    #     'execution_type': 'training',
    #     'train_input':  exp_dict['train_input'],
    #     'number_queries': str(len(time_query_list)),
    #     'value': str(time_query),
    # })    
    testTool.log_result(result_id, {
        'variable': 'TIME_QUERY',
        ** testTool.get_parameters(),
        ** exp_dict,
        'execution_type': 'training',
        'number_queries': str(len(time_query_list)),
        'value': str(time_query),
    })
    return np.array(X), np.array(Y), np.array(ids)


def conv1d(max_len, embed_size):
    '''
    CNN without Batch Normalisation.
    :param max_len: maximum sentence numbers, default=200
    :param embed_size: ELMo embeddings dimension, default=1024
    :return: CNN without BN model
    '''
    filter_sizes = [2, 3, 4, 5, 6]
    num_filters = 128
    drop = 0.5
    inputs = Input(shape=(max_len, embed_size), dtype='float32')

    conv_0 = Conv1D(num_filters, kernel_size=(filter_sizes[0]))(inputs)
    act_0 = Activation('relu')(conv_0)
    conv_1 = Conv1D(num_filters, kernel_size=(filter_sizes[1]))(inputs)
    act_1 = Activation('relu')(conv_1)
    conv_2 = Conv1D(num_filters, kernel_size=(filter_sizes[2]))(inputs)
    act_2 = Activation('relu')(conv_2)
    conv_3 = Conv1D(num_filters, kernel_size=(filter_sizes[3]))(inputs)
    act_3 = Activation('relu')(conv_3)
    conv_4 = Conv1D(num_filters, kernel_size=(filter_sizes[4]))(inputs)
    act_4 = Activation('relu')(conv_4)

    maxpool_0 = MaxPooling1D(pool_size=(max_len - filter_sizes[0]))(act_0)
    maxpool_1 = MaxPooling1D(pool_size=(max_len - filter_sizes[1]))(act_1)
    maxpool_2 = MaxPooling1D(pool_size=(max_len - filter_sizes[2]))(act_2)
    maxpool_3 = MaxPooling1D(pool_size=(max_len - filter_sizes[3]))(act_3)
    maxpool_4 = MaxPooling1D(pool_size=(max_len - filter_sizes[4]))(act_4)

    concatenated_tensor = Concatenate()(
        [maxpool_0, maxpool_1, maxpool_2, maxpool_3, maxpool_4])
    flatten = Flatten()(concatenated_tensor)
    dropout = Dropout(drop)(flatten)
    output = Dense(units=1, activation='sigmoid')(dropout)

    model = Model(inputs=inputs, outputs=output)
    # model = multi_gpu_model(model, gpus=gpus)
    model.summary()
    model.compile(loss='binary_crossentropy',
                  metrics=['acc'], optimizer='adam')
    return model


def conv1d_BN(max_len, embed_size):
    '''
    CNN with Batch Normalisation.
    :param max_len: maximum sentence numbers, default=200
    :param embed_size: ELMo embeddings dimension, default=1024
    :return: CNN with BN model
    '''
    filter_sizes = [2, 3, 4, 5, 6]
    num_filters = 128
    inputs = Input(shape=(max_len, embed_size), dtype='float32')
    conv_0 = Conv1D(num_filters, kernel_size=(filter_sizes[0]))(inputs)
    act_0 = Activation('relu')(conv_0)
    bn_0 = BatchNormalization(momentum=0.7)(act_0)

    conv_1 = Conv1D(num_filters, kernel_size=(filter_sizes[1]))(inputs)
    act_1 = Activation('relu')(conv_1)
    bn_1 = BatchNormalization(momentum=0.7)(act_1)

    conv_2 = Conv1D(num_filters, kernel_size=(filter_sizes[2]))(inputs)
    act_2 = Activation('relu')(conv_2)
    bn_2 = BatchNormalization(momentum=0.7)(act_2)

    conv_3 = Conv1D(num_filters, kernel_size=(filter_sizes[3]))(inputs)
    act_3 = Activation('relu')(conv_3)
    bn_3 = BatchNormalization(momentum=0.7)(act_3)

    conv_4 = Conv1D(num_filters, kernel_size=(filter_sizes[4]))(inputs)
    act_4 = Activation('relu')(conv_4)
    bn_4 = BatchNormalization(momentum=0.7)(act_4)

    maxpool_0 = MaxPooling1D(pool_size=(max_len - filter_sizes[0]))(bn_0)
    maxpool_1 = MaxPooling1D(pool_size=(max_len - filter_sizes[1]))(bn_1)
    maxpool_2 = MaxPooling1D(pool_size=(max_len - filter_sizes[2]))(bn_2)
    maxpool_3 = MaxPooling1D(pool_size=(max_len - filter_sizes[3]))(bn_3)
    maxpool_4 = MaxPooling1D(pool_size=(max_len - filter_sizes[4]))(bn_4)

    concatenated_tensor = Concatenate()(
        [maxpool_0, maxpool_1, maxpool_2, maxpool_3, maxpool_4])
    flatten = Flatten()(concatenated_tensor)
    output = Dense(units=1, activation='sigmoid')(flatten)

    model = Model(inputs=inputs, outputs=output)
    #model = multi_gpu_model(model, gpus=gpus)
    model.summary()
    model.compile(loss='binary_crossentropy',
                  metrics=['acc'], optimizer='adam')
    return model


mylogger.info('START OF CLF TRAINING')
mylogger.info(exp_id)

mylogger.info(f'DB: {exp_dict["db"]}')
mylogger.info(f'TOOL: {exp_dict["tool"]}')
mylogger.info(f'DB NAME: {exp_dict["db_name"]}')
mylogger.info(f'add_ir_variables: {add_ir_variables}')


parser = ArgumentParser()
parser.add_argument("inputTSV", help="Elmo format input file")
args = parser.parse_args()

seed = 7
max_len = 200
embed_size = 1024

x_data, y_data, ids = load_elmo(
    args.inputTSV, max_len=max_len, add_ir_variables=add_ir_variables)

mylogger.info(f'x_data shape: {x_data.shape}')
mylogger.info(f'y_data shape: {y_data.shape}')
# X_train, X_test, y_train, y_test = train_test_split(x_data, y_data,
#                                                     test_size=0.33,
#                                                     random_state=42)

# X_train, X_test, y_train, y_test = train_test_split(np.c_[y_data, ids], y_data, test_size=0.33333333, random_state=42)
# print(X_train)
# exit()

# sk-learn provides 10-fold CV wrapper.
kfold = StratifiedKFold(n_splits=10, shuffle=True, random_state=seed)
# list of validation accuracy from each fold.
cvscores = []
# counter to tell which fold.
i = 0
epochs = 30
cv_history_val_acc = []
if (add_ir_variables):
    max_len += 6

exp_dict['train_input'] = args.inputTSV
exp_dict['train_epochs'] = str(epochs)
exp_dict['train_final_score'] = str(np.mean(cvscores))

for train, test in kfold.split(x_data, y_data):
    i += 1
    print("current fold is : %s " % i)
    model = conv1d_BN(max_len, embed_size)
    checkpoints = ModelCheckpoint(filepath='./saved_models/BNCNN_vacc{val_acc:.4f}_f%s_e{epoch:02d}.hdf5' % str(i),
                                  verbose=1, monitor='val_acc', save_best_only=True)
    history = model.fit(x_data[train], y_data[train], batch_size=32, verbose=1,
                        epochs=epochs, callbacks=[checkpoints],
                        validation_data=[x_data[test], y_data[test]],
                        shuffle=False)
    # use the last validation accuracy from the 30 epochs
    his_val = history.history['val_acc']
    cv_history_val_acc.append(his_val)
    cvscores.append(his_val[-1])
    # clear memory
    K.clear_session()
mylogger.info("Final score: %.4f%% (+/- %.4f%%)" %
              (np.mean(cvscores), np.std(cvscores)))

# exp_dict['train_cv_history_val_acc'] = cv_history_val_acc

with open('saved_models/exp.pkl', 'wb+') as f:
    pickle.dump(exp_dict, f, pickle.HIGHEST_PROTOCOL)

mylogger.info(str(datetime.datetime.now()))
mylogger.info('END OF CLF TRAINING')
