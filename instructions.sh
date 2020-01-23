cd ~/Documentos/git/tcc-ii-ir-features-text-mining/tool-testing/
python time-index.py
cd ~/Documentos/git/tcc-ii-ir-features-text-mining/tool-testing/corpus_2_solution_1/
KERAS_BACKEND=tensorflow python CNN_elmo_adapted.py work/train-split.elmo.tsv
./ensemble_pred.sh work/test-split.elmo.tsv work/test-split-elastic-1-0.preds.txt
cd ~/Documentos/git/tcc-ii-ir-features-text-mining/tool-testing/
python corpus_1_solution_1/train_model_adapted.py
