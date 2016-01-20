# -*- coding: utf-8 -*-
""" naive bayes model trainer and tuner"""

"""
V0.3
Created Date: 2014/05/14
Last Updated: 2014/06/02
"""

### RESOURCES ###
import cPickle
from datetime import datetime
import logging
from multiprocessing import cpu_count, Pool
import numpy as np
import parse_content as pc
from pymongo import MongoClient
from os import getcwd
from sklearn.cross_validation import ShuffleSplit
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import f1_score
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline


### CONFIGURATION ###
# MongoDB Related
mongoclient = MongoClient('XXX.XXX.XXX.XXX', 27017)
mongo_db = mongoclient['rel_test']
training_data = mongo_db['gcc_train']

local_path = getcwd()


### FUNCTIONS ###
def create_ngram_model():
	"""
		Create the base NB model pipeline for training.
		Returns the pipeline
	"""
	tfidf_ngrams = TfidfVectorizer(ngram_range=(1, 1), analyzer="word", binary=False)
	clf = MultinomialNB()
	pipeline = Pipeline([('vect', tfidf_ngrams), ('clf', clf)])
	return pipeline


def get_data(topic_tag):
	"""
		Get the training data from MongoDB.
		Returns a list of posts
	"""
	content, labels = [], []
	params = ['clean', 'links', 'images', 'embed', 'ident', 'punct', 'misc', 'stop', 'encoding']

	mongo_data = training_data.find({'Topic': topic_tag})

	for entry in mongo_data:
		content.append(pc.parse_post(entry['Content'], params))
		labels.append('1')

	num_posts = len(content)
	mongo_data = training_data.find({'Topic': 'Irrelevant'})
	for entry in mongo_data:
		if len(content) <= num_posts * 2:
			content.append(pc.parse_post(entry['Content'], params))
			#labels.append(entry['Topic'])
			labels.append('0')

	content = np.asarray(content)
	labels = np.asarray(labels)

	return content, labels


def grid_search_model(clf_factory, X, Y):
	cv = ShuffleSplit(n=len(X), n_iter=10, test_size=0.3, indices=True, random_state=0)

	param_grid = dict(vect__ngram_range=[(1, 1)],  # (1, 2), (1, 3)],
		vect__min_df=[1, 2],
		vect__stop_words=[None, "english"],
		vect__smooth_idf=[False, True],
		vect__use_idf=[False, True],
		vect__sublinear_tf=[False, True],
		vect__binary=[False, True],
		clf__alpha=[0, 0.01, 0.05, 0.1, 0.5, 1],
		)

	grid_search = GridSearchCV(clf_factory(),
			param_grid=param_grid,
			cv=cv,
			score_func=f1_score,
			verbose=10)
	grid_search.fit(X, Y)

	return grid_search.best_estimator_


def mnb_train_model(topic_name):
	"""
		Train the model for a specific topic.
		Returns a traine dmodel
	"""
	topic_start = datetime.now()

	print 'Getting data for %s.' % topic_name
	get_data(topic_name)
	X_orig, Y_orig = get_data(topic_name)

	print X_orig[0]
	print Y_orig[0]
	print 'Training MN model for %s.' % topic_name
	#pos_neg = np.logical_or(Y_orig == topic_name, Y_orig == 'Irrelevant')
	pos_neg = np.logical_or(Y_orig == '1', Y_orig == '0')
	X = X_orig[pos_neg]
	Y = Y_orig[pos_neg]
	#Y = Y == 'Irrelevant'
	Y = Y == '1'

	print 'Finding the best fit parameters'
	model = grid_search_model(create_ngram_model, X, Y)

	print 'Saving the classifier model'
	filename = 'gcc_%s_nb_model_notop.pkl' % topic_name
	with open(filename, 'wb') as fid:
		cPickle.dump(model, fid)
	runtime = datetime.now() - topic_start
	print 'completed in %s' % runtime
	return runtime


### MAIN ###
if __name__ == '__main__':
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
	start_time = datetime.now()

	topics = training_data.distinct('Topic')
	if 'Irrelevant' in topics:
		topics.remove('Irrelevant')

	print 'Training MNB models.....'
	pool = Pool(processes=cpu_count())
	pool.map(mnb_train_model, topics)
	print 'done.'

	runtime = datetime.now() - start_time
	print 'Total runtime %s' % runtime
