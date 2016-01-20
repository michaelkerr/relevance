# -*- coding: utf-8 -*-
""" naive bayes model trainer and tuner"""

"""
V0.1
Created Date: 2014/04/25
Last Updated: 2014/04/28
"""

### RESOURCES ###
import cPickle
import parse_content as pc
from pymongo import MongoClient
import numpy as np
from sklearn.cross_validation import ShuffleSplit
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import f1_score
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline


### CONFIGURATION ###
# MongoDB Related
mongoclient = MongoClient('XXX.XXX.XXX.XXX', 27017)
mongo_db = mongoclient['relevance']
#training_data = mongo_db['training_set']
#test_data = mongo_db['test_set']
training_data = mongo_db['penguin_train']
test_data = mongo_db['penguin_test']


### CLASSES ###


### FUNCTIONS ###
def create_ngram_model():
	"""
		Create the base NB model pipeline for training.
		Returns the pipeline
	"""
	tfidf_ngrams = TfidfVectorizer(ngram_range=(1, 3), analyzer="word", binary=False)
	clf = MultinomialNB()
	pipeline = Pipeline([('vect', tfidf_ngrams), ('clf', clf)])
	return pipeline


def get_data():
	"""
		Get the training data from MongoDB.
		Returns two numpy arrays
	"""
	mongo_data = training_data.find()
	tweets, labels = [], []

	for entry in mongo_data:
		#print pc.remove_punct(pc.remove_links(pc.remove_identifiers(pc.clean_tweet(entry['content']))))
		#TODO Remove tweets that have spaces between every word
		#TODO such as "للَّهَ وَمَلَائِكَتَهُ يُصَلُّون عَلَى النَّبِي يَا أَيُّهَا الَّذِينَ آَمَنُوا صَلُ"
		tweets.append(pc.remove_punct(pc.remove_links(pc.remove_identifiers(pc.clean_tweet(entry['content'])))))
		labels.append(entry['user_code'])

	tweets = np.asarray(tweets)
	labels = np.asarray(labels)

	return tweets, labels


def grid_search_model(clf_factory, X, Y):
	cv = ShuffleSplit(n=len(X), n_iter=10, test_size=0.3, indices=True, random_state=0)

	param_grid = dict(vect__ngram_range=[(1, 1), (1, 2), (1, 3)],
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


### MAIN ###
# Get the training data
#TODO clean the data of stopwords, hashtags, links, etc
X_orig, Y_orig = get_data()

pos_neg = np.logical_or(Y_orig == '1', Y_orig == '0')
X = X_orig[pos_neg]
Y = Y_orig[pos_neg]
Y = Y == '1'

## Find the best fit model parameters
model = grid_search_model(create_ngram_model, X, Y)

## save the classifier
with open('outpost_nb_model_penguin.pkl', 'wb') as fid:
	cPickle.dump(model, fid)
