# -*- coding: utf-8 -*-
""" GCC General MNB Get Probabilities """

"""
V0.1
Created Date: 2014/05/22
Last Updated: 2014/05/22
"""

### RESOURCES ###
import cPickle
from datetime import datetime
from multiprocessing import cpu_count, Pool
import parse_content as pc
from pymongo import MongoClient
import numpy as np
import xlsxwriter


### CONFIGURATION ###
REL_LIMIT = 0.7

# MongoDB Related
mongoclient = MongoClient('XXX.XXX.XXX.XXX', 27017)
mongo_db = mongoclient['rel_test']
#training_data = mongo_db['gcc_train]
test_data = mongo_db['gcc_test']

# Create a workbook and add a worksheet.
workbook = xlsxwriter.Workbook('gcc_rel_test.xlsx')


### CLASSES ###


### FUNCTIONS ###
def get_data(topic_name):
	"""
		Get the training data from MongoDB.
		Returns a list of posts
	"""
	ids, content, labels = [], [], []
	params = ['clean', 'links', 'images', 'embed', 'ident', 'punct', 'misc', 'stop', 'encoding']

	mongo_data = test_data.find({'Topic': topic_name})

	for entry in mongo_data:
		ids.append(entry['PostId'])
		content.append(pc.parse_post(entry['Content'], params))
		labels.append(topic_name)

	content = np.asarray(content)
	labels = np.asarray(labels)

	return ids, content, labels


def get_model(topic_name):
	filename = 'gcc_%s_nb_model_notop.pkl' % topic_name
	with open(filename, 'rb') as fid:
		model = cPickle.load(fid)
	return model


### MAIN ###
if __name__ == '__main__':
	start_time = datetime.now()

	topics = test_data.distinct('Topic')
	if 'Irrelevant' in topics:
		topics.remove('Irrelevant')

	print 'Getting topic models...'
	topic_models = {}
	for topic in topics:
		topic_models[topic] = get_model(topic)
	print 'done.'

	print 'Coding posts.....'
	for topic in topics:
		print 'Getting test data for %s.....' % topic
		post_ids, post_contents, post_labels = get_data(topic)
		print 'done.'
		# Create a new tab
		worksheet = workbook.add_worksheet(topic)
		# Create the column titles
		worksheet.write(0, 0, 'Post Id')
		worksheet.write(0, 1, 'User Code')
		worksheet.write(0, 2, 'Auto Code')
		worksheet.write(0, 3, 'Prob')
		worksheet.write(0, 4, 'Content')
		row = 1

		#TODO dont really need the label - its all the same since we searched for it in mongo, save memory
		predict = topic_models[topic].predict_proba(post_contents)
		num_posts, sum_rel = 0, 0
		for postid, content, label, code in zip(post_ids, post_contents, post_labels, predict):
			if label == topic:
				worksheet.write(row, 0, postid)
				worksheet.write(row, 1, label)
				if code[1] > REL_LIMIT:
					worksheet.write(row, 2, 'Relevant')
					worksheet.write(row, 3, code[1])
					sum_rel += 1
				else:
					worksheet.write(row, 2, 'Irrelevant')
					worksheet.write(row, 3, code[1])
				worksheet.write(row, 4, content)
				row += 1
				num_posts += 1
		accuracy = float(sum_rel) / float(num_posts)
		print 'Accuracy = %s.' % accuracy

	runtime = datetime.now() - start_time
	print 'Total runtime %s' % runtime

	workbook.close()
