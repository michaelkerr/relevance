# -*- coding: utf-8 -*-
""" naive bayes keyword classifier """

"""
V0.1
Created Date: 2014/04/25
Last Updated: 2014/05/20
"""

import cPickle
from pymongo import MongoClient
import numpy as np
import xlsxwriter


### CONFIGURATION ###
REL_LIMIT = 0.7

# MongoDB Related
mongoclient = MongoClient('XXX.XXX.XXX.XXX', 27017)
mongo_db = mongoclient['relevance']
#training_data = mongo_db['training_set']
#test_data = mongo_db['test_set']
training_data = mongo_db['penguin_train']
test_data = mongo_db['penguin_test']

# Create a workbook and add a worksheet.
workbook = xlsxwriter.Workbook('twitter_rel_test.xlsx')
worksheet = workbook.add_worksheet()


### MAIN ###
#with open('nb_model.pkl', 'rb') as fid:
with open('outpost_nb_model_penguin.pkl', 'rb') as fid:
	model = cPickle.load(fid)

# Get some testing data
test_data = test_data.find()
test_content = []
test_ids = []
test_user = []

for entry in test_data:
	test_content.append(entry['content'])
	test_ids.append(entry['id'])
	if entry['user_code'] == '1':
		user_code = 'relevant'
	else:
		user_code = 'irrelevant'
	test_user.append(user_code)

test_content = np.asarray(test_content)
test_ids = np.asarray(test_ids)

predicted = model.predict_proba(test_content)

output_content = []
for postid, tweet_text, label, code in zip(test_ids, test_content, predicted, test_user):
	temp_dict = {}
	temp_dict['tweet_id'] = postid
	temp_dict['text'] = tweet_text
	temp_dict['rel_prob'] = float(label[1])
	temp_dict['user_code'] = code
	if label[1] > REL_LIMIT:
		temp_dict['auto_rel'] = 'relevant'
	else:
		temp_dict['auto_rel'] = 'irrelevant'
	output_content.append(temp_dict)


# Start from the first cell. Rows and columns are zero indexed.
worksheet.write(0, 0, 'Tweet Id')
worksheet.write(0, 1, 'Tweet Text')
worksheet.write(0, 2, 'Rel Prob')
worksheet.write(0, 3, 'Auto Code')
worksheet.write(0, 4, 'User Code')
worksheet.write(0, 5, 'Correct')
worksheet.write(0, 6, 'Capture')
row = 1

# Iterate over the data and write it out row by row.
for entry in output_content:
	col = 0
	worksheet.write(row, col, entry['tweet_id'])
	worksheet.write(row, col + 1, entry['text'])
	worksheet.write(row, col + 2, entry['rel_prob'])
	worksheet.write(row, col + 3, entry['auto_rel'])
	worksheet.write(row, col + 4, entry['user_code'])
	if entry['auto_rel'] == entry['user_code']:
		worksheet.write(row, col + 5, 1)
	else:
		worksheet.write(row, col + 5, 0)
	#TODO NEEDS UPDATING
	if (entry['user_code'] == 'relevant') and (entry['auto_rel'] == 'irrelevant'):
		worksheet.write(row, col + 6, 0)
	else:
		worksheet.write(row, col + 6, 1)
	row += 1

workbook.close()
