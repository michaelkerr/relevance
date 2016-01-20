# -*- coding: utf-8 -*-
""" naive bayes keyword classifier """

"""
V0.1
Created Date: 2014/04/25
Last Updated: 2014/05/20
"""

import cPickle
from elasticsearch import Elasticsearch, Urllib3HttpConnection
import parse_content as pc
import numpy as np
import xlsxwriter
import sys

### CONFIGURATION ###
REL_LIMIT = 0.7

# Create a workbook and add a worksheet.
workbook = xlsxwriter.Workbook('penguin_rel_test_2.xlsx')
worksheet = workbook.add_worksheet()

params = ['clean', 'links', 'images', 'embed', 'ident', 'punct', 'misc', 'stop', 'encoding']

# Elasticsearch
ES_DOC_COUNT = 5000
ES_SCROLL_TIMEOUT = '5m'
es_host = 'search.vendorx.com'
es_port = '8080'
index_name = 'thor'
es_timeout = 30.0
es = Elasticsearch(
	connection_class=Urllib3HttpConnection,
	host=es_host,
	port=es_port,
	timeout=es_timeout,
	index=index_name,
	http_auth='outpost:4LpPpwA7kBzqsk',
	use_ssl=False,
	)


### MAIN ###
with open('outpost_nb_model_penguin.pkl', 'rb') as fid:
	model = cPickle.load(fid)

data_dict = {}
with open('test2.csv', 'r') as infile:
	for line in infile.readlines():
		temp_dict = {}
		temp_line = line.rstrip().split(',', 2)

		if temp_line[1] == 'YES':
			temp_dict['user_code'] = 'relevant'
		else:
			temp_dict['user_code'] = 'irrelevant'

		data_dict[temp_line[0]] = temp_dict

# Get the content from Thor ES
es_query = {
	"filtered": {
		"query": {
			"match_all": {}
			},
		"filter": {
			"or": [
				]
			}
		}
	}

for entry in data_dict.keys():
	es_query['filtered']['filter']['or'].append({"term": {"_id": str(entry)}})

es_content = []
first_query = True
queries_complete = False

while queries_complete is False:
	if first_query is True:
		es_response = es.search(index=index_name, body={'query': es_query}, scroll=ES_SCROLL_TIMEOUT, size=ES_DOC_COUNT)
		es_scroll_id = es_response['_scroll_id']
		total_docs = es_response['hits']['total']
		first_query = False
	else:
		es_response = es.scroll(scroll_id=es_scroll_id, scroll=ES_SCROLL_TIMEOUT)
		es_scroll_id = es_response['_scroll_id']

	if len(es_response['hits']['hits']) == 0:
		queries_complete = True
	else:
		for entry in es_response['hits']['hits']:
			data_dict[entry['_id']]['content'] = pc.parse_post(entry['_source']['twitter']['text'], params)

test_ids, test_user, test_content = [], [], []
for key in data_dict.keys():
	test_ids.append(key)
	test_user.append(data_dict[key]['user_code'])
	test_content.append(data_dict[key]['content'])


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
		temp_dict['auto_code'] = 'relevant'
	else:
		temp_dict['auto_code'] = 'irrelevant'
	if (temp_dict['user_code'] == 'relevant') and (temp_dict['auto_code'] == 'irrelevant'):
		temp_dict['correct'] = 0
	else:
		temp_dict['correct'] = 1
	#TODO add "capture"
	if temp_dict['user_code'] == temp_dict['auto_code']:
		temp_dict['capture'] = 1
	else:
		temp_dict['capture'] = 0
	output_content.append(temp_dict)


# Start from the first cell. Rows and columns are zero indexed.
worksheet.write(0, 0, 'Tweet Id')
worksheet.write(0, 1, 'Tweet Text')
worksheet.write(0, 2, 'Rel Prob')
worksheet.write(0, 3, 'Auto Code')
worksheet.write(0, 4, 'User Code')
worksheet.write(0, 5, 'Capture')
worksheet.write(0, 6, 'Correct')
row = 1

# Iterate over the data and write it out row by row.
for entry in output_content:
	col = 0
	worksheet.write(row, col, entry['tweet_id'])
	worksheet.write(row, col + 1, entry['text'])
	worksheet.write(row, col + 2, entry['rel_prob'])
	worksheet.write(row, col + 3, entry['auto_code'])
	worksheet.write(row, col + 4, entry['user_code'])
	worksheet.write(row, col + 5, entry['correct'])
	worksheet.write(row, col + 6, entry['capture'])
	row += 1

workbook.close()