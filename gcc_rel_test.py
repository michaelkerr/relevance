# -*- coding: utf-8 -*-
""" GCC Relevance Test Report """

"""
V0.1
Created Date: 2014/06/18
Last Updated: 2014/06/18
"""

### RESOURCES ###
import cPickle
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import gc
from multiprocessing import cpu_count, Pool
import os
import parse_content as pc
import numpy as np

import sys

### CONFIGURATION ###
#TODO handle this better
local_path = '/media/sf_Projects/relevance/'
NUM_PROCS = cpu_count()
params = ['clean', 'links', 'images', 'embed', 'ident', 'punct', 'misc', 'stop', 'encoding']

start_date = date(2014, 4, 1)
end_date = date(2014, 4, 30)
day = timedelta(days=1)
daterange = lambda d1, d2: (d1 + timedelta(days=i) for i in range((d2 - d1).days + 1))

# Elasticsearch
ES_DOC_COUNT = 5000
ES_SCROLL_TIMEOUT = '5m'
es_host = 'XXX.XXX.XXX.XXX'
es_port = '9200'
index_name = 'gcc_test2'
es_timeout = 30.0
es = Elasticsearch(host=es_host, port=es_port, timeout=es_timeout)

es_query = {
	"filtered": {
		"query": {
			"match_all": {}
			},
		}
	}

# Topics
topic_map = {
	'c9cab6be-8265-40f1-8d40-5f3edbbef2ec': 'AQ Ideology',
	'0db46c0c-6f39-4eec-a51a-18966705f87c': 'AQAA Military Engagement',
	'38245855-243d-4211-bfde-8e70c131cf48': 'AQAA Non Violent Activity',
	'0c0ac7d2-a6f0-41fe-910f-f18de66b6b01': 'AQAA Terrorist Activity'
	}


### CLASSES ###


### FUNCTIONS ###
def code_posts(es_docs):
	contents, es_posts = [], []

	es_docs = es_docs['hits']['hits']
	for es_post in es_docs:
		contents.append(pc.parse_post(es_post['_source']['post']['content'], params))
	contents = np.asarray(contents)

	temp_data = []
	for es_post in es_docs:
		es_post['_source']['analysis'] = {'relevance': []}
		es_post.pop('_score', None)
		temp_data.append(es_post)
	es_docs = temp_data

	#TODO Only update the analysis field
	for topic in topic_models:
		predict = topic_models[topic].predict_proba(contents)
		temp_data = []

		for es_post, code in zip(es_docs, predict):
			es_post['_source']['analysis']['relevance'].append({'topicid': topic, 'code': code[1]})
			temp_data.append(es_post)
		es_docs = temp_data

	try:
		helpers.bulk(es, es_docs)
	except Exception as e:
		with open('temp.txt', 'a') as outfile:
			print 'Exception: logging'
			outfile.write('>Event Start:' + '\n' + str(e) + '\n')
			#outfile.write(str(es_docs))
	return


def get_models(prefix):
	models = {}
	# Get a list of files in the working directory (or other) that start with the prefix
	files = [i for i in os.listdir(local_path) if os.path.isfile(os.path.join(local_path, i)) and prefix in i and 'pkl' in i]
	for filename in files:
		with open(filename, 'rb') as fid:
			models[filename.replace('gcc_', '').replace('.pkl', '')] = cPickle.load(fid)
	return models


### MAIN ###
if __name__ == '__main__':
	start_time = datetime.now()

	queries_complete = False
	first_query = True
	count = 0

	while queries_complete is False:
		results_list = []

		if first_query is True:
			es_response = es.search(index=index_name, body={'query': es_query}, scroll=ES_SCROLL_TIMEOUT, size=ES_DOC_COUNT)
			es_scroll_id = es_response['_scroll_id']
			total_docs = es_response['hits']['total']
			print total_docs
			sys.exit()
			first_query = False
		else:
			es_response = es.scroll(scroll_id=es_scroll_id, scroll=ES_SCROLL_TIMEOUT)
			es_scroll_id = es_response['_scroll_id']
		for entry in es_response:
			if (len(es_response['hits']['hits']) != 0) and (entry not in results_list):
				results_list.append(es_response)

		count += len(es_response['hits']['hits'])
		print 'Retrieved %s of %s docs' % (count, total_docs)

		if len(es_response['hits']['hits']) == 0:
			queries_complete = True

		print 'Creating report....'
		print 'done.'

		#gc.collect()

	print es.clear_scroll(scroll_id=es_scroll_id)
	runtime = datetime.now() - start_time
	print 'Total runtime %s' % runtime
