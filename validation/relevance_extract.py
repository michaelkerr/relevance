# -*- coding: utf-8 -*-
""" Relevance Extract """

"""
V0.1
Created Date: 2014/05/13
Last Updated: 2014/07/17
"""

### RESOURCES ###
from datetime import date
from elasticsearch import Elasticsearch, Urllib3HttpConnection
#import elasticsearch
from random import randrange
import xlsxwriter


### CONFIGUATION ###
ES_DOC_COUNT = 5000
ES_SCROLL_TIMEOUT = '5m'
es_host = 'search.vendorx.com'
es_port = '8080'
index_name = 'thor'
es_timeout = 5.0
es = Elasticsearch(
	connection_class=Urllib3HttpConnection,
	host=es_host,
	port=es_port,
	timeout=es_timeout,
	#index=index_name,
	http_auth='outpost:4LpPpwA7kBzqsk',
	use_ssl=False,
	)

# General
project = 'penguin-1'
start_date = date(2014, 07, 01).strftime('%a, %d %b %Y %H:%M:%S +0000')
end_date = date(2014, 07, 15).strftime('%a, %d %b %Y 23:59:59 +0000')

# Xlswriter
workbook = xlsxwriter.Workbook(project + '_twitter_test' + '_july.xlsx')
worksheet = workbook.add_worksheet()


### CLASSES ###

### FUNCTIONS ###

### MAIN ###
es_query = {
	"filtered": {
		"query": {
			"match_all": {}
			},
		"filter": {"and": [
				{"range": {"interaction.created_at": {"gte": start_date, "lte": end_date}}},
				{"exists": {"field": "interaction.tag_tree." + project}},
				]
			}
		}
	}

print 'Querying es.......'
es_content = []
first_query = True
queries_complete = False
num_retr = 0

while queries_complete is False:
	if first_query is True:
		try:
			es_response = es.search(index=index_name, body={'query': es_query}, scroll=ES_SCROLL_TIMEOUT, size=ES_DOC_COUNT)
		except Exception as e:
			print e
		es_scroll_id = es_response['_scroll_id']
		total_tweets = es_response['hits']['total']
		print 'Found ' + str(total_tweets) + ' tweets.'
		first_query = False
	else:
		es_response = es.scroll(scroll_id=es_scroll_id, scroll=ES_SCROLL_TIMEOUT)
		es_scroll_id = es_response['_scroll_id']

	if len(es_response['hits']['hits']) == 0:
		queries_complete = True
	else:
		num_retr += len(es_response['hits']['hits'])
		print num_retr
		for entry in es_response['hits']['hits']:
			es_content.append(entry)

# Start from the first cell. Rows and columns are zero indexed.
worksheet.write(0, 0, 'Screen Name')
worksheet.write(0, 1, 'Tweet Text')
worksheet.write(0, 2, 'Created At')
worksheet.write(0, 3, 'Tweet Id')

row = 1

sample_total = 0
selected = []

while sample_total < 500:
	sample = randrange(total_tweets)

	if sample not in selected:
		col = 0
		tweet_dict = {}

		try:
			tweet_body = es_content[sample]['_source']['twitter']
			worksheet.write(row, col, tweet_body['user']['screen_name'])
			worksheet.write(row, col + 1, tweet_body['text'].strip(' \t\n\r'))
			worksheet.write(row, col + 2, tweet_body['created_at'])
			worksheet.write(row, col + 3, str(tweet_body['id']))
			row += 1
			sample_total += 1
		except Exception as e:
			print 'write error: ' + str(e)
			print tweet_body
	selected.append(sample)
	print sample_total


workbook.close()