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
# Elasticsearch
es_host = 'search.vendorx.com'
es_port = '8080'
index_name = 'thor'
es_timeout = 30.0
#es = elasticsearch.Elasticsearch(host=es_host, port=es_port, timeout=es_timeout, log='trace')
es = Elasticsearch(
	connection_class=Urllib3HttpConnection,
	host=es_host,
	port=es_port,
	timeout=es_timeout,
	index=index_name,
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
		"from": 0,
		"size": 1,
		"filtered": {"filter": {"and": [
				{"range": {"interaction.created_at": {"gte": start_date, "lte": end_date}}},
				{"exists": {"field": "interaction.tag_tree." + project}},
				]
			}
		}
	}

print 'Querying es.......'
try:
	es_response = es.search(index=index_name, body={'query': es_query})
	total_tweets = es_response['hits']['total']
	print 'Found ' + str(total_tweets) + ' tweets.'
except Exception as e:
	print 'initial connect error: ' + str(e)

if total_tweets > 0:
	try:
		es_query['size'] = total_tweets
		tweet_data = es.search(index=index_name, body={'query': es_query})['hits']['hits']
	except Exception as e:
		print 'secondary connect error: ' + str(e)
print 'done.'

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
	tweet_body = tweet_data[sample]['_source']['twitter']

	if (sample not in selected) and ('user' in tweet_body['user']):
		col = 0
		tweet_dict = {}

		try:
			worksheet.write(row, col, tweet_body['user']['screen_name'])
			worksheet.write(row, col + 1, tweet_body['text'].strip(' \t\n\r'))
			worksheet.write(row, col + 2, tweet_body['created_at'])
			worksheet.write(row, col + 3, str(tweet_body['id']))
			row += 1
		except Exception as e:
			print 'write error: ' + str(e)
			print tweet_body
	selected.append(sample)
	sample_total += 1
	print sample_total


# Export the user needed items to an excel spreadsheet, upload to mongodb
#for entry in selected:
	#col = 0
	#tweet_dict = {}

	#try:
		#tweet_body = tweet_data[entry]['_source']['twitter']
		#worksheet.write(row, col, tweet_body['user']['screen_name'])
		#worksheet.write(row, col + 1, tweet_body['text'].strip(' \t\n\r'))
		#worksheet.write(row, col + 2, tweet_body['created_at'])
		#worksheet.write(row, col + 3, str(tweet_body['id']))
		#row += 1
	#except Exception as e:
		#print 'write error: ' + str(e)
		#print tweet_body

workbook.close()