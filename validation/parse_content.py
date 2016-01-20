# -*- coding: utf-8 -*-
""" Post content parser """

"""
V0.3
Created Date: 2014/05/14
Last Updated: 2014/06/02
"""

#from lib.snowballstemmer import *
from nltk import word_tokenize
#from nltk.corpus import stopwords_analysis
#from nltk.stem.isri import ISRIStemmer
import re

# Stemmer
minDigit = 3
#_languages = {
	#'danish': DanishStemmer,
	#'dutch': DutchStemmer,
	#'english': EnglishStemmer,
	#'finnish': FinnishStemmer,
	#'french': FrenchStemmer,
	#'german': GermanStemmer,
	#'hungarian': HungarianStemmer,
	#'italian': ItalianStemmer,
	#'norwegian': NorwegianStemmer,
	#'porter': PorterStemmer,
	#'portuguese': PortugueseStemmer,
	#'romanian': RomanianStemmer,
	#'russian': RussianStemmer,
	#'spanish': SpanishStemmer,
	#'swedish': SwedishStemmer,
	#'turkish': TurkishStemmer,
#}


def stem(word, isUsingCharLimit, languageId):
	stemmer = stemmer(languageId)
	if (not(isUsingCharLimit) or (isUsingCharLimit and (len(word) >= minDigit))) and word.lower() not in stopset:
		return stemmer.stemWord(word)
	else:
		#TODO throw an error/warning
		print 'Error in stemming'
	return ""


def parse_post(content, params, **args):
	"""
		Cleans the content of a social media post.
		Takes the content of ths post and optional arguments.
		Returns cleaned content.
		Parameters:
			clean
			encoding
			images
			links
			embed
			ident
			punct
			stop
			misc
			single
		Arguments:
			language
	"""
	if 'language' in args.keys():
		#TODO create 2 letter lookup code? modify stemmer to handly 2 letter code?
		language = args['language']
	else:
		language = 'english'
	if 'clean' in params:
		#TODO should probably do this by default
		""" Cleans tweet content of newlines, tabs, etc. """
		content = re.sub(' +', ' ', content)
		content = re.sub(r'\s+', ' ', content)
	if 'encoding' in params:
		""" Cleans tweet content of html encoding """
		content = content.replace('--AMP--quot;', '')
		content = content.replace('--SHARP--', '')
		content = content.replace('--AMP--apos;', '')
	if 'images' in params:
		""" Removes VK image links from content """
		#TODO validate
		#TODO account for when an image cant be split by space
		remove_list = []
		[remove_list.append(word) for word in content.split(' ') if ('[Image:' in word)]
		for word in remove_list:
			content = content.replace(word, '')
	if 'links' in params:
		""" Removes links from content. """
		remove_list = []
		[remove_list.append(word) for word in content.split(' ') if (word.startswith('http'))]
		[remove_list.append(word) for word in content.split(' ') if (word.startswith('pic.twitter'))]
		for word in remove_list:
			content = content.replace(word, '')
	if 'embed' in params:
		""" Removes embedded content from content. """
		remove_list = []
		[remove_list.append(word) for word in content.split(' ') if (word.startswith('[Embedded'))]
		[remove_list.append(word) for word in content.split(' ') if (word.startswith('content:http'))]
		for word in remove_list:
			content = content.replace(word, '')
	if 'ident' in params:
		""" Removes hashtags and mentions """
		exclude_signs = set('@#')
		remove_list = []
		[remove_list.append(word) for word in content.split(' ') if any((sym in exclude_signs) for sym in word)]
		for word in remove_list:
			content = content.replace(word, '')
		pass
	if 'punct' in params:
		""" Removes punctuation """
		exclude_punct = set(u'!^*()-_+=`~;:",<.>?|{[]}\'/؟“”هه')
		for sym in content:
			if sym in exclude_punct:
				content = content.replace(sym, '')
	if 'misc' in params:
		content = content.replace('Google Facebook Twitter Digg', '')
	if 'single' in params:
		""" Ensures that a word only appears once in the content """
		#TODO Validate
		content = ' '.join(list(set(word_tokenize(content))))
	if 'stop' in params:
		#TODO
		pass
		#stopset = set(stopwords_analysis.words(languageId))
	if 'stem' in params:
		#TODO
		#Check if stopwords supported, else allow custom stopwords
		pass
		#stopset = set(stopwords_analysis.words(languageId))
		#if language == 'arabic':
			#
		#temp_list = []
		#[temp_list.append(stem(word, True, language)) for word in content.split(' ')]
		#content = ' '.join(temp_list)

	return content


def clean_tweet(content):
	"""
		Cleans tweet content of newlines, tabs, etc.
		Takes in the content of a tweet, returns the cleaned content as a string
	"""
	content = re.sub(' +', ' ', content)
	content = re.sub(r'\s+', ' ', content)

	return content


def get_stopwords(lang_name):
	""" Gets and returns a list of stopwords """
	temp_list, stop_list = [], []
	if lang_name.lower() == 'arabic':
		with open('arabic_stop_words.txt', 'r') as input_file:
			temp_list = input_file.readlines()
		input_file.closed
		for entry in temp_list:
			stop_list.append(entry.decode('utf-8'))
	if lang_name.lower() == 'english':
		stop_list = stopwords.words('english')
	return stop_list


def read_stop_file(filename):
	""" Takes in a filename of a stopword file and returns a list of the words """
	with open(filename, 'r') as input_file:
		stop_list = input_file.readlines()
	input_file.closed
	return stop_list


def remove_identifiers(content):
	"""
		Removes hashtags and mentions
		Takes in content of a tweet (or other social media), returns the cleaned content as a string
	"""
	#TODO option to record these as entities
	exclude_signs = set('@#')
	remove_list = []
	[remove_list.append(word) for word in content.split(' ') if any((sym in exclude_signs) for sym in word)]
	for word in remove_list:
		content = content.replace(word, '')
	return content


def remove_links(content):
	"""
		Removes links from content.
		Takes in content of a tweet (or other social media), returns the cleaned content as a string
	"""
	#TODO option to record these as entities
	remove_list = []
	[remove_list.append(word) for word in content.split(' ') if (word.startswith('http'))]
	[remove_list.append(word) for word in content.split(' ') if (word.startswith('[Image:'))]
	for word in remove_list:
		content = content.replace(word, '')
	return content


def remove_punct(content):
	"""
		Removes punctuation from as tring
		Takes in a social media post and returns a clean string.
	"""
	exclude_punct = set('!^*()-_+=`~;:",<.>?|{[]}\'/؟“”هه')
	for sym in content:
		if sym in exclude_punct:
			content = content.replace(sym, '')
	return content


def remove_stopwords(content, language_list):
	"""
		Removes stopwords from a list
	"""
	#for lang in language_list:
		#stop_list = []
		#if lang == 'english':
			#stop_list = stop_list + stopwords.words('english')
		#if lang == 'arabic':
			#stopword_file = 'arabic_stop_words.txt'
			#stop_list = stop_list + read_stop_file(stopword_file)

	#stop_words = []
	#for entry in stop_list:
		#stop_words.append(entry.rstrip())
		##stop_words[entry.rstrip()] = ''

	#content_words = content.split(' ')
	#for word in content_words:
		#if word.encode('utf-8').lower() in stop_words:
			#content.replace(' ' + word + ' ', ' ')

	return content