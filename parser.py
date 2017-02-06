import sys
import re
from elasticsearch import Elasticsearch
from yandex_translate import YandexTranslate
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


class BilingualPraser():

	def __init__(self, file1, file2, lang1, lang2):
		self.es = Elasticsearch()

		self.translate = YandexTranslate('trnsl.1.1.20161120T092429Z.26536300f1fab524.bb5fcc81494303125cb46f52681dfdcf652477e0')

		self.file1 = file1
		self.file2 = file2

		self.file1_lang = lang1
		self.file2_lang = lang2

		self.paragraphLinks = []

		self.withoutWhiteChars = re.compile(r'[\n\t \r]+')
		self.nonAlphaNumeric = re.compile(r'^\W+$')
		self.endOfSetnence = re.compile(r'([\.\?\!:])')
		self.sentences_pair = []
		self.getNameWords = re.compile(r'([A-ZА-Я]\w+)')
		self.onlyLetters = re.compile(r'[^a-zA-Zа-яА-Я ]+')

		self.MIN_RATE = 75
		self.synced_sentences = []
		self.nonsynced_sentences = {'first': [], 'second': [], 'keywords': []}

	def parse(self):
		par1 = parSplit(self.file1.readlines())
		par2 = parSplit(self.file2.readlines())
		self.firstPhase(par1, par2)
		self.secondPhase()
		self.splitSentences()
		return self.synced_sentences

		
	def firstPhase(self, pars1, pars2):
		for i in range(0, len(pars1)):
			pars1[i] = self.withoutWhiteChars.sub(' ', pars1[i])
			self.paragraphLinks.append({'first': pars1[i]})
		for i in range(0, len(pars2)):
			if i == len(self.paragraphLinks):
				pars2[i] = self.withoutWhiteChars.sub(' ', pars2[i])
				self.paragraphLinks.append({'second': pars2[i]})
			else:
				pars2[i] = self.withoutWhiteChars.sub(' ', pars2[i])
				self.paragraphLinks[i]['second'] = pars2[i]



	def parSplit(self, file_lines):
		paragraphs = []
		curr_par = 0
		par_whitespace = False
		for i in range(0, len(file_lines)):
			if file_lines[i][0] == ' ' \
					or file_lines[i][0] == '\t' \
					or file_lines[i][0] == '\n':
				file_lines[i] = file_lines[i].strip()
				if len(file_lines[i]) == 0:
					par_whitespace = True
				else:
					if self.nonAlphaNumeric.match(file_lines[i]) == None:
						paragraphs.append(file_lines[i])
						curr_par = len(paragraphs)
						par_whitespace = False
					else:
						par_whitespace = True
				continue
			if par_whitespace:
				paragraphs.append('')
				curr_par = len(paragraphs)
			par1_whitespace = False
			paragraphs[curr_par - 1] += ' ' + file_lines[i]
		return paragraphs



# def secondPhase():


	def getSentence(self, par):
		for pars in self.paragraphLinks:
			if par in pars.keys():
				# print('/-----------------------')
				sentences = self.endOfSetnence.split(pars[par])
				sentences_parsed = []
				newSentence = True
				curr_len = 0
				additional = ''
				brace_opened = False
				# print('/-----------------------')
				# print(sentences)
				for sentence in sentences:
					if self.nonAlphaNumeric.match(sentence) != None:
						if newSentence:
							if sentence == '\"':
								brace_opened = True
							additional = sentence
							newSentence = False
						else:
							if curr_len > 0:
								if sentence == '\"':
									if brace_opened:
										brace_opened = False
										sentences_parsed[curr_len - 1] += sentence
									else:
										brace_opened = True
										additional = sentence
								else:
									sentences_parsed[curr_len - 1] += sentence
							else:
								additional += sentence
					else:
						if len(sentence) == 0:
							continue
						sentences_parsed.append(additional + sentence)
						additional = ''
						curr_len = len(sentences_parsed)
						newSentence = False
				pars[par + '_sentences'] = sentences_parsed


# for pars in paragraphLinks:
#   if 'first' in pars.keys():
#       print('/--------------------------')
#       print(pars['first'])
#   if 'second' in pars.keys():
#       print('/--------------------------')
#       print(pars['second'])


	def secondPhase(self):
		self.getSentence('first')
		self.getSentence('second')
		sentence_count = []
		for par in self.paragraphLinks:
			if 'first_sentences' in par.keys():
				sentence_count.append(len(par['first_sentences']))
				for sent in par['first_sentences']:
					self.sentences_pair.append({'first': sent})

		offset = 0
		secondOffset = 0
		paragraphId = 0
		maxOffset = sentence_count[0]
		for par in self.paragraphLinks:
			if 'second_sentences' in par.keys():
				for sent in par['second_sentences']:

					if len(self.sentences_pair) > offset:
						self.sentences_pair[offset]['second'] = sent
						curr_par = self.paragraphLinks[paragraphId]
						if not 'second_phase' in curr_par.keys():
							curr_par['second_phase'] = []
						curr_par[
							'second_phase'].append(self.sentences_pair[offset])
					else:
						self.sentences_pair.append({'second': sent})
					offset += 1
					secondOffset += 1
					if maxOffset == secondOffset:
						if (len(sentence_count) - 1) <= paragraphId:
							maxOffset = len(par['second_sentences'])
						else:
							secondOffset = 0
							paragraphId += 1
							maxOffset = sentence_count[paragraphId]


	def updateTranslateElastica(self, data_id, lang, word):
		insert_body = {'doc': {}}
		insert_body['doc']['word_' + lang] = word
		resp = self.es.update(index='languages', doc_type='translates', id=data_id, body=insert_body)
		if resp['result'] != 'updated':
			print(resp)

	def addWordToElasticaOnlyIfTranslateExists(self, word_not_translated, word_translated, lang1, lang2):
		body = {"query": {'bool': {'must': [{'match': {}}]}}}
		body['query']['bool']['must'][0]['match']['word_' + lang2] = word_translated
		print(body)
		res = self.es.search(index="languages", doc_type="translates", body=body)
		if res['hits']['total'] == 0:
			insert_body = {}
			insert_body['word_' + lang1] = word_not_translated
			insert_body['word_' + lang2] = word_translated
			resp = es.index(index='languages', doc_type='translates', body=insert_body)
			if resp['result'] != 'created':
				print('Word ' + word_translated + ' was not inserted')
		else:
			data_id = res['hits']['hits'][0]['_id']
			self.updateTranslateElastica(data_id, lang1, word_not_translated)

	def getTranslate(self,word, lang1, lang2):
		body = {"query": {'bool': {'must': [{'match': {}}]}}}
		body['query']['bool']['must'][0]['match']['word_' + lang1] = word
		res = self.es.search(index="languages", doc_type="translates", body=body)
		if res['hits']['total'] == 0:
			translate_res = self.translate.translate(word, lang1 + '-' + lang2)
			if translate_res['code'] == 200:
				self.addWordToElasticaOnlyIfTranslateExists(word, translate_res['text'][0], lang1, lang2)
				# print(translate_res['text'])
				return translate_res['text'][0]
			else:
				print(translate_res)
				return None
		else:
			if not 'word_' + lang2 in res['hits']['hits'][0]['_source'].keys():
				data_id = res['hits']['hits'][0]['_id']
				translate_res = self.translate.translate(word, lang1 + '-' + lang2)
				if translate_res['code'] == 200:
					self.updateTranslateElastica(data_id, lang2, translate_res['text'][0])
					return translate_res['text'][0]
				else:
					print(translate_res)
					return None
			else:
				# print(res)
				return res['hits']['hits'][0]['_source']['word_' + lang2]


	def getSentenceRate(self, keyWords, sentence):
		# sentence = ' '.join(sentences)
		cleared_sentence = self.onlyLetters.sub('', sentence)
		word_array = cleared_sentence.split(' ')
		rate = 0.0
		# print('getSentenceRate')
		# print('//------------')
		# print(keyWords)
		# print('//------------')
		# print(sentence)
		for word in keyWords:
			res = process.extractOne(word, word_array)
			if res != None:
				if res[1] > MIN_RATE:
					rate += res[1]
					word_array.remove(res[0])

		rate = rate / len(keyWords)
		return rate

	def getSentenceSize(self, sentence):
		cleared_sentence = self.onlyLetters.sub('', sentence)
		word_array = cleared_sentence.split(' ')
		# word_array = list(filter(lambda x: len(x) > 2, word_array))
		return len(word_array)

	def getSentenceSub(self, sentence1, sentence2):
		# sentence1 = ' '.join(sentences1)
		# sentence2 = ' '.join(sentences2)

		# print(sentence1, sentence2)

		cleared_sentence1 = self.onlyLetters.sub('', sentence1)
		cleared_sentence2 = self.onlyLetters.sub('', sentence2)

		word_array1 = cleared_sentence1.split(' ')
		word_array2 = cleared_sentence2.split(' ')
		# word_array1 = list(filter(lambda x: len(x) > 2, word_array1))
		# word_array2 = list(filter(lambda x: len(x) > 2, word_array2))

		# print(word_array1)
		# print(word_array2)

		sub = abs(len(word_array1) - len(word_array2))
		# print(sub)
		maxWords = max(len(word_array1), len(word_array2))
		rate = 100 / float(maxWords) * (maxWords - sub)
		# print(rate)
		return rate


	def splitSentences(self):
		for par in self.sentences_pair:
			par['keywords1'] = []
			if 'first' in par.keys():
				cleared_sentence = self.onlyLetters.sub('', par['first'])
				word_array = cleared_sentence.split(' ')
				keyWords = self.getNameWords.findall(par['first'])
				keyWords = list(filter(lambda x: len(x) > 2, keyWords))
				keyWords.append(word_array[-1])
				# print(keyWords)
				if len(keyWords) > 0:
					for i in range(0, len(keyWords)):
						res = self.getTranslate(keyWords[i], file1_lang, self.file2_lang)
						# print(res)
						# print('^')
						if res != None:
							par['keywords1'].append(res)                    

		j = 0
		i = 0
		sync = True
		offset = 0
		reverse = False
		offsetoffset = 0
		nonsynced_count = 0
		ns_sentence1_size = 0
		ns_sentence2_size = 0
		j_changed = True
		i_changed = True
		while i < len(self.sentences_pair) and j < len(self.sentences_pair):
			if 'first' in self.sentences_pair[i].keys() and 'second' in self.sentences_pair[j].keys():
				
				if i_changed:
					self.nonsynced_sentences['first'].append(self.sentences_pair[i]['first'])
					self.nonsynced_sentences['keywords'].extend(self.sentences_pair[i]['keywords1'])
					ns_sentence1_size += self.getSentenceSize(self.sentences_pair[i]['first'])
				if j_changed:
					self.nonsynced_sentences['second'].append(self.sentences_pair[j]['second'])
					ns_sentence2_size += self.getSentenceSize(self.sentences_pair[j]['second'])
				i_changed = False
				j_changed = False

				rate = 0.0
				rate += self.getSentenceSub(self.sentences_pair[i]['first'], self.sentences_pair[j]['second'])
				print('//---------------------------')
				print('Length 1: ', ns_sentence1_size)
				print('Length 2: ', ns_sentence2_size)
				print('Rate 1:', rate)
				if len(self.sentences_pair[i]['keywords1']) > 0:
					rate1 = getSentenceRate(self.sentences_pair[i]['keywords1'], self.sentences_pair[j]['second'])
					
					print('Rate 2:', rate1)
					rate += rate1
					rate /= 2
				print(self.sentences_pair[i]['first'], self.sentences_pair[j]['second'])
				print('Rate: ', rate)
				print('//-----------------------------')
				if rate < self.MIN_RATE:
					sync = False
					if ns_sentence1_size <= ns_sentence2_size:
						i_changed = True
						i += 1
					else:
						j_changed = True
						j += 1
					continue
				else:
					self.synced_sentences.append({'first': ' '.join(self.nonsynced_sentences['first']), 'second': ' '.join(self.nonsynced_sentences['second'])})
					self.nonsynced_sentences['first'] = []
					self.nonsynced_sentences['second'] = []
					self.nonsynced_sentences['keywords'] = []
					ns_sentence1_size = 0
					ns_sentence2_size = 0

					j += 1
					i += 1
					i_changed = True
					j_changed = True
			else:
				while i < len(self.sentences_pair) and 'first' in self.sentences_pair[i].keys():
					self.nonsynced_sentences['first'].append(self.sentences_pair[i]['first'])
					i += 1
				while j < len(self.sentences_pair) and 'second' in self.sentences_pair[j].keys():
					self.nonsynced_sentences['second'].append(self.sentences_pair[j]['second'])
					j += 1
				break

		if len(self.nonsynced_sentences['first']) > 0:
			self.synced_sentences.append({'first': ''.join(self.nonsynced_sentences['first']), 'second': ''.join(self.nonsynced_sentences['second'])})



# for par in synced_sentences:
# 	print('/-----------------------------------')
# 	print(par['first'])
# 	print(par['second'])