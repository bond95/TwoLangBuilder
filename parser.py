import sys
import re
from elasticsearch import Elasticsearch
from yandex_translate import YandexTranslate
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


es = Elasticsearch()

translate = YandexTranslate('trnsl.1.1.20161120T092429Z.26536300f1fab524.bb5fcc81494303125cb46f52681dfdcf652477e0')

file1 = open(sys.argv[1])
file2 = open(sys.argv[2])

file1_lang = 'en'
file2_lang = 'ru'

paragraphLinks = []

withoutWhiteChars = re.compile(r'[\n\t \r]+')

def firstPhase(pars1, pars2):
    for i in range(0, len(par1)):
        par1[i] = withoutWhiteChars.sub(' ', par1[i])
        paragraphLinks.append({'first': par1[i]})
    for i in range(0, len(par2)):
        if i == len(paragraphLinks):
            par2[i] = withoutWhiteChars.sub(' ', par2[i])
            paragraphLinks.append({'second': par2[i]})
        else:
            par2[i] = withoutWhiteChars.sub(' ', par2[i])
            paragraphLinks[i]['second'] = par2[i]

nonAlphaNumeric = re.compile(r'^\W+$')


def parSplit(file_lines):
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
                if nonAlphaNumeric.match(file_lines[i]) == None:
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

par1 = parSplit(file1.readlines())
par2 = parSplit(file2.readlines())
firstPhase(par1, par2)

endOfSetnence = re.compile(r'([\.\?\!:])')

# def secondPhase():


def getSentence(par):
    for pars in paragraphLinks:
        if par in pars.keys():
            # print('/-----------------------')
            sentences = endOfSetnence.split(pars[par])
            sentences_parsed = []
            newSentence = True
            curr_len = 0
            additional = ''
            brace_opened = False
            # print('/-----------------------')
            # print(sentences)
            for sentence in sentences:
                if nonAlphaNumeric.match(sentence) != None:
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

sentences_pair = []

def secondPhase():
    getSentence('first')
    getSentence('second')
    sentence_count = []
    for par in paragraphLinks:
        if 'first_sentences' in par.keys():
            sentence_count.append(len(par['first_sentences']))
            for sent in par['first_sentences']:
                sentences_pair.append({'first': sent})

    offset = 0
    secondOffset = 0
    paragraphId = 0
    maxOffset = sentence_count[0]
    for par in paragraphLinks:
        if 'second_sentences' in par.keys():
            for sent in par['second_sentences']:

                if len(sentences_pair) > offset:
                    sentences_pair[offset]['second'] = sent
                    curr_par = paragraphLinks[paragraphId]
                    if not 'second_phase' in curr_par.keys():
                        curr_par['second_phase'] = []
                    curr_par[
                        'second_phase'].append(sentences_pair[offset])
                else:
                    sentences_pair.append({'second': sent})
                offset += 1
                secondOffset += 1
                if maxOffset == secondOffset:
                    if (len(sentence_count) - 1) <= paragraphId:
                        maxOffset = len(par['second_sentences'])
                    else:
                        secondOffset = 0
                        paragraphId += 1
                        maxOffset = sentence_count[paragraphId]

    # for par in paragraphLinks:
    #   print('END OF PARAGRAPH/---------------------------------------------')
    #   if 'second_phase' in par.keys():
    #       for sent in par['second_phase']:
    #           if 'first' in sent.keys():
    #               print('/--------------------------')
    #               print(sent['first'])
    #           if 'second' in sent.keys():
    #               print('/--------------------------')
    #               print(sent['second'])


secondPhase()

getNameWords = re.compile(r'([A-ZА-Я]\w+)')

def thirdPhase():
    for par in paragraphLinks:
        if 'second_phase' in par.keys():
            for sent in par['second_phase']:
                if 'first' in sent.keys():
                    print(getNameWords.findall(sent['first']))
                if 'second' in sent.keys():
                    print('/--------------------------')
                    print(getNameWords.findall(sent['second']))

def updateTranslateElastica(data_id, lang, word):
    insert_body = {'doc': {}}
    insert_body['doc']['word_' + lang] = word
    resp = es.update(index='languages', doc_type='translates', id=data_id, body=insert_body)
    if resp['result'] != 'updated':
        print(resp)

def addWordToElasticaOnlyIfTranslateExists(word_not_translated, word_translated, lang1, lang2):
    body = {"query": {'bool': {'must': [{'match': {}}]}}}
    body['query']['bool']['must'][0]['match']['word_' + lang2] = word_translated
    print(body)
    res = es.search(index="languages", doc_type="translates", body=body)
    if res['hits']['total'] == 0:
        insert_body = {}
        insert_body['word_' + lang1] = word_not_translated
        insert_body['word_' + lang2] = word_translated
        resp = es.index(index='languages', doc_type='translates', body=insert_body)
        if resp['result'] != 'created':
            print('Word ' + word_translated + ' was not inserted')
    else:
        data_id = res['hits']['hits'][0]['_id']
        updateTranslateElastica(data_id, lang1, word_not_translated)

def getTranslate(word, lang1, lang2):
    body = {"query": {'bool': {'must': [{'match': {}}]}}}
    body['query']['bool']['must'][0]['match']['word_' + lang1] = word
    res = es.search(index="languages", doc_type="translates", body=body)
    if res['hits']['total'] == 0:
        translate_res = translate.translate(word, lang1 + '-' + lang2)
        if translate_res['code'] == 200:
            addWordToElasticaOnlyIfTranslateExists(word, translate_res['text'][0], lang1, lang2)
            # print(translate_res['text'])
            return translate_res['text'][0]
        else:
            print(translate_res)
            return None
    else:
        if not 'word_' + lang2 in res['hits']['hits'][0]['_source'].keys():
            data_id = res['hits']['hits'][0]['_id']
            translate_res = translate.translate(word, lang1 + '-' + lang2)
            if translate_res['code'] == 200:
                updateTranslateElastica(data_id, lang2, translate_res['text'][0])
                return translate_res['text'][0]
            else:
                print(translate_res)
                return None
        else:
            # print(res)
            return res['hits']['hits'][0]['_source']['word_' + lang2]

onlyLetters = re.compile(r'[^a-zA-Zа-яА-Я ]+')

MIN_RATE = 75

def getSentenceRate(keyWords, sentence):
    cleared_sentence = onlyLetters.sub('', sentence)
    word_array = cleared_sentence.split(' ')
    rate = 0.0
    # print('getSentenceRate')
    # print(keyWords)
    # print(sentence)
    for word in keyWords:
        res = process.extractOne(word, word_array)
        if res != None:
            if res[1] > MIN_RATE:
                rate += res[1]
                word_array.remove(res[0])

    rate = rate / len(keyWords)
    return rate


def getSentenceSub(sentence1, sentence2):
    cleared_sentence1 = onlyLetters.sub('', sentence1)
    cleared_sentence2 = onlyLetters.sub('', sentence2)

    word_array1 = cleared_sentence1.split(' ')
    word_array2 = cleared_sentence2.split(' ')
    word_array1 = list(filter(lambda x: len(x) > 2, word_array1))
    word_array2 = list(filter(lambda x: len(x) > 2, word_array2))

    # print(word_array1)
    # print(word_array2)

    sub = abs(len(word_array1) - len(word_array2))
    # print(sub)
    maxWords = max(len(word_array1), len(word_array2))
    rate = 100 / float(maxWords) * (maxWords - sub)
    # print(rate)
    return rate

synced_sentences = []
nonsynced_sentences = {'first': [], 'second': []}

def splitSentences():
    for par in sentences_pair:
        par['keywords1'] = []
        if 'first' in par.keys():
            keyWords = getNameWords.findall(par['first'])
            keyWords = list(filter(lambda x: len(x) > 2, keyWords))
            # print(keyWords)
            if len(keyWords) > 0:
                for i in range(0, len(keyWords)):
                    res = getTranslate(keyWords[i], file1_lang, file2_lang)
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
    while i < len(sentences_pair) and j < len(sentences_pair):
        if 'first' in sentences_pair[i].keys():
            rate = 0.0
            rate += getSentenceSub(sentences_pair[i]['first'], sentences_pair[j]['second'])
            # print('Rate 1:', rate)
            if len(sentences_pair[i]['keywords1']) > 0:
                rate1 = getSentenceRate(sentences_pair[i]['keywords1'], sentences_pair[j]['second'])
                
                # print('Rate 2:', rate1)
                rate += rate1
                rate /= 2
            # print(sentences_pair[i]['first'], sentences_pair[j]['second'])
            # print('Rate: ', rate)
            if rate < MIN_RATE:
                if sync == True:
                    nonsynced_sentences['first'].append(sentences_pair[i]['first'])
                sync = False
                if offset >= 5:
                    reverse = True
                if reverse:
                    nonsynced_sentences['first'].append(sentences_pair[i]['first'])
                    i += 1
                    offset = 0
                else:
                    nonsynced_sentences['second'].append(sentences_pair[j]['second'])
                    j += 1
                    offset += 1
                continue
            else:
                if sync == False and offset > 0:
                    reverse = True
                    offset = 0
                    if j + 1 == len(sentences_pair) or not 'second' in sentences_pair[j + 1]:
                        break
                    nonsynced_sentences['second'].append(sentences_pair[j + 1]['second'])
                else:
                    if reverse:
                        synced_sentences.append({'first': ''.join(nonsynced_sentences['first']), 'second': ''.join(nonsynced_sentences['second'])})
                        nonsynced_sentences['first'] = []
                        nonsynced_sentences['second'] = []
                    else:
                        synced_sentences.append({'first': sentences_pair[i]['first'], 'second': sentences_pair[j]['second']})
                    reverse = False
                    sync = True
                j += 1
                i += 1
        else:
            break

    if len(nonsynced_sentences['first']) > 0:
        synced_sentences.append({'first': ''.join(nonsynced_sentences['first']), 'second': ''.join(nonsynced_sentences['second'])})


# thirdPhase()
splitSentences()

for par in synced_sentences:
    print('/-----------------------------------')
    print(par['first'])
    print(par['second'])