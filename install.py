from elasticsearch import Elasticsearch

es = Elasticsearch()

configuration = {
    'settings': {
        'analysis': {
            'filter': {
                'desc_ngram': {
                    'type': 'nGram',
                    'min_gram': 3,
                    'max_gram': 8
                }
            },
            'analyzer': {
                'index_ngram': {
                    'type': 'custom',
                    'tokenizer': 'keyword',
                    'filter': [ 'desc_ngram', 'lowercase' ]
                },
                'search_ngram': {
                    'type': 'custom',
                    'tokenizer': 'keyword',
                    'filter': [ 'lowercase', 'desc_ngram' ]
                }
            }
        }
    },
    'mappings': {
        'translates': {
            'properties': {
                'word_en': {
                    'type': 'string',
                    'analyzer': 'index_ngram',
                    'search_analyzer': 'search_ngram'
                },
                'word_ru': {
                    'type': 'string',
                    'analyzer': 'index_ngram',
                    'search_analyzer': 'search_ngram'
                }
            }
        },
    }
}

es.indices.create(index='languages', body=configuration)