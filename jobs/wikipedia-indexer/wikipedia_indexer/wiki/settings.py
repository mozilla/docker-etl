SUGGEST_MAPPING = {
  "dynamic": False,
  "properties": {
    "batch_id": {
      "type": "long"
    },
    "doc_id": {
      "type": "keyword"
    },
    "title": {
        "type": "keyword"
    },
    "suggest": {
      "type": "completion",
      "analyzer": "plain",
      "search_analyzer": "plain_search",
      "preserve_separators": True,
      "preserve_position_increments": True,
      "max_input_length": 255
    },
    "suggest-stop": {
      "type": "completion",
      "analyzer": "stop_analyzer",
      "search_analyzer": "stop_analyzer_search",
      "preserve_separators": False,
      "preserve_position_increments": False,
      "max_input_length": 255
    }
  }
}


SUGGEST_SETTINGS = {
  "number_of_replicas": "2",
  "refresh_interval": "-1",
  "number_of_shards": "8",
  "analysis": {
    "filter": {
      "stop_filter": {
        "type": "stop",
        "remove_trailing": "true",
        "stopwords": "_english_"
      },
      "token_limit": {
        "type": "limit",
        "max_token_count": "20"
      },
      "lowercase": {
        "name": "nfkc_cf",
        "type": "icu_normalizer"
      },
      "remove_empty": {
        "type": "length",
        "min": "1"
      },
      "accentfolding": {
        "type": "icu_folding"
      }
    },
    "analyzer": {
      "stop_analyzer": {
        "filter": [
          "icu_normalizer",
          "stop_filter",
          "accentfolding",
          "remove_empty",
          "token_limit"
        ],
        "type": "custom",
        "tokenizer": "standard"
      },
      "plain_search": {
        "filter": [
          "remove_empty",
          "token_limit",
          "lowercase"
        ],
        "char_filter": [
          "word_break_helper"
        ],
        "type": "custom",
        "tokenizer": "whitespace"
      },
      "plain": {
        "filter": [
          "remove_empty",
          "token_limit",
          "lowercase"
        ],
        "char_filter": [
          "word_break_helper"
        ],
        "type": "custom",
        "tokenizer": "whitespace"
      },
      "stop_analyzer_search": {
        "filter": [
          "icu_normalizer",
          "accentfolding",
          "remove_empty",
          "token_limit"
        ],
        "type": "custom",
        "tokenizer": "standard"
      }
    },
    "char_filter": {
      "word_break_helper": {
        "type": "mapping",
        "mappings": [
          "_=>\\u0020",
          ",=>\\u0020",
          "\"=>\\u0020",
          "-=>\\u0020",
          "'=>\\u0020",
          "\\u2019=>\\u0020",
          "\\u02BC=>\\u0020",
          ";=>\\u0020",
          "\\[=>\\u0020",
          "\\]=>\\u0020",
          "{=>\\u0020",
          "}=>\\u0020",
          "\\\\=>\\u0020",
          "\\u00a0=>\\u0020",
          "\\u1680=>\\u0020",
          "\\u180e=>\\u0020",
          "\\u2000=>\\u0020",
          "\\u2001=>\\u0020",
          "\\u2002=>\\u0020",
          "\\u2003=>\\u0020",
          "\\u2004=>\\u0020",
          "\\u2005=>\\u0020",
          "\\u2006=>\\u0020",
          "\\u2007=>\\u0020",
          "\\u2008=>\\u0020",
          "\\u2009=>\\u0020",
          "\\u200a=>\\u0020",
          "\\u200b=>\\u0020",
          "\\u200c=>\\u0020",
          "\\u200d=>\\u0020",
          "\\u202f=>\\u0020",
          "\\u205f=>\\u0020",
          "\\u3000=>\\u0020",
          "\\ufeff=>\\u0020"
        ]
      }
    }
  }
}
