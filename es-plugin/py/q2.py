import time
import json
import base64
import numpy as np
from elasticsearch import Elasticsearch, helpers

dbig = np.dtype('>f8')

es = Elasticsearch('http://101.43.201.238:9200')

body = {
    "from": 0,
    "size": 5,
    "_source": {
        "excludes": ""
    },
    "sort": {
        "_score": {
            "order": "asc"
        }
    },
    "query": {
        "function_score": {
            "query": {
                "match_all": {}
            },
            "functions": [
                {
                    "script_score": {
                        "script": {
                            "source": "DebugWorld",
                            "lang": "ImageSimilarity",
                            "params": {
                                "field": "feature",
                                "feature": [0.01, 0.03]
                            }
                        }
                    }
                }
            ]
        }
    }
}


def decode_float_list(base64_string):
    """
    base64 转 list
    :param base64_string:
    :return:
    """
    bytes_ = base64.b64decode(base64_string)
    return np.frombuffer(bytes_, dtype=dbig).tolist()


time_list = list()
for i in range(1):
    start_time = time.time()
    result = es.search(index='test', doc_type='image_search', body=body)
    for hit in result['hits']['hits']:
        hit['_source']['feature'] = decode_float_list(hit['_source']['feature'])
    time_list.append(time.time() - start_time)
    print(json.dumps(result, indent=4))


print(sum(time_list)/len(time_list), max(time_list), min(time_list))