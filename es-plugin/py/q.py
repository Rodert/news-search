import random
import base64
import numpy as np
from elasticsearch import Elasticsearch, helpers

dbig = np.dtype('>f8')

es = Elasticsearch('http://101.43.201.238:9200')

body = {
    "mappings": {
        "image_search": {
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "feature": {
                    "type": "binary",
                    "doc_values": True
                }
            }
        }
    }
}

index = 'test'
es.indices.delete(index=index, ignore=404)
es.indices.create(index=index, ignore=400, body=body)


def decode_float_list(base64_string):
    """
    base64 转 list
    :param base64_string:
    :return:
    """
    bytes_ = base64.b64decode(base64_string)
    return np.frombuffer(bytes_, dtype=dbig).tolist()


def encode_array(arr):
    """
    List 转 base64
    :param arr:
    :return:
    """
    base64_str = base64.b64encode(np.array(arr).astype(dbig)).decode("utf-8")
    return base64_str


def generator():
    i = 0
    while True:
        yield {

                'id': i,
                'feature': encode_array([random.random(), random.random()])
            }
        i += 1
        if i >= 50000:
            break


# 批量插入100w数据到es
helpers.bulk(es, generator(), index=index, doc_type='_doc')
