from elasticsearch import Elasticsearch
es = Elasticsearch('http://101.43.201.238:9200')

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index='test-index', ignore=400)

# ignore 404 and 400
es.indices.delete(index='test-index', ignore=[400, 404])


print("12", "23", "q")