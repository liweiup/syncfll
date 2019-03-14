#!/usr/bin/python
#coding=utf-8

from elasticsearch import Elasticsearch, helpers
from elasticsearch import RequestError, ConnectionError, ConflictError, NotFoundError
from elasticsearch_dsl import Search, Q

import logging
logger = logging.getLogger()


class ElasticSearch():
    #初始化连接
    def __init__(self,host,port=9200):
        self.host = host
        self.port = port
        self.es = Elasticsearch("%s:%d" % (host,port))
    #创建索引
    def createIndex(self, index_name):
        return self.es.indices.create(index = index_name, ignore=[400, 404])
    #插入一条数据
    def insert(self, index_name, doc_type, doc_body):
        return self.es.index(index = index_name, doc_type = doc_type, body = doc_body)
    def get(self, index_name, doc_type, id):
        return self.es.get(index = index_name, doc_type = doc_type, id = id)
    def search(self, index_name, doc_body):
        return self.es.search(index = index_name, body=doc_body)
    def mulitInsert(self, action):
        try:
            result = helpers.bulk(self.es, action, stats_only= True)
        except ConnectionError, error:
            logger.error("connect failed,%s" % error)
            return 0
        # except ConflictError, error:
            # logger.error("Exception representing a 409 status code,%s" % error)
        # except NotFoundError, error:
            # logger.error("Exception representing a 404 status code,%s" % error)
        # except RequestError, error:
            # logger.error("Exception representing a 400 status code,%s" % error)
        except Exception, error:
            logger.error("indexing faild,%s" % error)
        else:
            logger.info("indexing %s rows ok" % result[0])
        return 1
   
    
    
if __name__ == "__main__":
    pass