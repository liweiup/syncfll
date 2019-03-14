#!/usr/bin/env python
#coding=utf-8

import redis
import logging
import const
import time
logger=logging.getLogger()


class Redis():
    """
    @host  redis ip
    @port  redis port
    """
    #初始化连接
    def __init__(self,host,port=6379):
        self.host = host
        self.port = port
        #连接Hbase
        while True:
            try:
                pool = redis.ConnectionPool(host = self.host, port = self.port, db=0)
                self.client = redis.Redis(connection_pool=pool)
                break
            except Exception,error:
                logger.error("redis connect error:%s" % error)
                time.sleep(3)
    #关闭连接
    def __del__(self):
        pass  #redis会自动关闭
    #将一个或多个值插入到列表的尾部(最右边)
    def rpush(self, key, value):
        try:
            return self.client.rpush(key, value)
        except Exception,error:
            logger.error("rpush error,%s" % error)
    #移除并返回列表的第一个元素
    def lpop(self, key):
        try:
            return self.client.lpop(key)
        except Exception,error:
            logger.error("lpop error,%s" % error)
    #向集合添加一个成员活多个成员,返回成功写入数如已存在则不会重复写入
    def sadd(self, key, member):
        try:
            if type(member) is list:
                return self.client.sadd(key,*member)
            else:
                return self.client.sadd(key,member)
        except Exception,error:
            logger.error("lpop error,%s" % error)
    def set(self, key, value):
        try:
            return self.client.set(key,value)
        except Exception,error:
            logger.error("set error,%s" % error)
    def get(self, key, value):
        try:
            return self.client.get(key,value)
        except Exception,error:
            logger.error("get error,%s" % error)
    def hset(self, key, field, value):
        """
        返回整数
        1 如果字段是哈希值和一个新字段被设置
        0 如果字段已经存在于哈希并且值被更新
        """
        try:
            return self.client.hset(key,field, value)
        except Exception,error:
            logger.error("hset error,%s" % error)
    def hget(self, key, field):
        try:
            return self.client.hget(key,field)
        except Exception,error:
            logger.error("get error,%s" % error)
#
def test():
    pass
    #r = Redis(const.REDIS_REPEAT_HOST, const.REDIS_REPEAT_PORT)
    #print r.rpush("mylist", 123)
    #print r.sadd("testset", "999")
    

if __name__ == "__main__":
    test()