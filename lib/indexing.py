#! /usr/bin/env python
#coding=utf-8

#standard 标准库
import os
import sys
import logging
import json
import time
import re

#extended module 第三方扩展库


#defined #自定义库
import const
import log
import searcher
import mdb
import utils
import rdb

reload(sys)
sys.setdefaultencoding('utf8')





#检测redis中是否有ip ua 的hash记录
def check_ip_ua(reqIpUserAgentHash):
    key = "reqIpUserAgentHash"
    if rs_repeat.sadd(key, reqIpUserAgentHash) == 0:
        logger.debug("%s is old" % reqIpUserAgentHash)
        return False
    else:
        logger.info("%s is new" % reqIpUserAgentHash)
        return True
    
    
    

#计算页面停留时间
def time_on_page(reqTime, productChannel,productPageCode, userId):
    """
    reqTime 页面访问时间
    productChannel 产品入口
    productPageCode 页面编号
    userId 用户ID
    """
    key = "timeOnPage"
    field = str(userId) + ":" + str(productChannel)
    value = json.dumps({"productPageCode": productPageCode,"reqTime":reqTime }, ensure_ascii=False)
    #获取用户上一次浏览页面
    last_view_page = rs_repeat.hget(key, field)
    if last_view_page:
        last_view_page_info = json.loads(last_view_page, strict=False)
        last_view_reqTime = last_view_page_info["reqTime"]
        last_view_productPageCode = last_view_page_info["productPageCode"]
        #计算出上一次访问的停留时间
        wyq_top_log = {"userId": userId,
               "productChannel": productChannel,
               "productPageCode": last_view_productPageCode,
               "reqTime": last_view_reqTime,
               "timeOnPage": abs(utils.time_diff(last_view_reqTime, reqTime)),
               }
        #丢到redis的 wyq_top_log队列中
        if rs_queue.rpush("wyq_top_log", json.dumps(wyq_top_log, ensure_ascii=False)):
            logger.info("rpush wyq_top_log in redis ok") 
            
        #redis中记录最新一次访问信息(覆盖原来的记录)
        if rs_repeat.hset(key, field, value) == 0:
            logger.info( "cover %s ok" % field)
        
    else:
        logger.info( ("%s not in redis:%s" % (field, key)))
        if rs_repeat.hset(key, field, value) == 1:
            logger.info( "hset new %s ok" % field)




#从redis 获取多条日志
def get_action_list(threshold_time = 5, threshold_count = 1000 ):
    """
    @threshold_time  多少秒后返回结果
    @threshold_count 累积多少条数据后返回结果
    """
    action_list = []
    start_time = time.time()
    count = 0
    while True:
        if count >= threshold_count:
            return action_list
        if time.time() - start_time >= threshold_time:
            return action_list
        log = rs_queue.lpop(const.REDIS_QUEUE_KEY)
        if log:
            try:
                logger.info(log)
                jsonlog = json.loads(log,strict=False)
                message = jsonlog["message"]
                path = jsonlog["path"]
                host = jsonlog["host"]
                real_log = re.findall(r"\{.*\}$", message)[0]
                op_log = json.loads(real_log,strict=False)
                #错误日志格式处理
                if op_log.has_key("operateAfterObj"):
                    operateAfterObj = op_log["operateAfterObj"]
                    logger.info(type(operateAfterObj))
                    if  operateAfterObj  and type(operateAfterObj) != dict:
                        logger.warning("operateAfterObj is not a dict object")
                        op_log["operateAfterObj"] = json.loads(operateAfterObj,strict=False)
                ##################开始日志处理###########
                reqTime = op_log["reqTime"]
                #统计页面停留
                try:
                    productChannel = op_log["productChannel"]
                    productPageCode = op_log["productPageCode"]
                    userId = op_log["user"]["userId"]
                    if productPageCode and  userId and  productChannel and  reqTime:
                        time_on_page(reqTime, productChannel, productPageCode, userId)
                    else:
                        logger.warning("can't stat time on page reason: Incomplete information ")
                except Exception, error:
                    logger.warning("can't stat time on page reason: %s" % error)
                #
                reqIp = op_log["reqIp"]
                reqUA = op_log["reqUA"]
                if reqIp:
                    province_city = utils.ip_parse(reqIp)
                    op_log["reqProv"] = province_city["province"]
                    op_log["reqCity"] = province_city["city"]
                    if reqUA:
                        reqIpUserAgentHash = utils.md5(reqIp + reqUA)
                        op_log["reqIpUserAgentHash"] = reqIpUserAgentHash
                        op_log["reqIpUserAgentNew"] = check_ip_ua(reqIpUserAgentHash)
                else:
                    op_log["reqIp"]="0.0.0.0"
                op_log["fromLogPath"] = path
                op_log["fromLogHost"] = host
                _index = "wyq_op_log_" + reqTime[0:7]
                action_list.append({
                "_index": _index,
                "_type": "all",
                "_source":op_log
                })
                count += 1
            except Exception, error:
                logger.error("parse error:%s=> %s" % (error, log))
            else:
                logger.debug(op_log)
        else:
            logger.info("no new data in queue")
            time.sleep(1)






#
def wyq_op_log_indexing():
    global rs_queue, rs_repeat, logger
    #初始化日志
    log.init()
    logger = logging.getLogger()
    # log.init(os.path.join(const.LOGDIR,"wyqOperateLogIndexing.log"))
    # logger = logging.getLogger()
    es = searcher.ElasticSearch(const.ES_HOST, const.ES_PORT)  #elasticsearch
    rs_queue = rdb.Redis(const.REDIS_QUEUE_HOST, const.REDIS_QUEUE_PORT)  #redis队列
    rs_repeat = rdb.Redis(const.REDIS_REPEAT_HOST, const.REDIS_REPEAT_PORT)  #redis去重库
    while True:
        action_list = get_action_list()
        if action_list:
            logger.info("get %d rows from queue"%len(action_list))
            while True:
                if es.mulitInsert(action_list):
                    break
                else:
                    logger.error(action_list)
                    time.sleep(2)






#从redis队列获取多条页面停留时间的日志
def get_top_action_list(threshold_time = 5, threshold_count = 1000 ):
    """
    @threshold_time  多少秒后返回结果
    @threshold_count 累积多少条数据后返回结果
    """
    action_list = []
    start_time = time.time()
    count = 0
    while True:
        if count >= threshold_count:
            return action_list
        if time.time() - start_time >= threshold_time:
            return action_list
        log = rs_queue.lpop(const.REDIS_TOP_LOG_QUEUE_KEY)
        if log:
            try:
                logger.info(log)
                top_log = json.loads(log,strict=False)
                ##################开始日志处理###########
                reqTime = top_log["reqTime"]
                # productChannel = top_log["productChannel"]
                # productPageCode = top_log["productPageCode"]
                # timeOnPage = top_log["timeOnPage"]

                action_list.append({
                "_index": "wyq_top_log_" + reqTime[0:7],
                "_type": "all",
                "_source":top_log
                })
                count += 1
            except Exception, error:
                logger.error("parse error:%s=> %s" % (error, log))
            else:
                logger.debug(top_log)
        else:
            logger.info("no new data in queue")
            time.sleep(1)













#页面停留时间索引
def wyq_top_log_indexing():
    global rs_queue, logger
    #初始化日志
    log.init(const.TOP_LOG_INDEXING_LOGFILE)
    logger = logging.getLogger()
    es = searcher.ElasticSearch(const.ES_HOST, const.ES_PORT)  #elasticsearch
    rs_queue = rdb.Redis(const.REDIS_QUEUE_HOST, const.REDIS_QUEUE_PORT)  #redis队列
    while True:
        action_list = get_top_action_list()
        if action_list:
            logger.info("get %d rows from queue"%len(action_list))
            while True:
                if es.mulitInsert(action_list):
                    break
                else:
                    logger.error(action_list)
                    time.sleep(2)


















#
if __name__ == "__main__":
    wyq_top_log_indexing()
    pass
    #rs_repeat = rdb.Redis(const.REDIS_REPEAT_HOST, const.REDIS_REPEAT_PORT)
    #time_on_page("2016-08-26 11:11:39", "2",1002, "363671811")
    # indexing()
