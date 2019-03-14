#! /usr/bin/env python
#coding=utf-8
#python2.6

#标准库
import sys
import os
import json
import commands
import logging
import time

#第三方库


#自定义库
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'lib'))

import const
import log
import mdb

import smtp
# import phonesms

#
reload(sys)
sys.setdefaultencoding('utf8')

rootdir=const.ROOTDIR
tmpdir=const.TMPDIR
ropdir=const.ROPDIR
datadir=const.DATADIR
logdir=const.LOGDIR

#logfile = const.LOGFILE
logfile = logdir + '/' + __file__.split("/")[-1].split('.')[0] + '.log'
pidfile = const.PIDFILE
dbdict = const.DBDICT

#logger
log.init(logfile,level=logging.INFO)
logger=logging.getLogger()

#time
TDate=time.strftime("%Y-%m-%d",time.localtime(time.time()))
YDate=time.strftime("%Y-%m-%d",time.localtime(time.time()-1*24*60*60))
TDate_lite=time.strftime("%Y%m%d",time.localtime(time.time()))
YDate_lite=time.strftime("%Y%m%d",time.localtime(time.time()-1*24*60*60))


# 百度百科明星人气榜--将从爬虫库获取的数据写入到产品库
def starRank1(env,dbdict,aType='1',dType='1'):
    '''
    aType:爬虫type类型
    dType:data_type(1:实时数据 2:周榜数据 3:月榜数据)
    dst_table:tb_star_baiduflower
    '''

    #关键数据
    proname = "百度百科明星人气榜"
    src_table = "iis_web_crawler188"
    dst_table = "tb_star_baiduflower" #产品目标表
    data_type = dType
    #用于第一名
    firstName = "第一名"

    MCRefer = {"starName":"starname",
               "flowernum":"flowernum",
               "rankNumber":"ranknum",
               "capture_time":"catchtime",}


    db_host=dbdict['dstFll']['db_host']
    db_name=dbdict['dstFll']['db_name']
    db_user=dbdict['dstFll']['db_user']
    db_passwd=dbdict['dstFll']['db_password']
    db_port=dbdict['dstFll']['db_port']
    db_table=dbdict['dstFll']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)


    #获取指定类型（aType）的明星名
    '''
    #明星状态0不存在1存在
    #0 爬虫库中未找到对应的明星
    #1 爬虫库中找到对应的明星
    {starName1:0,
     starName2:1,}
    #otherName_starName_dict
    {otherName1:starName1,
     otherName2:starName2,}
    '''
    starName_dict = {} #明星状态，明星名用的昵称；
    otherName_starName_dict = {} #明星与otherName对应表
    starSql = """select starName,other_name from tb_star where add_type=1;"""
    starResults=db.queryAll(starSql)
    if len(starResults):
        for d in starResults:
            starName = d["starName"]
            otherName = d["other_name"]
            starName_dict[starName] = 0
            if otherName:
                otherName_starName_dict[otherName] = starName
    else:
        logger.info("""库：%s 类型：%s 明星数为0."""%(db_host,aType))
        return False


    #根据明星列表将从爬虫库获取的数据插入到产品表
    starName_list = starName_dict.keys()
    otherName_list = otherName_starName_dict.keys()
    OrigData = getOrigData(env,dbdict,proname,aType)
    if OrigData:
        content = OrigData[0]
        rankList = eval(content)["rank"]
        for d in rankList:
            starName = d["starname"]
            rankNumber = d["ranknum"]
            if (starName in starName_list) or (starName in otherName_list):
                if starName in otherName_list:
                    starName = otherName_starName_dict[starName]
                if starName_dict[starName] == 1:
                    continue
                starName_dict[starName] = 1 #更改更新状态
                flowernum = d["flowernum"]
                capture_time = d["catchtime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,flowernum,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                   '%s','%s','%s','%s');"""%(dst_table,starName,flowernum,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',flowernum='%s',rankNumber=%s,data_type=%s,update_time='%s',capture_time='%s'
                                   where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,flowernum,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)

            #用于第一名
            if rankNumber == 1:
                starName = firstName
                flowernum = d["flowernum"]
                capture_time = d["catchtime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,flowernum,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                   '%s','%s','%s','%s');"""%(dst_table,starName,flowernum,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',flowernum='%s',rankNumber=%s,data_type=%s,update_time='%s',capture_time='%s'
                                   where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,flowernum,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)


    #将tb_star中有而爬虫重没有的明星置空
    for starName,status in starName_dict.items():
        if status == 0:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
            current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
            emptySql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
            emptyResults = db.queryAll(emptySql)
            if len(emptyResults) == 0 :
                eInsertSql = """INSERT INTO %s (starName,data_type,date,create_time) VALUES ('%s','%s','%s','%s');"""%(dst_table,starName,data_type,current_date,current_time)
                db.executeSql(eInsertSql)
                logger.info("未在表：%s 中找到明星：%s，%s 已插入默认值"%(src_table,starName,dst_table))
            else:
                logger.info("未在表：%s 中找到明星：%s，%s 已存在不操作"%(src_table,starName,dst_table))



#百度搜索风云榜--将从爬虫库获取的数据写入到产品库
def starRank2(env,dbdict,aType='2'):
    MCRefer = {"starName":"starname",
               "score":"searchnum",
               "rankNumber":"ranknum",
               "capture_time":"catchtime",}

    db_host=dbdict['dstFll']['db_host']
    db_name=dbdict['dstFll']['db_name']
    db_user=dbdict['dstFll']['db_user']
    db_passwd=dbdict['dstFll']['db_password']
    db_port=dbdict['dstFll']['db_port']
    db_table=dbdict['dstFll']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)
    #关键数据
    proname = "百度搜索风云榜"
    src_table = "iis_web_crawler188"
    dst_table = "tb_star_baidusearch"
    data_type = 1
    #用于第一名
    firstName = "第一名"

    #获取指定类型（aType）的明星名
    '''
    #0 爬虫库中未找到对应的明星
    #1 爬虫库中找到对应的明星
    {starName1:0,
     starName2:1}
    '''
    starName_dict = {} #明星状态，明星名用的昵称；
    starSql = """select starName from tb_star where add_type=1;"""
    starResults=db.queryAll(starSql)
    if len(starResults):
        for d in starResults:
            starName = d["starName"]
            starName_dict[starName] = 0
    else:
        logger.info("""库：%s 类型：%s 明星数为0."""%(db_host,aType))
        return False


    #根据明星列表将从爬虫库获取的数据插入到产品表
    starName_list = starName_dict.keys()
    OrigData = getOrigData(env,dbdict,proname,aType)
    if OrigData:
        content = OrigData[0]
        rankList = eval(content)["rank"]
        #根据searchnum重新排序-开始
        new_rankList_dict = {}
        new_rankList_list = []
        for d in rankList:
            searchnum = int(d['searchnum'])
            if new_rankList_dict.has_key(searchnum):
                repeat = new_rankList_dict[searchnum]['repeat']
                new_searchnum = searchnum + repeat*0.01
                d['repeat'] = repeat + 1
                new_rankList_dict[new_searchnum] = d
            else:
                d["repeat"] = 1
                new_rankList_dict[searchnum] = d
        n = 1
        for searchnum in sorted(new_rankList_dict.keys(), reverse=True):
            new_rankList_dict[searchnum]['ranknum'] = str(n)
            n+=1
        for searchnum in sorted(new_rankList_dict.keys(), reverse=True):
            new_rankList_list.append(new_rankList_dict[searchnum])
        #根据searchnum重新排序-结束
        for d in new_rankList_list:
            starName = d["starname"]
            rankNumber = d["ranknum"]
            if starName in starName_list:
                if starName_dict[starName] == 1:
                    continue
                starName_dict[starName] = 1 #更改更新状态
                score = d["searchnum"]
                capture_time = d["catchtime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,score,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                   '%s','%s','%s','%s');"""%(dst_table,starName,score,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',score='%s',rankNumber=%s,data_type='%s',update_time='%s',capture_time='%s'
                                   where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,score,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)

            #用于第一名
            if rankNumber == "1":
                starName = firstName
                score = d["searchnum"]
                capture_time = d["catchtime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,score,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                   '%s','%s','%s','%s');"""%(dst_table,starName,score,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',score='%s',rankNumber=%s,data_type='%s',update_time='%s',capture_time='%s'
                                   where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,score,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)


    #将tb_star中有而爬虫重没有的明星置空
    for starName,status in starName_dict.items():
        if status == 0:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
            current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
            emptySql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
            emptyResults = db.queryAll(emptySql)
            if len(emptyResults) == 0 :
                eInsertSql = """INSERT INTO %s (starName,data_type,date,create_time) VALUES ('%s','%s','%s','%s');"""%(dst_table,starName,data_type,current_date,current_time)
                db.executeSql(eInsertSql)
                logger.info("未在表：%s 中找到明星：%s，%s 已插入默认值"%(src_table,starName,dst_table))
            else:
                logger.info("未在表：%s 中找到明星：%s，%s 已存在不操作"%(src_table,starName,dst_table))




#寻艺榜--将从爬虫库获取的数据写入到产品库
def starRank3(env,dbdict, aType='3', dType='1'):
    '''
    aType:爬虫type类型
    dType:data_type(1:实时数据 2:周榜数据 3:月榜数据)
    dst_table:tb_star_xunyi
    '''
    #关键数据
    proname = "寻艺榜"
    src_table = "iis_web_crawler188"
    dst_table = "tb_star_xunyi" #产品目标表
    data_type = dType
    #用于第一名
    firstName = "第一名"

    #mysql和crawler字段对应
    MCRefer = {"starName":"starname",
               "score":"score",
               "rankNumber":"ranknum",
               "capture_time":"catchtime",}


    db_host=dbdict['dstFll']['db_host']
    db_name=dbdict['dstFll']['db_name']
    db_user=dbdict['dstFll']['db_user']
    db_passwd=dbdict['dstFll']['db_password']
    db_port=dbdict['dstFll']['db_port']
    db_table=dbdict['dstFll']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)

    #获取指定类型（aType）的明星名
    '''
    #明星状态0不存在1存在
    #0 爬虫库中未找到对应的明星
    #1 爬虫库中找到对应的明星
    {starName1:0,
     starName2:1,}
    #otherName_starName_dict
    {otherName1:starName1,
     otherName2:starName2,}
    '''
    starName_dict = {} #明星状态，明星名用的昵称；
    otherName_starName_dict = {} #明星与xunyiName对应表
    starSql = """select starName,xunyi_name from tb_star where add_type=1;"""
    starResults=db.queryAll(starSql)
    if len(starResults):
        for d in starResults:
            starName = d["starName"]
            otherName = d["xunyi_name"]
            starName_dict[starName] = 0
            if otherName:
                otherName_starName_dict[otherName] = starName
    else:
        logger.info("""库：%s 类型：%s 明星数为0."""%(db_host,aType))
        return False


    #根据明星列表将从爬虫库获取的数据插入到产品表
    starName_list = starName_dict.keys()
    otherName_list = otherName_starName_dict.keys()
    OrigData = getOrigData(env,dbdict,proname,aType)
    if OrigData:
        content = OrigData[0]
        rankList = eval(content)["rank"]
        for d in rankList:
            starName = d["starname"]
            rankNumber = d["ranknum"]
            if (starName in starName_list) or (starName in otherName_list):
                if starName in otherName_list:
                    starName = otherName_starName_dict[starName]
                if starName_dict[starName] == 1:
                    continue
                starName_dict[starName] = 1 #更改更新状态
                score = d["score"]
                capture_time = d["catchtime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,score,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                   '%s','%s','%s','%s');"""%(dst_table,starName,score,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',score='%s',rankNumber=%s,data_type='%s',update_time='%s',capture_time='%s'
                                   where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,score,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)

            #用于第一名
            if rankNumber == "1":
                starName = firstName
                score = d["score"]
                capture_time = d["catchtime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,score,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                   '%s','%s','%s','%s');"""%(dst_table,starName,score,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',score='%s',rankNumber=%s,data_type='%s',update_time='%s',capture_time='%s'
                                   where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,score,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)


    #将tb_star中有而爬虫重没有的明星置空
    for starName,status in starName_dict.items():
        if status == 0:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
            current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
            emptySql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
            emptyResults = db.queryAll(emptySql)
            if len(emptyResults) == 0 :
                eInsertSql = """INSERT INTO %s (starName,data_type,date,create_time) VALUES ('%s','%s','%s','%s');"""%(dst_table,starName,data_type,current_date,current_time)
                db.executeSql(eInsertSql)
                logger.info("未在表：%s 中找到明星：%s，%s 已插入默认值"%(src_table,starName,dst_table))
            else:
                logger.info("未在表：%s 中找到明星：%s，%s 已存在不操作"%(src_table,starName,dst_table))




#从爬虫库获取数据源
def getOrigData(env,dbdict,proname,aType=''):
    db_host=dbdict['srcWeb']['db_host']
    db_name=dbdict['srcWeb']['db_name']
    db_user=dbdict['srcWeb']['db_user']
    db_passwd=dbdict['srcWeb']['db_password']
    db_port=dbdict['srcWeb']['db_port']
    db_table=dbdict['srcWeb']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)

    sql = """select * from star_rank where createtime>='%s' and type=%s ORDER BY createtime desc limit 0,1;"""%(TDate,aType)
    #print sql
    results=db.queryAll(sql)

    if len(results):
        createtime = results[0]["createtime"]
        aType = results[0]["type"]
        content = results[0]["content"]
        result = [content,aType,createtime]
        ##test
        #current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
        #sms_body = "%s同步成功\n数据库:%s\n数据表:%s\n查询时间:%s"%(proname,db_name,db_table,current_time)
        #phoneNumber="15862183966,15727337093,15138931970,15221194570"
        #phonesms.sms(phoneNumber, sms_body)
        return result
    else:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
        mail_to = ["junyu@miduchina.com",
                   # "peilong@miduchina.com",
                   "baichun@miduchina.com",
                   "xuemei@miduchina.com",
                   # "gaojing@miduchina.com",
                   "liwei2@miduchina.com",
                   # "junjun@miduchina.com",
                   "huichen@miduchina.com",
                   "bowen@miduchina.com",
                   "junjie@miduchina.com"]
        mail_sub = "明星榜无数据预警-%s"%proname
        mail_body = "@All\n\n%s无源数据\n类型：%s\n数据库：%s\n数据表：%s\n查询时间：%s"%(proname,aType,db_name,db_table,current_time)
        ms = smtp.EmailSender("mail.miduchina.com", "opd@miduchina.com", "rXS06HrbMDif")
        ms.send(mail_to, mail_sub, mail_body)
        if env == "www":
            #短信通知
            sms_body = "%s无源数据\n数据库：%s\n数据表：%s\n查询时间：%s"%(proname,db_name,db_table,current_time)
            phoneNumber="15862183966,15727337093,18141906780,15138931970,15221194570,18501968603"
            #phoneNumber="15862183966,15727337093,15138931970,15221194570"
            phonesms.sms(phoneNumber, sms_body)
        logger.info("the content is empty db:%s,table:%s,type:%s"%(db_host,db_table,aType))
        return False


def help():
    print "%s '[1|2|3](aType)'" % sys.argv[0]
    sys.exit()


def get_args():
    if len(sys.argv) != 2:
        help()
    else:
        aType = sys.argv[1]
        return (aType,)


def main():
    args = get_args()
    aType = args[0]
    #aType爬虫类型：
    #实时更新：1（tb_star_baiduflower）,2（tb_star_baidusearch）,3（tb_star_xunyi）
    #周更新：4（tb_star_baiduflower）,5（tb_star_xunyi）

    #dType产品库类型
    #1:实时数据 2:周榜数据 3:月榜数据,

    for env in dbdict.keys():
        env_dbdict = dbdict[env]
        #实时榜
        if aType == '1':
            '''
            百度百科明星人气榜
            src:192.168.40.28|iis_web_crawler188|star_rank|(type:1)
            dst:192.168.20.101|super_fans_beta|tb_star_baiduflower|(data_type:1)
            '''
            starRank1(env,env_dbdict, aType='1', dType='1')
        elif aType == '2':
            '''
            百度搜索风云榜
            '''
            starRank2(env,env_dbdict, aType='2')
        elif aType == '3':
            '''
            寻艺榜
            src:192.168.40.28|iis_web_crawler188|star_rank|(type:3)
            dst:192.168.20.101|super_fans_beta|tb_star_xunyi|(data_type:1)
            '''
            starRank3(env,env_dbdict, aType='3', dType='1')


        #周榜
        elif aType == '4':
            '''
            百度百科明星人气榜
            src:192.168.40.28|iis_web_crawler188|star_rank|(type:4)
            dst:192.168.20.101|super_fans_beta|tb_star_baiduflower|(data_type:2)
            '''
            starRank1(env,env_dbdict, aType='4',dType='2')
        elif aType == '5':
            '''
            寻艺榜
            src:192.168.40.28|iis_web_crawler188|star_rank|(type:5)
            dst:192.168.20.101|super_fans_beta|tb_star_xunyi|(data_type:2)
            '''
            starRank3(env,env_dbdict, aType='5', dType='2')



if __name__ == '__main__':
    main()
