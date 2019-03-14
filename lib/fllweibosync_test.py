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
import daemon
import log
import mdb
import fh
import utils
import smtp
import phonesms

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


#将从爬虫库获取的数据写入到产品库
def starRank(env,dbdict, src_aType, dType):
    #aType: 0-新星，1-韩国，3-港澳台，5-内地
    #area_type = [0, 1, 3, 5]
    #dType: 1-实时，3-周榜，5-月榜
    db_host=dbdict['dstFll']['db_host']
    db_name=dbdict['dstFll']['db_name']
    db_user=dbdict['dstFll']['db_user']
    db_passwd=dbdict['dstFll']['db_password']
    db_port=dbdict['dstFll']['db_port']
    db_table=dbdict['dstFll']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)
    
    #用于第一名
    dst_aType = src_aType.split("-")[0]
    firstName = "第一名_%s"%(dst_aType)
    src_table="iis_weibo_crawler34"
    dst_table="tb_star_power"
    proname = "明星势力榜"
    data_type = dType
    
    
    #获取指定类型（aType）的明星名
    '''
    starName_real_dict
    {starName1_nickname:realName1,
    starName2_nickname:realName2}
    
    starName_status_dict
    #0 爬虫库中未找到对应的明星
    #1 爬虫库中找到对应的明星
    {starName1_nickname:0,
     starName2_nickname:1}
    '''
    starName_real_dict = {}  #明星微博昵称和真实名字参照；
    starName_status_dict = {} #明星状态，明星名用的昵称；
    #starSql = """select starName,weibo_screen_name from tb_star where type=%s;"""%(aType)
    starSql = """select starName,weibo_screen_name from tb_star where (type=0 or type=1 or type=3 or type=5 or type=6) and add_type=1;"""
    starResults=db.queryAll(starSql)
    if len(starResults):
        for d in starResults:
            starName = d["weibo_screen_name"]
            realName = d["starName"]
            starName_real_dict[starName] = realName
    else:
        logger.info("""库：%s 类型：%s 明星数为0."""%(db_host,dst_aType))
        return False
        
    
    #根据明星列表将从爬虫库获取的数据插入到产品表
    starName_list = starName_real_dict.keys()
    OrigData = getOrigData(env, dbdict, proname, dType, src_aType)
    #print OrigData
    if OrigData:
        content = OrigData[0]
        rankList = eval(content)[0]["ranklist"]
        for d in rankList:
            starName = d["starName"]
            rankNumber = str(d["rankNumber"])
            if starName in starName_list:
                realName = starName_real_dict[starName]
                starName_status_dict[starName] = 1 #更改更新状态
                #old
                searchNum = d["searchNum"]
                score = d["score"]
                loveRank = d["loveRank"]
                searchRank = d["searchRank"]
                discussRank = d["discussRank"]
                ineractRank = d["ineractRank"]
                loveNum = d["loveNum"]
                ineractNum = d["ineractNum"]
                discussNum = d["discussNum" ]
                #new
                ineractScore = d["ineractScore"]
                loveScore = d["loveScore"]
                discussScore = d["discussScore"]
                searchScore = d["searchScore"]
                
                capture_time = d["catchTime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from tb_star_power where starName='%s' and data_type='%s' and create_time>='%s';"""%(realName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO tb_star_power (starName,searchNum,score,loveRank,searchRank,discussRank,
                                   ineractRank,loveNum,rankNumber,ineractNum,discussNum,ineractScore,loveScore,discussScore,searchScore,data_type,date,score_type,create_time,capture_time) VALUES ('%s',
                                   '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"""%(realName,searchNum,score,
                                   loveRank,searchRank,discussRank,ineractRank,loveNum,rankNumber,ineractNum,discussNum,ineractScore,loveScore,discussScore,searchScore,data_type,current_date,dst_aType,current_time,capture_time)
                    logger.info(insertSql)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE tb_star_power SET starName='%s',searchNum='%s',score='%s',loveRank='%s',searchRank='%s',
                                   discussRank='%s',ineractRank='%s',loveNum='%s',rankNumber='%s',ineractNum='%s',discussNum='%s',ineractScore='%s',loveScore='%s',discussScore='%s',searchScore='%s',score_type='%s',
                                   update_time='%s',capture_time='%s' where starName='%s' and data_type='%s' and date='%s';"""%(realName,searchNum,score,loveRank,searchRank,discussRank,ineractRank,
                                                         loveNum,rankNumber,ineractNum,discussNum,ineractScore,loveScore,discussScore,searchScore,dst_aType,current_time,capture_time,realName,data_type,current_date)
                    logger.info(updateSql)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)
                    
            #用于第一名
            if rankNumber == '1':
                starName = firstName
                realName = firstName
                #old
                searchNum = d["searchNum"]
                score = d["score"]
                loveRank = d["loveRank"]
                searchRank = d["searchRank"]
                discussRank = d["discussRank"]
                ineractRank = d["ineractRank"]
                loveNum = d["loveNum"]
                ineractNum = d["ineractNum"]
                discussNum = d["discussNum" ]
                #new
                ineractScore = d["ineractScore"]
                loveScore = d["loveScore"]
                discussScore = d["discussScore"]
                searchScore = d["searchScore"]
                
                capture_time = d["catchTime"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))                
                searchSql = """select * from tb_star_power where starName='%s'and data_type='%s' and create_time>='%s';"""%(realName,data_type,current_date)
                searchResults = db.queryAll(searchSql)                    
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO tb_star_power (starName,searchNum,score,loveRank,searchRank,discussRank,
                                   ineractRank,loveNum,rankNumber,ineractNum,discussNum,ineractScore,loveScore,discussScore,searchScore,data_type,date,score_type,create_time,capture_time) VALUES ('%s',
                                   '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"""%(realName,searchNum,score,
                                   loveRank,searchRank,discussRank,ineractRank,loveNum,rankNumber,ineractNum,discussNum,ineractScore,loveScore,discussScore,searchScore,data_type,current_date,dst_aType,current_time,capture_time)
                    logger.info(insertSql)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE tb_star_power SET starName='%s',searchNum='%s',score='%s',loveRank='%s',searchRank='%s',
                                   discussRank='%s',ineractRank='%s',loveNum='%s',rankNumber='%s',ineractNum='%s',discussNum='%s',ineractScore='%s',loveScore='%s',discussScore='%s',searchScore='%s',score_type='%s',
                                   update_time='%s',capture_time='%s' where starName='%s' and data_type='%s' and date='%s';"""%(realName,searchNum,score,loveRank,searchRank,discussRank,ineractRank,
                                                         loveNum,rankNumber,ineractNum,discussNum,ineractScore,loveScore,discussScore,searchScore,dst_aType,current_time,capture_time,realName,data_type,current_date)
                    logger.info(updateSql)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)                

    
    #将tb_star中有而爬虫重没有的明星置空
    for starName in starName_real_dict.keys():
        if starName not in starName_status_dict.keys():
            realName = starName_real_dict[starName]
            current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
            current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
            emptySql = """select * from tb_star_power where starName='%s' and data_type='%s' and create_time>='%s';"""%(realName,data_type,current_date)
            emptyResults = db.queryAll(emptySql)
            if len(emptyResults) == 0 :
                eInsertSql = """INSERT INTO tb_star_power (starName,data_type,date,score_type,create_time) VALUES ('%s','%s','%s','%s','%s');"""%(realName,data_type,current_date,dst_aType,current_time)
                db.executeSql(eInsertSql)
                logger.info("未在表：%s 中找到明星：%s，%s 已插入默认值"%(src_table,starName,dst_table))
            else:
                logger.info("未在表：%s 中找到明星：%s，%s 已存在不操作"%(src_table,starName,dst_table))            
 

    
#从爬虫库获取数据源    
def getOrigData(env,dbdict,proname,dType,aType=''):
    '''
    evn: www-现网, beta-测试
    proname: 明星势力榜
    dType: 1-昨日, 2-周, 3-月(用于监控预警)
    aType: 0-新星, 1-韩国, 3-港澳台，5-内地
    '''
    db_host=dbdict['srcWeibo']['db_host']
    db_name=dbdict['srcWeibo']['db_name']
    db_user=dbdict['srcWeibo']['db_user']
    db_passwd=dbdict['srcWeibo']['db_password']
    db_port=dbdict['srcWeibo']['db_port']
    db_table=dbdict['srcWeibo']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)

    sql = """select * from star_rank_test where createtime>='%s' and type='%s' ORDER BY createtime desc limit 0,1;"""%(TDate,aType)
    results=db.queryAll(sql)    
    
    if len(results):
        createtime = results[0]["createtime"]
        crawler_timestamp=time.mktime(time.strptime(str(createtime),'%Y-%m-%d %H:%M:%S'))
        ten_timestamp=time.mktime(time.strptime("%s 10:00:00"%TDate,'%Y-%m-%d %H:%M:%S'))#
        if dType=='1' and (ten_timestamp < time.time()) and (crawler_timestamp < ten_timestamp):
            inform(env, proname, aType, db_name, db_table)
            logger.info("the content is empty db:%s,table:%s,type:%s"%(db_host,db_table,aType))
            return False
        else:
            aType = results[0]["type"]
            aType = aType.split("-")[0]
            content = results[0]["content"]
            result = [content,aType,createtime]
            return result            
    else:
        inform(env, proname, aType, db_name, db_table)
        logger.info("the content is empty db:%s,table:%s,type:%s"%(db_host,db_table,aType))  
        return False


#邮件/短信通知模块
def inform(env, proname, aType, db_name, db_table):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
    mail_to = ["junyu@miduchina.com",
               "peilong@miduchina.com",
               "baichun@miduchina.com",
               "xuemei@miduchina.com",
               "gaojing@miduchina.com",                   
               "liwei2@miduchina.com",
               "junjun@miduchina.com",
               "huichen@miduchina.com",
               "bowen@miduchina.com",
               "junjie@miduchina.com"]
    mail_sub = "明星榜无数据预警-%s"%proname
    mail_body = "@All\n\n%s无源数据\n类型：%s\n数据库：%s\n数据表：%s\n查询时间：%s"%(proname,aType,db_name,db_table,current_time)
    ms = smtp.EmailSender("mail.miduchina.com", "opd@miduchina.com", "rXS06HrbMDif")
    ms.send(mail_to, mail_sub, mail_body)
    if env == "www":
        #短信通知
        sms_body = "%s无源数据\n类型：%s\n数据库：%s\n数据表：%s\n查询时间：%s"%(proname,aType,db_name,db_table,current_time)
        phoneNumber="15862183966,15727337093,18141906780,15138931970,15221194570,18501968603"
        phonesms.sms(phoneNumber, sms_body)        


def help():
    print "%s '[hour|week|month](aType)'" % sys.argv[0]
    sys.exit()


def get_args():
    if len(sys.argv) != 2:
        help()
    else:
        aType = sys.argv[1]
        return (aType,)


def main():
    args = get_args()
    Type = args[0]
    
    for env in dbdict.keys():
        env = "beta"
        env_dbdict = dbdict[env]
        #实时榜
        if Type == 'hour':
            '''
            明星势力实时榜
            src:192.168.40.28|iis_weibo_crawler34|star_rank|(type:1|3|5)
            dst:192.168.20.101|super_fans_beta|tb_star_power|(data_type:1)
            '''
            area_type = ['0','1','3','5','6']
            for aType in area_type:
                starRank(env,env_dbdict, src_aType=aType,dType='1')
        #周榜
        elif Type == 'week':
            '''
            明星势力周榜
            src:192.168.40.28|iis_weibo_crawler34|star_rank|(type:1-week|3-week|5-week)
            dst:192.168.20.101|super_fans_beta|tb_star_power|(data_type:2)
            '''
            area_type = ['0-week','1-week', '3-week', '5-week']
            for aType in area_type:
                starRank(env,env_dbdict, src_aType=aType,dType='2')
        #月榜
        elif Type == 'month':
            '''
            明星势力周榜
            src:192.168.40.28|iis_weibo_crawler34|star_rank|(type:1-month|3-month|5-month)
            dst:192.168.20.101|super_fans_beta|tb_star_power|(data_type:3)
            '''
            area_type = ['0-month','1-month', '3-month', '5-month']
            for aType in area_type:
                starRank(env,env_dbdict, src_aType=aType,dType='3')      
    
 
if __name__ == '__main__':
    main()

