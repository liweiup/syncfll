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

proname = "微舆情热度榜"
proname_en = "WYQHeatRank"

#time
TDate=time.strftime("%Y-%m-%d",time.localtime(time.time()))
TDate_lite=time.strftime("%Y%m%d",time.localtime(time.time()))
YDate=time.strftime("%Y-%m-%d",time.localtime(time.time()-1*24*60*60))
YDate_lite=time.strftime("%Y%m%d",time.localtime(time.time()-1*24*60*60))
NHour=time.strftime("%H",time.localtime(time.time()))


# 热度榜
def heatRank1(dbdict,aType='1'):
    '''
    aType:爬虫type类型
    dType:data_type(1:实时数据 2:周榜数据 3:月榜数据)
    dst_table:heat_statistics_hour(1实时)
              heat_statistics_week（2周）
              heat_statistics_month（3月）
    '''
    #关键数据
    dst_table = "tb_star_hot"
    firstName = "第一名"
    if aType == '1':
        src_table = "heat_statistics_hour"
        data_type = aType 
    elif aType == '2':
        src_table = "heat_statistics_week"
        data_type = aType 
    elif aType == '3':
        src_table = "heat_statistics_month"
        data_type = aType
    
    #dst_table = "tb_star_hot" #产品目标表
    #data_type = dType     
    
    DSRefer = {"starName":"name",
               "hot":"heat_value",
               "rankNumber":"rank",
               "capture_time":"create_time"}

    db_host=dbdict['dstFll']['db_host']
    db_name=dbdict['dstFll']['db_name']
    db_user=dbdict['dstFll']['db_user']
    db_passwd=dbdict['dstFll']['db_password']
    db_port=dbdict['dstFll']['db_port']
    db_table=dbdict['dstFll']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)
    
    
    #获取指定类型（aType）的明星名
    '''
    #0 爬虫库中未找到对应的明星
    #1 爬虫库中找到对应的明星
    {starName1:0,
     starName2:1}
    '''
    starName_dict = {} #明星状态0不存在1存在
    otherName_starName_dict = {} #明星与otherName对应表
    starSql = """select starName,other_name from tb_star;"""
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
    OrigData = getOrigData(dbdict,src_table)
    if OrigData:
        rankList = OrigData
        for d in rankList:
            starName = d["starName"]
            rankNumber = d["rankNumber"]
            if (starName in starName_list) or (starName in otherName_list):
                if starName in otherName_list:
                    starName = otherName_starName_dict[starName]
                if starName_dict[starName] == 1:
                    continue
                starName_dict[starName] = 1 #更改更新状态
                hot = d["hot"]
                capture_time = d["capture_time"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,hot,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                           '%s','%s','%s','%s');"""%(dst_table,starName,hot,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',hot='%s',rankNumber=%s,data_type=%s,update_time='%s',capture_time='%s' 
                                           where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,hot,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
                    db.executeSql(updateSql)
                    logger.info("明星：%s 已更新"%starName)                
                
            #用于第一名
            if rankNumber == 1:
                starName = firstName
                hot = d["hot"]
                capture_time = d["capture_time"]
                current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
                current_date = time.strftime("%Y-%m-%d",time.localtime(time.time()))                
                searchSql = """select * from %s where starName='%s' and data_type='%s' and create_time>='%s';"""%(dst_table,starName,data_type,current_date)
                searchResults = db.queryAll(searchSql)
                if len(searchResults) == 0 :
                    insertSql = """INSERT INTO %s (starName,hot,rankNumber,data_type,date,create_time,capture_time) VALUES ('%s','%s','%s',
                                           '%s','%s','%s','%s');"""%(dst_table,starName,hot,rankNumber,data_type,current_date,current_time,capture_time)
                    db.executeSql(insertSql)
                    logger.info("明星：%s 已插入"%starName)
                else:
                    updateSql = """UPDATE %s SET starName='%s',hot='%s',rankNumber=%s,data_type=%s,update_time='%s',capture_time='%s' 
                                           where starName='%s' and data_type='%s' and date='%s';"""%(dst_table,starName,hot,rankNumber,data_type,current_time,capture_time,starName,data_type,current_date)
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
                logger.info("未在表：%s 中找到明星：%s，%s 已插入默认值"%(src_table,starName,db_table))
            else:
                logger.info("未在表：%s 中找到明星：%s，%s 已存在不操作"%(src_table,starName,db_table))
    

#获取第一名               

    
#从分析库获取数据源    
def getOrigData(dbdict,db_table=''):
    """
    data_type  1实时 2周 3月
    heat_statistics_hour(1实时)
    heat_statistics_week（2周）
    heat_statistics_month（3月）
    src:dst
    name:starName
    heat_value:hot
    rank:rankNumber
    create_time:capture_time
    """
    
    db_host=dbdict['srcHeat']['db_host']
    db_name=dbdict['srcHeat']['db_name']
    db_user=dbdict['srcHeat']['db_user']
    db_passwd=dbdict['srcHeat']['db_password']
    db_port=dbdict['srcHeat']['db_port']
    #db_table=dbdict['srcHeat']['db_table']
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)
    
    print db_table
    if db_table == "heat_statistics_hour":
        key_sql = '''select cfg_value from business_config where cfg_name='HOT_REAL-TIME_BATCHES_HOUR';'''
        key_value=db.queryOne(key_sql)["cfg_value"]
        sql = """select * from %s where analysis_task_ticket='%s';"""%(db_table,key_value)
    elif db_table == "heat_statistics_week":
        key_sql = '''select cfg_value from business_config where cfg_name='HOT_REAL-TIME_BATCHES_WEEK';'''
        key_value=db.queryOne(key_sql)["cfg_value"]
        print key_value
        print key_sql        
        sql = """select * from %s where analysis_task_ticket='%s';"""%(db_table,key_value)
        print sql
    results=db.queryAll(sql)    
    
    if len(results):
        heat_results = []
        for d in results:
            capture_time = d["create_time"]
            rankNumber = int(d["rank"])
            hot = d["heat_value"]
            starName = d["name"]
            result = {"starName":starName,
                      "hot":hot,
                      "rankNumber":rankNumber,
                      "capture_time":capture_time,
                      }
            heat_results.append(result)
        #test
        current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
        sms_body = "%sNoOrigData\nDBName：%s\nDBTable：%s\nQueryTime：%s"%(proname_en,db_name,db_table,current_time)
        sms_body = "%s无源数据\n数据库:%s\n数据表:%s\n查询时间:%s"%(proname,db_name,db_table,current_time)
        phoneNumber="15138931970,15821761903,18141906780"
        #phoneNumber="15862183966,15727337093,18141906780,15138931970,15221194570"
        phonesms.sms(phoneNumber, sms_body)        
        return heat_results
    else:
        current_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
        mail_to = ["junyu@miduchina.com",
                   "peilong@miduchina.com",
                   "zhaoman@miduchina.com",
                   "xuemei@miduchina.com",
                   "gaojing@miduchina.com",                   
                   "junjun@miduchina.com",
                   "huichen@miduchina.com",
                   "bowen@miduchina.com",
                   "junjie@miduchina.com"]
        mail_sub = "明星榜无数据预警-%s"%proname
        mail_body = "@All\n\n%s无源数据\n数据库：%s\n数据表：%s\n查询时间：%s"%(proname,db_name,db_table,current_time)
        ms = smtp.EmailSender("mail.miduchina.com", "opd@miduchina.com", "rXS06HrbMDif")
        ms.send(mail_to, mail_sub, mail_body)
        #短信通知
        sms_body = "%s无源数据\n数据库：%s\n数据表：%s\n查询时间：%s"%(proname,db_name,db_table,current_time)
        phoneNumber="15138931970,18141906780"
        phoneNumber="15862183966,15727337093,18141906780,15138931970,15221194570"
        phonesms.sms(phoneNumber, sms_body)
        logger.info("the content is empty db:%s table:%s"%(db_host,db_table))
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
    
    #实时榜
    if aType == '1':
        '''
        微舆情热度榜
        src:192.168.1.42|iis_application_platform_analysis|heat_statistics_hour
        dst:192.168.20.101|super_fans_beta|tb_star_hot|(data_type:1)
        '''
        heatRank1(dbdict,aType='1')  
    #周榜
    elif aType == '2':
        '''
        微舆情热度榜
        src:192.168.40.28|iis_application_platform_analysis|heat_statistics_week
        dst:192.168.20.101|super_fans_beta|tb_star_hot|(data_type:2)
        '''
        heatRank1(dbdict,aType='2')
    #月榜
    elif aType == '3':
        '''
        微舆情热度榜
        src:192.168.40.28|iis_application_platform_analysis|heat_statistics_month
        dst:192.168.20.101|super_fans_beta|tb_star_hot|(data_type:2)
        '''
        heatRank1(dbdict,aType='3')        
    
    

if __name__ == '__main__':
    main()
