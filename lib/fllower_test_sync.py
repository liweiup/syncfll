#! /usr/bin/env python
#coding=utf-8
#python2.6

#标准库
import sys
import os
import json
import commands
# import logging
import time
import datetime


def week_get():
    d = datetime.datetime.now()
    dayscount = datetime.timedelta(days=d.isoweekday())
    dayto = d - dayscount
    sixdays = datetime.timedelta(days=6)
    date_to = datetime.date(dayto.year, dayto.month, dayto.day)
    return str(date_to)
# 自定义库
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'lib'))
import MySQLdb
import const
# import log
import mdb
dbdict = const.DBDICT
def getFllowStar(dbdict,dstFll_dbdict):
    db_host=dbdict['db_host']
    # db_host='192.168.40.9'
    db_name=dbdict['db_name']
    # db_name='iis_weibo_crawler34'
    db_user=dbdict['db_user']
    # db_user='admin'
    db_passwd=dbdict['db_password']
    # db_passwd='richard'
    db_port=dbdict['db_port']
    # db_port=3326
    time = week_get()
    db=mdb.RunMysql(db_host, db_name, db_user, db_passwd,port=db_port,result_dict=True)
    # db=mdb.RunMysql("localhost","superfans","root","123456",port=3346,result_dict=True)
    print(time)
    starSql = """select * from star_flower where date_format(createtime, "%%Y-%%m-%%d") = '%s'"""%(time)
    Results = db.queryAll(starSql)
    setFllowStar(dstFll_dbdict,Results)

    # print(dstFll_dbdict)
def setFllowStar(dstFll_dbdict,Results):
    db_host = dstFll_dbdict['db_host']
    # db_host='192.168.40.9'
    db_name = dstFll_dbdict['db_name']
    # db_name='iis_weibo_crawler34'
    db_user = dstFll_dbdict['db_user']
    # db_user='admin'
    db_passwd = dstFll_dbdict['db_password']
    # db_passwd='richard'
    db_port = dstFll_dbdict['db_port']
    # db_port=3326
    db = mdb.RunMysql(db_host, db_name, db_user, db_passwd, port=db_port, result_dict=True)
    # conn = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='123456', db='super_fans')
    # db = mdb.RunMysql("localhost", "super_fans", "root", "123456", port=3306, result_dict=True)
    print(len(Results))
    if len(Results):
        for d in Results:
            id       = d["id"]
            content  = d["content"]
            createtime = d["createtime"]
            insertSql = """REPlACE INTO tb_star_flower (id,content,createtime) VALUES ('%s','%s','%s');"""%(id,content, createtime)
            row = db.executeSql(insertSql)
            print(row)
        # logging.basicConfig()
def main():
    env_dbdict = dbdict['www']['srcWeibo']
    dstFll_dbdict = dbdict['www']['dstFll']
    getFllowStar(env_dbdict,dstFll_dbdict)

# if __name__ == '__main__':
main()
