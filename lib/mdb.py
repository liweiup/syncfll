#!/usr/bin/python
#coding:utf-8

import MySQLdb
from warnings import filterwarnings
filterwarnings( 'ignore', category = MySQLdb.Warning )

import logging
logger = logging.getLogger()


# MySQL类库
class RunMysql:
    def __init__(self,host,database,user,passwd,port=3306,character="utf8",connect_timeout=1,result_dict=False):
        self.host=host
        self.user=user
        self.passwd=passwd
        self.database=database
        self.charaxter=character
        self.port=port
        self.conn = MySQLdb.connect(self.host,self.user,self.passwd,self.database,port=self.port)
        #需要指定编码 不然会出现'latin-1' codec can't encode character错误
        self.conn.set_character_set(self.charaxter)
        #返回dict
        if result_dict:
            self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        else:
            self.cursor = self.conn.cursor()
    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()
        #print "db close"
        logger.debug("db close")
    #执行SQL语句
    def executeSql(self,sql):
        logger.debug("sql:"+sql)
        try:
            result=self.cursor.execute(sql)
            self.conn.commit()
        except Exception, error:
            logger.error("sql run error%s"%error)
            #logger.error("sql run error%s"%error,"error")
        else:
            #返回值为受影响的行数 
            return result
    #查询返回结果集
    def queryAll(self,sql):
        self.executeSql(sql)
        #指针
        #cursor.scroll(0,mode='absolute') 
        rows=self.cursor.fetchall()
        return rows
    #查询返回1条结果
    def queryOne(self,sql):
        self.executeSql(sql)
        result = self.cursor.fetchone()
        return result
    #获取最后插入行的主键ID 
    def getLastInsertId(self):
        return self.cursor.lastrowid
    #最新插入行的主键ID，conn.insert_id()一定要在conn.commit()之前，否则会返回0  
    #def getLastInsertId2(self):
        #return self.conn.insert_id()
    #上次查询或更新所发生行数
    def rowCount(self):
        return self.cursor.rowcount
    #提交事务不然innodb数据库不能插入数据
    def commit(self):
        self.conn.commit()
    def rollback(self):
        self.conn.rollback()
