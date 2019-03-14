#!/usr/bin/env python
#coding=utf-8


from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
#from thrift.protocol import TCompactProtocol
#
from hbase import Hbase
from hbase.ttypes import  *


import logging
logger=logging.getLogger()


class HbaseConnectException(Exception):
    pass


class RunHbase():
    """
    @host  Hbase Thrift接口IP
    @port  Hbase Thrift接口端口默认9090
    @table 表名
    
    """
    #初始化连接
    def __init__(self,host,table,port=9090):
        self.host=host
        self.port=port
        self.table=table
        #连接Hbase
        try:
            self.transport = TSocket.TSocket(self.host,  self.port)
            self.transport.setTimeout(120000)
            self.transport = TTransport.TBufferedTransport(self.transport)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            #self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
            
            self.client = Hbase.Client(self.protocol)
            self.transport.open()
            tables = self.client.getTableNames()
            if self.table not in tables:
                logger.error( "表%s不存在"%(self.table))
        except Exception,error:
            logger.error("hbase connection error,%s"%error)
            raise HbaseConnectException("hbase connection exception")
    #关闭连接
    def __del__(self):
        self.transport.close()
        # print("hbase close  ok")
    #通过rowkey获取一行数据
    def getOneRow(self,rowkey):
        result=self.client.getRow(self.table, rowkey, None)
        return result
    #插入1行数据
    def putOneRow(self,rowkey,data):
        """ 
        @data list数据为字典
        data=[{'column':'content:target_type','value':'wz'},
          {'column':'content:data','value':"{'a':'1','b':'2','c':'3'}"},
          ]"""
        mutations= []
        for d in data:
            mutations.append(Mutation(column=d['column'],value=d['value']))
        self.client.mutateRow(self.table, rowkey, mutations, None)
    #插入多行(主键空数据)
    def putMultiRowkey(self,rowkeylist,column_name,column_value):
        mutationsbatch = []
        for rowkey in rowkeylist:
            mutations = [
            Hbase.Mutation(column=column_name, value=column_value),
            ]
            mutationsbatch.append(Hbase.BatchMutation(row=rowkey,mutations=mutations))
        self.client.mutateRows(self.table, mutationsbatch,None)
    #插入1个单元的数据(cell)
    def put(self,rowkey,c,v):
        mutations = [Mutation(column=c, value=v)]
        self.client.mutateRow(self.table, rowkey, mutations, None)
    #scan表获取数据列表
    def scan(self,num):
        id = self.client.scannerOpenWithScan(self.table,
                                             TScan(),
                                             None)
        result=self.client.scannerGetList(id,num)
        self.client.scannerClose(id)
        return result
    def scanRange(self,startRowkey,stopRowkey,columnname,numRows):
        """
        startRowkey开始主键
        stopRowkey结束主键(不包含)
        column想要获取的字段
        numRows返回几条数据
        函数返回值为list：
        [(rawkey1,columvalue1),(rawkey2,columvalue2)........]
        """
        try:
            scan = Hbase.TScan(startRow=startRowkey, stopRow=stopRowkey)
            scannerId = self.client.scannerOpenWithScan(self.table, scan,None)
            #获取1行
            #row = self.client.scannerGet(scannerId)
            #获取指定行
            rowList = self.client.scannerGetList(scannerId,numRows)
            resultlist=[]
            for row in rowList:
                try:
                    #字段值
                    rowKey = row.row
                    columnvalue=row.columns.get(columnname).value
                except Exception, error:
                    logger.error(" get value error,%s"%error)
                else:
                    resultlist.append((rowKey,columnvalue))
            self.client.scannerClose(scannerId)
        except Exception, error:
            logger.error("scan error,%s" % error)
        else:
            return resultlist
    #删除1行数据
    def deleteOneRow(self,rowkey):
        self.client.deleteAllRow(self.table, rowkey, None)
    #建立表
    def createTable(self):
        pass