#!/usr/bin/python
#coding=utf-8

import logging
from logging.handlers import TimedRotatingFileHandler
import const


#初始化日志
def init(log_file = const.LOGFILE, logger_name = '', level=logging.INFO,rotat=True):
    #生成一个日志对象
    logger = logging.getLogger()
    logger.setLevel(level)
    if level is logging.DEBUG:
        formatter = logging.Formatter('%(asctime)s  %(levelname)s %(threadName)s  %(funcName)s  %(message)s ')
    else:
        #formatter = logging.Formatter('%(asctime)s  %(levelname)s %(threadName)s %(funcName)s %(lineno)d  %(message)s ')    
        formatter = logging.Formatter('%(asctime)s  %(levelname)s %(threadName)s %(funcName)s %(message)s ')    
    if rotat:
        fhdr =  TimedRotatingFileHandler(log_file,when='midnight',backupCount=3000)
        fhdr.suffix= "%Y%m%d"
    else:
        fhdr = logging.FileHandler(log_file)
    fhdr.setFormatter(formatter)
    # Create a stream handler to print all messages to console 
    chdr = logging.StreamHandler()
    chdr.setFormatter(formatter)
    #
    logger.addHandler(fhdr)
    logger.addHandler(chdr)
