#!/bin/bash

export LANG=zh_CN.GBK
export LC_CTYPE="zh_CN.GBK"
export LC_NUMERIC="zh_CN.GBK"
export LC_TIME="zh_CN.GBK"
export LC_COLLATE="zh_CN.GBK"
export LC_MONETARY="zh_CN.GBK"
export LC_MESSAGES="zh_CN.GBK"
export LC_PAPER="zh_CN.GBK"
export LC_NAME="zh_CN.GBK"
export LC_ADDRESS="zh_CN.GBK"
export LC_TELEPHONE="zh_CN.GBK"
export LC_MEASUREMENT="zh_CN.GBK"
export LC_IDENTIFICATION="zh_CN.GBK"
export LC_ALL=zh_CN.GBK

/usr/bin/curl "http://sms.xdqxcm.xd-tech.cn/esmsThirdpartySend?businessId=126&mobile=15138931970,18141906780&content=微舆情热度榜无源数据\n数据库：iis_application_platform_analysis_beta\n数据表：heat_statistics_hour\n查询时间：2017-03-22_19:24:04"
