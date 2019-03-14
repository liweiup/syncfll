#!/usr/bin/env python
#coding=utf-8

import sys
import os

#base config

ROOTDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SBINDIR = os.path.join(ROOTDIR,'sbin')
BINDIR = os.path.join(ROOTDIR,'bin')
LOGDIR = os.path.join(ROOTDIR,'logs')
LIBDIR = os.path.join(ROOTDIR,'lib')
TMPDIR = os.path.join(ROOTDIR,'tmp')
ROPDIR = os.path.join(ROOTDIR,'rop')
DATADIR = os.path.join(ROOTDIR,'data')

PROJECT_NAME = ROOTDIR.split("/")[-1]

PIDFILE = os.path.join(TMPDIR,PROJECT_NAME + '.pid')
LOGFILE = os.path.join(LOGDIR,PROJECT_NAME + '.log')


DBDICT = {'www':{'srcWeb':{'db_host':'192.168.40.9',
                           'db_port':3326,
                           'db_name':'iis_web_crawler188',
                           'db_user':'admin',
                           'db_password':'richard',
                           'db_table':'star_rank'
                           },
                'srcWeibo':{'db_host':'192.168.40.9',
                       'db_port':3326,
                       'db_name':'iis_weibo_crawler34',
                       'db_user':'admin',
                       'db_password':'richard',
                       'db_table':'star_rank'
                       },
                'srcHeat':{'db_host':'ro.sql.51wyq.cn',
                       'db_port':3356,
                       'db_name':'iis_application_platform_analysis',
                       'db_user':'datanalysis',
                       'db_password':'data_analysis_paswd',
                       'db_table':'heat_statistics_hour'
                       },          
                'dstFll':{'db_host':'192.168.20.115',
                       'db_port':3346,
                       'db_name':'super_fans',
                       'db_user':'supfan_sre',
                       'db_password':'sowqcuus',
                       'db_table':''
                       },
                         },
          'beta':{'srcWeb':{'db_host':'192.168.40.9',
                            'db_port':3326,
                            'db_name':'iis_web_crawler188',
                            'db_user':'admin',
                            'db_password':'richard',
                            'db_table':'star_rank'
                            },
                     'srcWeibo':{'db_host':'192.168.40.9',
                            'db_port':3326,
                            'db_name':'iis_weibo_crawler34',
                            'db_user':'admin',
                            'db_password':'richard',
                            'db_table':'star_rank'
                            },
                     'srcHeat':{'db_host':'ro.sql.51wyq.cn',
                            'db_port':3356,
                            'db_name':'iis_application_platform_analysis',
                            'db_user':'datanalysis',
                            'db_password':'data_analysis_paswd',
                            'db_table':'heat_statistics_hour'
                            },          
                     'dstFll':{'db_host':'192.168.20.115',
                            'db_port':3346,
                            'db_name':'super_fans_beta',
                            'db_user':'supfan_sre',
                            'db_password':'sowqcuus',
                            'db_table':''
                            },
                          },          
          }



if __name__ == '__main__':
    print LOGDIR
