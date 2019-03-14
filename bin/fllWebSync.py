#!/usr/bin/env python
#coding:utf-8
#python2.6
import sys
import os


sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'lib'))
import fllwebsync


if __name__ == '__main__':
    fllwebsync.main()