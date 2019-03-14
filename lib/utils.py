#!/usr/bin/env python
#coding=utf-8
#文件操作，字符串md5计算

import os
import hashlib
import logging
logger=logging.getLogger()


#读取进度
def pos(ropfile):
    if os.path.exists(ropfile):
        with open(ropfile) as f:
            pos = f.read().strip()
            if pos:
                return pos
            else:
                logger.error('pos formate error' % ropfile)
    else:
        logger.error('ropfile:%s not exist' % ropfile)
        
    
#写入进度
def record_pos(postion, ropfile):
    try:
        with open(ropfile,'w') as f:
            f.write(str(postion))
    except Exception, error:
        logger.error("record postion:%s %s [failed]"%(error, postion))
    else:
        logger.info("record postion:%s [ok]"%postion)


#文件操作
# writefile(str)
def writefile(file, new_str, type='w'):
    with open(file, type) as f:
        f.write("%s\n"%new_str)
        
        
# writefile(list)
def writefile(files, line_list, type='w'):
    f=open(files, type)
    for n in line_list:
        f.write('%s\n'%n)
    f.close()
    
    
# 以上两个整理为
# writefile(str/list)
def writefile(file, obj, type='w'):
    '''
    obj：可为str与list
    '''
    if isinstance(obj, str):
        with open(file, type) as f:
            f.write("%s\n"%obj)
            
    elif isinstance(obj, list):
        f=open(file, type)
        for n in obj:
            f.write('%s\n'%n)
        f.close()
        
        
# readfile
def readfile(file):
    with open(file,'r') as f:
        content = f.readlines()
    return content


# noline(\n)
def noline(line_list):
    new_list=[]
    for line in line_list:
        line=line.strip()
        new_list.append(line)
    return new_list


# 读取文件内容
def readFileContent(file):
    with open(file,'r') as f:
        content = f.readlines()
    str_content=''
    for line in content:
        str_content = str_content+line 
    return str_content


# 文件可迭代对象
def haha(file):
    with open(file) as file:
        for line in file:
            do_things(line)
        
        

#获取str的md5值
def md5(str):
    m = hashlib.md5()  
    m.update(str)
    return m.hexdigest()


if __name__ == '__main__':
    pass