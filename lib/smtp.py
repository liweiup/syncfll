# -*- coding: utf-8 -*-
"""
email相关协议分封装
"""
__version__ = '1.0'
__author__ = 'songqiang'
__date__ = '2017-01-01'
__title__ = 'email'


import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header


class EmailSender():
    """
    邮件发送
    登录--邮件内容--发送
    
    """
    def __init__(self,smtp_host, smtp_user, smtp_password, smtp_port = 25):
        self.smtp_host = smtp_host
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.smtp_port = smtp_port
        self._connect()
    def _connect(self):
        self.smtp_client = smtplib.SMTP() 
        #连接到服务器
        self.smtp_client.connect(self.smtp_host, self.smtp_port)
        #登录到服务器
        self.smtp_client.login(self.smtp_user, self.smtp_password)
    def __del__(self):
        self.smtp_client.quit()
    def send(self, receivers, subject, content, attachments = []):
        """
        @receivers 收件人  list或string  多个用","分隔
        @subject 主题
        @content 邮件内容
        @sender 发件人
        @attachments 附件的绝对路径
        """
        sender = self.smtp_user
        if isinstance(receivers, str):
            receivers = receivers.split(",")
        try:
            message = MIMEMultipart()
            #邮件主题
            message['Subject'] = Header(subject, 'utf-8')
            #发送方信息
            message['From'] = self.smtp_user 
            #接受方信息     
            message['To'] = ",".join(receivers)
            #text
            message_text = MIMEText(content,'plain','utf-8')
            message.attach(message_text)
            #附件
            for attachment in  attachments:
                att = MIMEApplication(open(attachment,'rb').read())
                filename = os.path.basename(attachment)
                att.add_header('Content-Disposition', 'attachment', filename = filename)  
                message.attach(att)
            #发送
            self.smtp_client.sendmail(sender,receivers,message.as_string())
        except smtplib.SMTPException as error:
            logger.error('error:',error)
        except Exception, error:
            print error

        
if __name__ == '__main__':
    ms = EmailSender("mail.xd-tech.cn", "songqiang@miduchina.com", "密码")
    ms.send(["songqiang@xd-tech.cn", "songqiang@miduchina.com"], "测试", "测试正文")
    # ms.send(["songqiang@xd-tech.cn", "songqiang@miduchina.com"], "测试", "测试正文", ["c:\\version.conf", "C:\\Users\\sq\\Desktop\\2M0F7HCZQLZT(A(SJJJ103Y.png"])
    