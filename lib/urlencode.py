#!/usr/bin/python
#coding:utf-8
import urllib

class urlencode():
    """urlencode/urldecode .gbk兼容gb2312"""
    
    #判断
    def is_unicode(self,s):
        if isinstance(s, unicode):
            return s
        else:
            s = s.decode('utf-8')
            return s
       
    #编码
    def urlencode(self,s,encoding):
        s = self.is_unicode(s)
        s = s.strip()
        if not s:
            if s is None:
                raise TypeError('None object cannot be quoted')
            return s
        
        if encoding=='gbk' or encoding=='gb2312':
            s=s.encode('gbk')
            gbk_urlecode=urllib.quote(s)
            return gbk_urlecode     
    
        elif encoding=='utf8' or encoding=='utf-8':
            s=s.encode('utf-8')
            utf8_urlecode=urllib.quote(s)
            return utf8_urlecode
        else:
            raise TypeError('encoding is not gbk/utf-8')
    #解码
    def urldecode(self,s):
        s = s.strip()
        result = urllib.unquote(s)
        return result
    

if __name__ == '__main__':          
    content="""［洲·微榜20160623］每天早晨看数据时，居然有点小忐忑，一面盼着数据上涨，一面又告诫自己没有常涨不落的数据。但这种心情，你能理解吧？专为许魏洲订做的每天一期［洲·微榜］，白粥们，订阅了吗？http://weibo.com/5919572497/DBxfojTal?from=page_1005055919572497_profile&wvr=6&mod=weibotime&type=comment#_rnd1466646011201"""
    u=urlencode()
    print u.urlencode(content, 'gbk')
    #gbk
    a='''%A3%DB%D6%DE%A1%A4%CE%A2%B0%F120160623%A3%DD%C3%BF%CC%EC%D4%E7%B3%BF%BF%B4%CA%FD%BE%DD%CA%B1%A3%AC%BE%D3%C8%BB%D3%D0%B5%E3%D0%A1%EC%FE%EC%FD%A3%AC%D2%BB%C3%E6%C5%CE%D7%C5%CA%FD%BE%DD%C9%CF%D5%C7%A3%AC%D2%BB%C3%E6%D3%D6%B8%E6%BD%EB%D7%D4%BC%BA%C3%BB%D3%D0%B3%A3%D5%C7%B2%BB%C2%E4%B5%C4%CA%FD%BE%DD%A1%A3%B5%AB%D5%E2%D6%D6%D0%C4%C7%E9%A3%AC%C4%E3%C4%DC%C0%ED%BD%E2%B0%C9%A3%BF%D7%A8%CE%AA%D0%ED%CE%BA%D6%DE%B6%A9%D7%F6%B5%C4%C3%BF%CC%EC%D2%BB%C6%DA%A3%DB%D6%DE%A1%A4%CE%A2%B0%F1%A3%DD%A3%AC%B0%D7%D6%E0%C3%C7%A3%AC%B6%A9%D4%C4%C1%CB%C2%F0%A3%BFhttp%3A//weibo.com/5919572497/DBxfojTal%3Ffrom%3Dpage_1005055919572497_profile%26wvr%3D6%26mod%3Dweibotime%26type%3Dcomment%23_rnd1466646011201'''
    #utf8
    b='''%EF%BC%BB%E6%B4%B2%C2%B7%E5%BE%AE%E6%A6%9C20160623%EF%BC%BD%E6%AF%8F%E5%A4%A9%E6%97%A9%E6%99%A8%E7%9C%8B%E6%95%B0%E6%8D%AE%E6%97%B6%EF%BC%8C%E5%B1%85%E7%84%B6%E6%9C%89%E7%82%B9%E5%B0%8F%E5%BF%90%E5%BF%91%EF%BC%8C%E4%B8%80%E9%9D%A2%E7%9B%BC%E7%9D%80%E6%95%B0%E6%8D%AE%E4%B8%8A%E6%B6%A8%EF%BC%8C%E4%B8%80%E9%9D%A2%E5%8F%88%E5%91%8A%E8%AF%AB%E8%87%AA%E5%B7%B1%E6%B2%A1%E6%9C%89%E5%B8%B8%E6%B6%A8%E4%B8%8D%E8%90%BD%E7%9A%84%E6%95%B0%E6%8D%AE%E3%80%82%E4%BD%86%E8%BF%99%E7%A7%8D%E5%BF%83%E6%83%85%EF%BC%8C%E4%BD%A0%E8%83%BD%E7%90%86%E8%A7%A3%E5%90%A7%EF%BC%9F%E4%B8%93%E4%B8%BA%E8%AE%B8%E9%AD%8F%E6%B4%B2%E8%AE%A2%E5%81%9A%E7%9A%84%E6%AF%8F%E5%A4%A9%E4%B8%80%E6%9C%9F%EF%BC%BB%E6%B4%B2%C2%B7%E5%BE%AE%E6%A6%9C%EF%BC%BD%EF%BC%8C%E7%99%BD%E7%B2%A5%E4%BB%AC%EF%BC%8C%E8%AE%A2%E9%98%85%E4%BA%86%E5%90%97%EF%BC%9Fhttp%3A//weibo.com/5919572497/DBxfojTal%3Ffrom%3Dpage_1005055919572497_profile%26wvr%3D6%26mod%3Dweibotime%26type%3Dcomment%23_rnd1466646011201'''
    u=urlencode()
    print u.urldecode(b)
