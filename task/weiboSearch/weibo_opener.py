# encoding: utf-8
import urllib2
import urllib
import cookielib
from weibo_login import WeiboSearchLogin
from core.opener import Opener
from StringIO import StringIO
import gzip
#urllib version of weibo search opener
class urllibWSearchOpener(Opener):
    def __init__(self, cookie_filename=None):
        self.cj = cookielib.LWPCookieJar()
        if cookie_filename is not None:
            self.cj.load(cookie_filename)
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)
        urllib2.install_opener(self.opener)
    
    #extract url for search, can be abandoned, since the search url is specific
    def _extractSearchUrl(self,lxmlPage):
        i = 0
        url = ''
        for keyword in lxmlPage.xpath('//a[@class="nl"]/text()'):
            if keyword == '搜索':
                url = lxmlPage.xpath('//a[@class="nl"]/@href')[i]
                break
            i += 1
        return url
    
    #create cookie for request headers
    def _appendCookie(self,tmpcookie):
        cookieList = []
        for ck in tmpCookie:
            if ck.name == '_T_ML' or ck.name == '_T_WM' or ck.name == 'gsid_CTandWM':
                cookieList.append(str(ck.name)+'='+str(ck.value))
                
        cookieStr = ';'
        cookieStr = cookieStr.join(cookieList)
        return cookieStr
    
    def open(self, url,keyword,cookie=None):
        headers = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                   'Accept-Encoding':'gzip, deflate',
                   'Accept-Language':'en-US,en;q=0.5',
                   'Connection':'keep-alive',
                   'Host':'weibo.cn',
                   'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:28.0) Gecko/20100101 Firefox/28.0',
                   'Referer':'http://weibo.cn/search/?pos=search&vt=4' ####it is the url of search of weibo.cn
                   }
        data = urllib.urlencode({'keyword':keyword,
                                 'smblog':'搜微博'})
        if cookie != None:
            headers['Cookie']=cookie
            
        req = urllib2.Request(url ,data, headers)
        resp = urllib2.urlopen(req)
        
        #
        url_f = StringIO(resp.read())
        g = gzip.GzipFile(fileobj=url_f)
        print 'test'
        print g.read()
        print 'test2'
        #
#         is_gzip = resp.headers.dict.get('content-encoding') == 'gzip'
#         if is_gzip:
#             return self.ungzip(resp)
#         return resp.read()
    
if __name__ == "__main__":
    fetcher = WeiboSearchLogin("dengwei_cq@sina.com","8557726368")
    success, tmpCookie, page = fetcher.login()
    opener = urllibWSearchOpener()
    if success == True:
        cookieStr = opener._appendCookie(tmpCookie)
        url = 'http://weibo.cn/search/?vt=4'        
        if url != '':
            print opener.open(url,'#警察日记#',cookieStr)
    else:
        pass
        
    