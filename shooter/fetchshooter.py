#!-*- encoding:utf8 -*-
'''
Created on 2012-2-7

@author: Legend
爬取射手网所有字幕
保存至本地
并将可用信息入库
'''
from BeautifulSoup import BeautifulSoup, Tag
import urlparse, urllib, urllib2
import os, sys, platform
from Queue import Queue
import datetime, time
import threading
import logging
#import MySQLdb
#import libxml2
import hashlib
import random
import conf

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='./url.log',
                filemode='w')


#Producer thread
class Producer(threading.Thread):  
    
    def __init__(self, t_name, queue):  
        threading.Thread.__init__(self, name=t_name)  
        self.data = queue  

    def run(self):

        for i in range(conf.numstart, conf.numend):
            newurl = conf.baseurl.replace("xxx", str(i))
            for j in range(1, conf.numsub):
                url = newurl.replace("yyy", str(i * 1000 + j))
                initurl = url
                print "start URL:%s " % url
                
                """
                *    subId - 射手字幕ID
                *    subName - 射手字幕Title
                *    subType - 字幕类型
                *    subLeg - 字幕语种
                *    subTime - 字幕上传时间
                *    subFile - 字幕文件名
                *
                *    initurl - 初始爬取的URL
                *    subStatus - 字幕的下载状态
                *    url - 真实下载链接
                """
                
                content = self.getContent(url)
                if content == -1:
                    continue
#                print content
                subId = content[0]
                
#                判断sid是否已经在库中，和状态。未成功的继续下载
                data = {'action':'select', 'subId':subId}
                req = postDB(data)
                subStatus = req
                
                if req == "1":#已经下载成功
                    print "SubId:%s Have been Downloaded! " % subId
                    continue
                else:
                    url = self.shtgdownfile(subId)
                    if url == -1:
                        print "GetTrueUrlException! URL:%s" % initurl
                        continue
                    print "TrueURL:%s" % url
                    
                    content.append(url)
                    content.append(subStatus)
                    print "%s: %s is producing %s to the queue!" % (time.ctime(), self.getName(), content)
                    
                    self.data.put(content)
                    time.sleep(1)
    
                    if self.data.qsize() > 5:
                        Flag = True
                    else:
                        Flag = False
                    
                    while Flag:
                        if self.data.qsize() > 5:
                            print "Producer Queue's Size > 5,sleep 5 second ..."
                            time.sleep(5)
                        else:
                            Flag = False
                        
        print "%s: %s finished!" % (time.ctime(), self.getName())
        

    #获取字幕信息
    def getContent(self, url):
#        Proxy()
#        proxy = ip
#        print "Producer getProxySuccess! %s is Good! do Next..." % proxy
        try:
#            proxy_support = urllib2.ProxyHandler({"http" : 'http://%s:%s' % (proxy[0], proxy[1])})
#            opener = urllib2.build_opener(proxy_support)
#            urllib2.install_opener(opener)
            
            f = urllib2.urlopen(url)
            req = f.read()
            
            soup = BeautifulSoup(req)
            content = soup.findAll(attrs={"name":"readonlycounter2"})
            subId = content[0].string.split(',')[1]
            subName = soup.html.body.h1.span.string
            
            content = soup.findAll(attrs={"class":"subdes_td"})
            subType = content[0].string
            subLeg = content[1].string
            
            content = soup.findAll(attrs={"colspan":"3"})
            subTime = content[2].string
            subFile = content[7].div.string
            
            req = [subId, subName, subType, subLeg, subTime, subFile, url]
            
            print "SubId:%s SubName:%s SubType:%s SubLanguage:%s subFile:%s SubTime:%s URL:%s" % (subId, subName, subType, subLeg, subFile, subTime, url)
            return req
        except Exception:
            print "GetContentException! URL:%s" % url
            req = -1
        return req

    #获取加密字符串
    def shtgdownfile(self, str):
        try:
            url = "http://shooter.cn/files/file3.php?hash=duei7chy7gj59fjew73hdwh213f&fileid=" + str
            str = urllib2.urlopen(url)
            str = str.read()
            newstr = shtg_calcfilehash(str)
            url = "http://file.shooter.cn" + newstr
            return url
        except Exception:
            url = -1
            print "GetTrueUrlException!"
            return url


#Consumer thread  
class Consumer(threading.Thread):  
    def __init__(self, t_name, queue):  
        threading.Thread.__init__(self, name=t_name)  
        self.data = queue  

    def run(self):  

        while True:
            if self.data.qsize() > 0:
                content = self.data.get()
                print "%s: %s is consuming. %s in the queue is consumed!" % (time.ctime(), self.getName(), content)  
                self.download(content)
#                time.sleep(10)
            else:
                time.sleep(5)
                print "Consumer Queue's Size is NULL ,sleep 5 second ..."
                
        print "%s: %s finished!" % (time.ctime(), self.getName())

    #文件下载
    def download(self, content):
#        Proxy()
#        proxy = ip
#        print "Consumer getProxySuccess! %s is Good! do Next..." % proxy
        
        subId = content[0]
        subName = content[1]
        subType = content[2]
        subLeg = content[3]
        subTime = content[4]
        subFile = content[5]
        initUrl = content[6]
        url = content[7]
        subStatus = content[8]
        
        #判断sid是否已经在库中，是写入还是更新
        if subStatus == "2":
            action = "insert"
        if subStatus == "0":
            action = "update"
        print action
        
        length = self.GetHttpFileSize(url)#文件大小
        print "FileSize:%s" % length
        if not subFile:
            filename = self.getFileName(url)#文件名
            print "FileName:%s" % filename
        else:
            filename = subFile
        
        try:
#            proxy_support = urllib2.ProxyHandler({"http" : 'http://%s:%s' % (proxy[0], proxy[1])})
#            opener = urllib2.build_opener(proxy_support)
#            urllib2.install_opener(opener)
#            url = urllib2.Request(url)
#            f = opener.open(url, timeout=120)
#            print f.info()
#            header = urllib2.urlparse.urlparse(url)
#            print header
#            try:
            print "DownLoadFileBegin:%s -  Time:%s" % (filename, time.ctime())

            outpath = self.getOutPath(filename, initUrl)
            if not outpath:
                outpath = conf.datapath + "defult" + splitchar + filename

            print "DownLoadFilePath:%s" % outpath

            f = urllib2.urlopen(url, timeout=120)
            file = open(outpath, 'wb')
            req = f.read()
            file.write(req)
            print "DownLoadFileEnd:%s -  Time:%s" % (filename, time.ctime())
            msg = "DownLoadFileSuccess! | SubId:%s | FileName:%s | SubName:%s | SubType:%s | SubLanguage:%s | subFile:%s | FileSize:%s | SubTime:%s | InitUrl:%s | URL:%s | Time:%s" % (subId, filename, subName, subType, subLeg, subFile, length, subTime, initUrl , url, time.ctime())
            logging.info(msg)
            print msg
            file.close()
            
            #信息入库
            data = {'action':action,
                    'subId':subId,
                    'subStatus':"1",
                    'filename':filename,
                    'subName':subName,
                    'subType':subType,
                    'subLeg':subLeg,
                    'subFile':subFile,
                    'subTime':subTime,
                    'subSize':length,
                    'initUrl': initUrl,
                    'url': url,
                    'subMD5':hashlib.md5(initUrl).hexdigest()}
            
            res = postDB(data)
            
#            except Exception:
#                print "DownLoadFileException!"
#                pass
            
        except Exception:
            msg = "DownLoadFileException! SubId:%s | FileName:%s | SubName:%s | SubType:%s | SubLanguage:%s | subFile:%s | FileSize:%s | SubTime:%s | InitUrl:%s | URL:%s | Time:%s" % (subId, filename, subName, subType, subLeg, subFile, length, subTime, initUrl , url, time.ctime())
            logging.info(msg)
            print msg
            #信息入库
            data = {'action':action,
                    'subId':subId,
                    'subStatus':"0",
                    'subName':subName,
                    'subType':subType,
                    'subLeg':subLeg,
                    'subFile':subFile,
                    'subTime':subTime,
                    'subSize':length,
                    'initUrl': initUrl,
                    'url': url,
                    'subMD5':hashlib.md5(initUrl).hexdigest()}
            
            res = postDB(data)
#            sys.exc_info()
            pass

    #目录操作
    def getOutPath(self, filename, initUrl):
        try:
            #目录操作
            dirname = hashlib.md5(initUrl).hexdigest()[0:1]
            path = conf.datapath + dirname
            if not os.path.exists(path):
                os.makedirs(path)
            return path + splitchar + filename
        except Exception:
            print "MkdirError:! FileName:%s" % filename 
            pass

    #文件大小
    def GetHttpFileSize(self, url):
        length = 0
        try:
            conn = urllib.urlopen(url)
            headers = conn.info().headers
            for header in headers:
                if header.find('Length') != -1:
                    length = header.split(':')[-1].strip()
                    length = int(length)
        except Exception, err:
            print "GetHttpFileSizeException!"
            pass
        return length


    #获取文件名
    def getFileName(self, url):
        try:
            filename = url.split("?")[0].split("/")[-1]
            if filename:
                pass
            else:
                filename = url[:-10]
            return filename
        except Exception:
            print "getFileNameException!"
            pass


#代理
class Proxy:

    def __init__(self):
        proxy = self.readProxy()
        if self.validateProxy(proxy):
            self.setProxy(proxy)
        else:
            print "getProxyError! %s is Bad! GetAgain..." % proxy
            self.__init__()

     #读取代理
    def readProxy(self):
        try:
            filename = conf.proxyfile
            f = file(filename, 'r')
            data = f.readlines()
            num = len(data)
            req = data[random.randrange(num)]
            proxy = req.split()[0].split(":")
            return proxy
        except Exception:
            print "ReadProxyException!"

    #判断代理是否可用
    def validateProxy(self, proxy):
        if proxy:
            try:
                proxy_support = urllib2.ProxyHandler({"http" : 'http://%s:%s' % (proxy[0], proxy[1])})
                opener = urllib2.build_opener(proxy_support)
                urllib2.install_opener(opener)
                f = urllib2.urlopen('http://www.baidu.com', timeout=5)
                req = f.read()
                if len(req) > 5000:
                    return True
                else:
                    return False
            except Exception:
                return False
        else:
            return False

    #设置代理
    def setProxy(self, proxy):
        global ip
        ip = proxy

def postDB(data):
    try:
        req = urllib2.Request(conf.posturl)
#        req = urllib2.Request("http://localhost:8088/test/shooter.php")
        data = urllib.urlencode(data)
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
        res = opener.open(req, data)
        rs = res.read()
        print rs
        return rs

    except Exception:
        print "postDBException Data:%s " % data
        pass

#射手真实URL获取
def shtg_calcfilehash(str):
    a = str
    if (len(a) > 32):
        case = a[0]
        if case == "o":
            return (b((c(a[1:], 8, 17, 27))))
        if case == "n":
            return (b(d(c(a[1:], 6, 15, 17))))
        if case == "m":
            return (d(c(a[1:], 6, 11, 17)))
        if case == "l":
            return (d(b(c(a[1:], 6, 12, 17))))
        if case == "k":
            return (c(a[1:], 14, 17, 24))
        if case == "j":
            return (c(b(d(a[1:])), 11, 17, 27))
        if case == "i":
            return (c(d(b(a[1:])), 5, 7, 24))
        if case == "h":
            return (c(b(a[1:]), 12, 22, 30))
        if case == "g":
            return (c(d(a[1:]), 11, 15, 21))
        if case == "f":
            return (c(a[1:], 14, 17, 24))
        if case == "e":
            return (c(a[1:], 4, 7, 22))
        if case == "d":
            return (d(b(a[1:])))
        if case == "c":
            return (b(d(a[1:])))
        if case == "b":
            return (d(a[1:]))
        if case == "a":
            return b(a[1:])


def b(j):
    g = ""
    for f in range(0, len(j)):
        h = j[f]
        h = ord(h)
        g += (h + 47 >= 126) and chr(ord(" "[0]) + (h + 47) % 126) or chr(h + 47)
    return g


def d(g):

    j = len(g)
    j = j - 1
    h = ""
    for f in range(j, -1, -1):
        h += g[f]
    return h


def c(j, h, g, f):

    c = j[len(j) - f + g - h:len(j) - f + g - h + h] + \
    j[len(j) - f:len(j) - f + g - h] + \
    j[len(j) - f + g:len(j) - f + g + f - g] + \
    j[0 : len(j) - f]
    return c


#Main thread  
def main():  
  
    queue = Queue()
    producer = Producer('Pro.', queue)
    consumer = Consumer('Con.', queue)
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()

    print 'All threads terminate!'  


if __name__ == '__main__':
    sysstr = platform.system()
    global splitchar
    if(sysstr == "Windows"):
        splitchar = "\\"
        print ("Welcome Windows platform!")
    elif(sysstr == "Linux"):
        splitchar = "/"
        print ("Welcome Linux platform!")
    main()
#nohup python fetchshooter.py > run.log &
