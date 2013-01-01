'''
Created on 2012-12-20

@author: Tony
'''
#coding:utf-8

import re , urllib2 , threading , urlparse , hashlib
import Queue , sqlite3 , logging , time , os 
from optparse import OptionParser 
from BeautifulSoup import BeautifulSoup


threadsPool = []

class Spider(object):
    def __init__(self,begin_url = "www.baidu.com",depth = 2,logfile = "log.txt",loglevel = 3,\
                 thread_num = 10,database_file = "spider.db",key = "Nome"):
        #initialize the class
        self.in_queue  = 0                                  #the number of the url input the queue
        self.out_queue = 0                                  #the number of the url output the queue
        self.task_queue = Queue.Queue()                     #be careful here
        self.curr_url = None                                #the current url
        self.curr_page = None                               #the current web page
        self.db = None                                      
        self.urls = {}                                      #the set of urls
        self.visited = []                                   #the urls which were visited
        self.begin_url = begin_url                          #the begin url
        self.depth = depth                                  #the depth of the spider path
        self.logfile = logfile                              #the path of the log 
        self.loglevel = loglevel                            #the level of the log
        self.thread_num = thread_num                        #the number of the threads
        self.database_file = database_file                  #the path of the database file
        self.key = key                                      #the key words

    def spider(self):                                       #the spider  
        while True:
            try:
                url = self.task_queue.get(timeout=10)       #get the url from the task_queue
                self.out_queue += 1                         
                self.task_queue.task_done()
                self.urls_get(url)                          #the function of search the urls from the web page
            except Queue.Empty:
                print 'kill %s'%threading.currentThread().getName()
                break
               
    def conn(self):                                         #create the database and create a table named spider_url       
        dfn = self.database_file                            #if the file is already there  delete it first
        if os.path.exists(dfn):
            os.remove(dfn)
        self.db = sqlite3.connect(self.database_file)
        self.db = sqlite3.connect(':memory:', check_same_thread = False)
        self.db.text_factory = str
        cu = self.db.cursor()                               #get the cursor
        cu.execute("create table data(id INTEGER PRIMARY KEY  AUTOINCREMENT ,url varchar(100),key varchar(100),html varchar(1000))") #create the table of the database

    def urls_get(self,url):                                 #search the urls from the web page
        if url == self.begin_url:
            url = urlparse.urlunparse( ('http',self.begin_url,'','','','') )#spilt joint of the URL
            print "the spider begin with:" + url + "\n"
        
        content = urllib2.urlopen(url).read()               
        self.curr_url = url                                 #get the page url
        self.curr_page = content                            #get the page content
        self.urls = re.findall(r'href=[\'"]?([h][^\'" >]+)', content) #use the ce to find the useful urls
        print '\n'.join(self.urls)                          
        put = open("output.txt",'w+')                       #write it into the text
        put.write('\n'.join(self.urls))
        for i in self.urls:
            if hashlib.md5(i).hexdigest() in self.visited:  #delete the same url   use the md5 to compress the urls
                continue
            else:
                self.visited.append(hashlib.md5(i).hexdigest())
            self.task_queue.put(i)
            self.in_queue += 1
        print "urls.len:" + str(self.urls.__len__())
        cu = self.db.cursor()

    def run(self):
        while True:
            if self._dismissed.isSet():
                break    
            print "urls.inqueue:" + str(self.in_queue)
            print "urls.outqueue:" + str(self.out_queue)
            time.sleep(10)

usage = """For example:

    python mspider.py -u sina.com.cn -d 2 -f logfile

testing:

    python mspider.py --testself

"""
       
def main():
    parser = OptionParser(usage)                            #to set the parameters
    options_a = [
        ["-u",        "--url",          dict(dest="begin_url", type="string", default="www.baidu.com", help="the beginning URL")],
        ["-d",        "--depth",        dict(dest="depth",     type="int", default=2,             help="the depth of the spider")],
        ["-t",        "--thread",       dict(dest="thread_num",type="int", default=10,            help="the number of threads")],
        ["-s",        "--dbfile",       dict(dest="database_file",type="string", default="spider.db", help="the path of the database file")],
        ["-k",        "--key",          dict(dest="key",       type="string", default="None",        help="the key words")],
        ["-f",        "--logfile",      dict(dest="logfile",   type="string", default="log.txt",     help="file of URLs to crawl")],
        ["-l",        "--loglevel",     dict(dest="loglevel",  type="int", default=3,             help="the level of the logger")]
    ]
    for s, l, k in options_a:
        parser.add_option(s, l, **k)
    (options, args) = parser.parse_args()

    kwargs = {
        'begin_url' : options.begin_url,
        'depth'     : options.depth,
        'thread_num': options.thread_num,
        'database_file': options.database_file,
        'key'       : options.key,
        'logfile'   : options.logfile,
        'loglevel'  : options.loglevel
    }
    print parser.usage
    spider = Spider(**kwargs)                               #build the instance of the Spider
    spider.conn()                                           #connect the database
    spider.urls_get(spider.begin_url)                       #begin to search the urls 

    for i in range(spider.thread_num):                      #set the thread pool?
        thread = threading.Thread(target=spider.spider)
        thread.daemon = True
        threadsPool.append(thread)
        thread.start()
        
    for _ in threadsPool:                                   #waiting for the all thread finished
        _.join()
        
if __name__ == '__main__':                          
    print "hello python!\n"                                 # ^_^ hello python!  take notes for my first python program
    main()
    pass
