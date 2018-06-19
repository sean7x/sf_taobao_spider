from bs4 import BeautifulSoup
import urllib.parse, json
import re, requests, pymysql, threading, traceback, random, codecs, time, sys


class product:
    pid = int()
    itemUrl = str()
    status = str()
    title = str()
    initialPrice = float()
    currentPrice = float()
    consultPrice = float()
    marketPrice = int()
    sellOff = int()
    start = int()
    end = int()
    timeToStart = int()
    timeToEnd = int()
    viewerCount = int()
    bidCount = int()
    delayCount = int()
    applyCount = int()
    xmppVersion = str()
    buyRestrictions = int()
    supportLoans = int()
    supportOrgLoan = int()

def closeDB():
    global conn,cur
    conn.close()
    cur.close()

def create_table(db):
    global conn, cur
    print('\n提示：正在创建{}表...'.format(db))
    cur.execute('''CREATE TABLE IF NOT EXISTS {}
                  (id INT PRIMARY KEY AUTO_INCREMENT, pid BIGINT NOT NULL, itemUrl VARCHAR(60), status VARCHAR(20),
                  title TEXT, initialPrice REAL, currentPrice REAL, consultPrice REAL, marketPrice REAL, sellOff TINYINT,
                  start BIGINT, end BIGINT, timeToStart BIGINT, timeToEnd BIGINT, viewerCount INT, bidCount INT, delayCount INT,
                  applyCount INT, xmppVersion VARCHAR(10), buyRestrictions TINYINT, supportLoans TINYINT, supportOrgLoan TINYINT, UNIQUE (pid))'''.format(db))

def save_data(db, pid, itemUrl, status, title, initialPrice, currentPrice, consultPrice, marketPrice, sellOff, start, end, timeToStart, timeToEnd, viewerCount, bidCount, delayCount, applyCount, xmppVersion, buyRestrictions, supportLoans, supportOrgLoan):
    if title != '' and title != None:
        global conn, cur
        sql = '''REPLACE INTO {} (pid, itemUrl, status, title, initialPrice, currentPrice, consultPrice, marketPrice, sellOff, start, end, timeToStart, timeToEnd, viewerCount, bidCount, delayCount, applyCount, xmppVersion, buyRestrictions, supportLoans, supportOrgLoan)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''.format(db)
        cur.execute(sql, (pid, itemUrl, status, title, initialPrice, currentPrice, consultPrice, marketPrice, sellOff, start, end, timeToStart, timeToEnd, viewerCount, bidCount, delayCount, applyCount, xmppVersion, buyRestrictions, supportLoans, supportOrgLoan))
        conn.commit()
    else: print('因数据为空，商品编号:', pid, '未保存')

def isexist(db, pid):
    global conn, cur
    cur.execute('SELECT pid FROM {} WHERE pid=%s AND status<>"todo"'.format(db), (pid,))
    exist = len(cur.fetchall()) > 0
    return exist

def randHeader():
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                        'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                        'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                        'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                        'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                        'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
                        'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                        'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11']

    header = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
    }
    return header

def getHTMLText(url):
    try:
        headers = randHeader()
        global con_counter
        # if con_counter%100 == 0 :
        #     proxy = proxies.proxies()
        if con_counter == 500 :
            print('已达到500次页面访问，请明天继续')
            sys.exit()
        r = requests.get(url, headers = headers, timeout=5)
        r.raise_for_status()
        con_counter += 1
        # r.encoding = 'utf-8'
        r.encoding = r.apparent_encoding
        return r.content
    except:
        print('Fail to getHTMLText')
        sys.exit()
        return ''

def getSoupObject(url):
    try:
        html = getHTMLText(url)
        soup = BeautifulSoup(html,'lxml')
        return soup
    except:
        print('Fail to getSoupObject')
        return ''

def getCounts(url):
    try:
        soup = getSoupObject(url)
        # a = soup('meta', {'name':'description'})[0]['content'] #从页面头取值
        # count = int(re.findall('共\D*(\d+.*\d*)条', a)[0].replace(',', '')) #从页面头取值
        count = int(soup('em', {'class': 'count'})[0].text.replace(',',''))
        page_count = int(soup('em', {'class': 'page-total'})[0].text)
        return count, page_count
    except:
        print('\n错误：获取总条数和总页数时出错...')
        return -1

def getProduct(db, page_count, surl):
    # fhand = codecs.open('pidlist.txt', 'a', "utf-8")
    # fhand.write(time.asctime(time.localtime(time.time()))+'\n')
    # print(length)
    count = 0
    exist = 0
    for i in range(1, int(page_count)):
        url = surl + '&page={}'.format(i)
        # print(url)
        try:
            soup = getSoupObject(url)
            data = soup('script', {'id':'sf-item-list-data', 'type':'text/json'})[0].text
            js = json.loads(data)
            # print(json.dumps(js, indent=4))
        except Exception as e:
            print('获取商品清单失败')
            print('错误:', e)
            print('')
            print(data)
            break
        except KeyboardInterrupt:
            print('')
            print('程序被用户中止...')
            break
        for p in js['data']:
            pdata = product()
            pdata.pid = p["id"]
            if isexist(db, pdata.pid) :
                exist += 1
                continue
            pdata.itemUrl = p["itemUrl"]
            pdata.status = p["status"].lower().strip()
            pdata.title = p["title"]
            pdata.initialPrice = p["initialPrice"]
            pdata.currentPrice= p["currentPrice"]
            pdata.consultPrice = p["consultPrice"]
            pdata.marketPrice = p["marketPrice"]
            if p["sellOff"] == False: pdata.sellOff = 0
            elif p["sellOff"] == True: pdata.sellOff = 1
            pdata.start = p["start"]
            pdata.end = p["end"]
            pdata.timeToStart = p["timeToStart"]
            pdata.timeToEnd = p["timeToEnd"]
            pdata.viewerCount = p["viewerCount"]
            pdata.bidCount = p["bidCount"]
            pdata.delayCount = p["delayCount"]
            pdata.applyCount = p["applyCount"]
            pdata.xmppVersion = p["xmppVersion"]
            pdata.buyRestrictions = p["buyRestrictions"]
            pdata.supportLoans = p["supportLoans"]
            pdata.supportOrgLoan = p["supportOrgLoan"]
            save_data(db, pdata.pid, pdata.itemUrl, pdata.status, pdata.title, pdata.initialPrice, pdata.currentPrice, pdata.consultPrice, pdata.marketPrice, pdata.sellOff, pdata.start, pdata.end, pdata.timeToStart, pdata.timeToEnd, pdata.viewerCount, pdata.bidCount, pdata.delayCount, pdata.applyCount, pdata.xmppVersion, pdata.buyRestrictions, pdata.supportLoans, pdata.supportOrgLoan)
            count += 1
            print('\r提示：正在存入商品数据,已处理{}个'.format(count), end='')
        time.sleep(3+random.randint(0, 3))
    # fhand.close()
    print('\n', count, '个商品获取成功')
    print('\n', exist, '个商品已存在')

def main():
    db = 'db_list'
    create_table(db)

    surl = 'https://sf.taobao.com/item_list.htm?'
    # category = 50025969 #(50025969:房产)
    # city = '苏州'
    # location_code = None #(320595:苏州/园区)
    # sorder = None #(-1:不限, 0:正在进行, 1:即将开始, 2:已结束, 4:中止, 5:撤回)
    # auction_start_seg = -1 #(-1:不限, 3:最近3天, 7:最近7天, 30:最近30天)
    # url = surl + urllib.parse.urlencode({'category':category, 'city':city, 'location_code':location_code, 'sorder':sorder, 'auction_start_seg':auction_start_seg}, encoding='GBK')

    keyword = input('请输入搜索关键词: ')
    url = surl + urllib.parse.urlencode({'q':keyword}, encoding='GBK')

    print('\n提示：开始爬取淘宝数据...')

    count,page_count = getCounts(url)
    print('共计', count, '条')
    print('共计', page_count, '页')

    getProduct(db, page_count, url)


con_counter = 0

try:
    conn = pymysql.connect(host='116.62.190.97', port=3306, user='remote', passwd='ali545783Tx.*', db='sf_taobao',charset="utf8")
    cur = conn.cursor()
except:
    sys.exit('\n错误：数据库连接失败')

main()

closeDB()
