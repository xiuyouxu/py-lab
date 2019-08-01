# -*- coding:utf-8 -*-
import json
import sqlite3
import threading
import time

import requests
from bs4 import BeautifulSoup


class SQLiteWrapper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """

    def __init__(self, path, command='', *args, **kwargs):
        self.lock = threading.RLock()  # 锁
        self.path = path  # 数据库连接参数

        if command != '':
            conn = self.get_conn()
            cu = conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)  # ,check_same_thread=False)
        conn.text_factory = str
        return conn

    def conn_close(self, conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self, *args, **kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self, *args, **kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs

        return connection

    @conn_trans
    def execute(self, command, method_flag=0, conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0], command[1])
            conn.commit()
        except sqlite3.IntegrityError as e:
            print(e)
            return -1
        except Exception as e:
            print(e)
            return -2
        return 0

    @conn_trans
    def fetchall(self, command="select name from xiaoqu", conn=None):
        cu = conn.cursor()
        lists = []
        try:
            cu.execute(command)
            lists = cu.fetchall()
        except Exception as e:
            print(e)
            pass
        return lists


# create db house:
#        id, 链家编号
#        price, 报价
#        unitprice, 每平米单价
#        community, 小区名称
#        district, 区县
#        region, 街道区域
#        area, 具体区域
#        url, 链接地址
#        huxing, 房屋户型
#        louceng, 所在楼层
#        jianmian, 建筑面积
#        hugou, 户型结构
#        taomian, 套内面积
#        jianxing, 建筑类型
#        chaoxiang, 房屋朝向
#        jiangou, 建筑结构
#        zhuangxiu, 装修情况
#        tihu, 梯户比例
#        gongnuan, 供暖方式
#        dianti, 配备电梯
#        chanquan, 产权年限
#        guapai, 挂牌时间
#        quanshu, 交易权属
#        shangci, 上次交易
#        yongtu, 房屋用途
#        nianxian, 房屋年限
#        chanshu, 产权所属
#        diya, 抵押信息
#        fangben, 房本备件
command = '''create table if not exists house (
            id TEXT primary key UNIQUE,
            price TEXT,
            unitprice TEXT,
            community TEXT,
            district TEXT,
            region TEXT,
            area TEXT,
            url TEXT,
            huxing TEXT,
            louceng TEXT,
            jianmian TEXT,
            hugou TEXT,
            taomian TEXT,
            jianxing TEXT,
            chaoxiang TEXT,
            jiangou TEXT,
            zhuangxiu TEXT,
            tihu TEXT,
            gongnuan TEXT,
            dianti TEXT,
            chanquan TEXT,
            guapai TEXT,
            quanshu TEXT,
            shangci TEXT,
            yongtu TEXT,
            nianxian TEXT,
            chanshu TEXT,
            diya TEXT,
            fangben TEXT
)'''

# command = "create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT)"
# db_cj = SQLiteWrapper('lianjia-chengjiao.db', command)

url = 'https://bj.lianjia.com/ershoufang/'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

districts = ['dongcheng', 'xicheng', 'chaoyang', 'haidian', 'fengtai',
             'shijingshan', 'tongzhou', 'changping', 'daxing', 'mentougou']

info_list = [u'id', u'price', u'unitprice', u'community', u'district',
             u'region', u'areaName', u'url', u'房屋户型', u'所在楼层',
             u'建筑面积', u'户型结构', u'套内面积', u'建筑类型', u'房屋朝向',
             u'建筑结构', u'装修情况', u'梯户比例', u'供暖方式', u'配备电梯',
             u'产权年限', u'挂牌时间', u'交易权属', u'上次交易', u'房屋用途',
             u'房屋年限', u'产权所属', u'抵押信息', u'房本备件']


def gen_house_insert_command(info):
    """
    生成房屋数据库插入命令
    """
    t = []
    for il in info_list:
        if il in info:
            t.append(info[il])
        else:
            t.append('')
    t = tuple(t)
    command = (
        r"insert into house values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        t)
    return command


def crawl_detail(db_house, district, region, soup):
    houses = soup.find('ul', {'class': 'sellListContent'}).findAll('li')
    house_data = []
    for house in houses:
        href = ''
        try:
            href = house.find('a', {'data-el': 'ershoufang'}).attrs['href']
            r = requests.get(href, headers=headers)
            txt = r.content.decode('utf-8')
            s = BeautifulSoup(txt, features='lxml')

            house_info = {'district': district, 'region': region, 'url': href}

            priceInfo = s.find('div', {'class': 'price'})
            price = priceInfo.find('span', {'class': 'total'}).string
            unitPrice = priceInfo.find('span',
                                       {'class': 'unitPriceValue'}).find(
                'i').previous_sibling
            aroundInfo = s.find('div', {'class': 'aroundInfo'})
            community = \
                aroundInfo.find('div', {'class': 'communityName'}).findAll('a')[
                    0].string
            areaName = \
                aroundInfo.find('div', {'class': 'areaName'}).findAll('span')[
                    1].text
            id = aroundInfo.find('div', {'class': 'houseRecord'}).find('span',
                                                                       {
                                                                           'class': 'info'}).find(
                'span').previous_sibling

            house_info['price'] = price
            house_info['unitprice'] = unitPrice
            house_info['community'] = community
            house_info['areaName'] = areaName
            house_info['id'] = id

            introContent = s.find('div', {'class': 'introContent'})
            base_infos = introContent.find('div', {'class': 'base'}).findAll(
                'li')
            for base_info in base_infos:
                label = base_info.find('span')
                value = label.next_sibling
                house_info[label.string] = value.strip()
            transactions = introContent.find('div',
                                             {'class': 'transaction'}).findAll(
                'li')
            for transaction in transactions:
                spans = transaction.findAll('span')
                label = spans[0].string
                value = spans[1].string
                house_info[label] = value.strip()

            # save to db
            # command = gen_house_insert_command(house_info)
            # db_house.execute(command, 1)

            house_data.append(house_info)

            if len(house_data) == 10:
                # save to db
                for info in house_data:
                    command = gen_house_insert_command(info)
                    db_house.execute(command, 1)
                house_data = []
                time.sleep(0.3)
        except Exception as e:
            print(href, e)


# r = requests.get(url, headers=headers)
# txt = r.content.decode("utf-8")
# soup = BeautifulSoup(txt, features="lxml")
# districts = soup.find('div', {'data-role': 'ershoufang'}).div.findAll('a')
# list all the districts
# for d in districts:
#     href = d.attrs['href']
#     dnames = href.split('/')
#     district = dnames[2]
#     print(dnames[2])

for district in districts:
    db_house = SQLiteWrapper('lianjia-house-%s.db' % district, command)
    try:
        r = requests.get(url + district + '/', headers=headers)
        txt = r.content.decode('utf-8')
        soup = BeautifulSoup(txt, features='lxml')
        regions = soup.find('div', {'data-role': 'ershoufang'}).findAll('div')[
            1].findAll('a')
        print(district)
        for region in regions:
            href = region.attrs['href']
            rnames = href.split('/')
            print('\t%s' % rnames[2])

            # get first page
            r = requests.get(url + rnames[2] + '/', headers=headers)
            txt = r.content.decode('utf-8')
            soup = BeautifulSoup(txt, features='lxml')
            # get details
            crawl_detail(db_house, district, rnames[2], soup)

            pd = soup.find('div', {'class': 'page-box house-lst-page-box'})
            if pd is not None:
                d = json.loads(pd.get('page-data'))
                total_pages = d['totalPage']
                for i in range(2, total_pages):
                    r = requests.get(url + rnames[2] + '/pg%d' % i,
                                     headers=headers)
                    txt = r.content.decode('utf-8')
                    soup = BeautifulSoup(txt, features='lxml')
                    # get details
                    crawl_detail(db_house, district, rnames[2], soup)
    except Exception as e:
        print(e)

# file = open('test.html', 'w', encoding='utf-8')
# file.write(r.content.decode("utf-8"))
