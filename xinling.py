# coding:utf-8
# dev(redis cache) version on 2017-10-1, please use stable version on master branch

import os
import pymysql
import requests
import socket
import sys
import time
import traceback
from EasyLogin import EasyLogin  # 安装依赖：pip install bs4 requests pymysql
from pprint import pprint, pformat
from time import sleep
from bs4 import BeautifulSoup
from config import COOKIE, db, redis_conn, enable_multiple_ip, CONFIG_INTERESTING_BOARDS, CONFIG_IGNORE_POSTS
from mpms import MPMS  # 虽然本项目里面已经包含mpms的代码，你也可以pip install mpms，项目地址：https://github.com/aploium/mpms

"""
欢迎阅读chenyuan写的cc98爬虫代码, 我假设你已经安装了以下软件/阅读了以下文档：
1) python3 https://python.org/
2) mysql https://www.mysql.com/
3) requests: 让 HTTP 服务人类 http://cn.python-requests.org/zh_CN/latest/
4) Beautiful Soup 文档 https://www.crummy.com/software/BeautifulSoup/bs4/doc.zh/
5) PyMySQL documentation: https://pymysql.readthedocs.io/en/latest/
6) EasyLogin: 这个是我为了简化对requests和BeautifulSoup的调用而写的库 https://github.com/zjuchenyuan/EasyLogin
7) mpms: 多进程多线程并发 https://github.com/aploium/mpms
8) redis: 存储数据结构的内存NoSQL数据库
9) redis python package: https://pypi.python.org/pypi/redis

代码阅读建议：
    建议使用PyCharm(https://www.jetbrains.com/pycharm/)阅读python代码，哪里不懂按住Ctrl点哪里
"""


def function_hook_parameter(oldfunc, parameter_index, parameter_name, parameter_value):
    """
    这个函数只是用于辅助实现多IP爬取，并不是很重要，你可以放心地跳过此函数继续阅读

    创造一个wrapper函数，劫持oldfunc传入的第parameter_index名为parameter_name的函数，固定其值为parameter_value; 不影响调用该函数时传入的任何其他参数
    用法： 原函数 = function_hook_parameter(原函数, 从1开始计数的参数所处的位置, 这个参数的名称, 需要替换成的参数值)

    例子： 需要劫持socket.create_connection这个函数，其函数原型如下： 
               create_connection(address, timeout=_GLOBAL_DEFAULT_TIMEOUT, source_address=None)
           需要对其第3个参数source_address固定为value，劫持方法如下
               socket.create_connection = function_hook_parameter(socket.create_connection, 3, "source_address", value)
    """
    real_func = oldfunc

    def newfunc(*args, **kwargs):  # args是参数列表list，kwargs是带有名称keyword的参数dict
        newargs = list(args)
        if len(args) >= parameter_index:  # 如果这个参数被直接传入，那么肯定其前面的参数都是无名称的参数，args的长度肯定长于其所在的位置
            newargs[parameter_index - 1] = parameter_value  # 第3个参数在list的下表是2
        else:  # 如果不是直接传入，那么就在kwargs中 或者可选参数不存在这个参数，强制更新掉kwargs即可
            kwargs[parameter_name] = parameter_value
        return real_func(*newargs, **kwargs)

    return newfunc


if enable_multiple_ip:  # 是否启用多IP轮换爬取，一般设置为 False
    from config import myip  # myip是一个目前操作系统已经获得的IP，至于Linux如何获得多个IP可以参考：https://py3.io/Linux-setup.html#ip-1

    socket.create_connection = function_hook_parameter(socket.create_connection, 3, "source_address", (myip, 0))
    requests.packages.urllib3.util.connection.create_connection = function_hook_parameter(
        requests.packages.urllib3.util.connection.create_connection,
        3,
        "source_address",
        (myip, 0)
    )
    # 我就是在PyCharm里一步步Ctrl点击发现的这requests复制了一份socket.create_connection的代码并加上了新功能 从而绕过了socket.create_connection，藏的这么深Orz
else:
    myip = ""

DOMAIN = "http://www.cc98.org"  # 假设当前网络能访问到本域名

conn = db()  # 建立数据库连接，如果数据库连接失败 不处理异常 直接退出
myredis = redis_conn()  # 建立redis连接


def createTable(boardid, big=""):
    """
    建表函数 需要传入板块id 和 大表前缀("big"或"")，尝试进行建表sql语句，忽视错误(如表已经存在)
    :param boardid: 板块id，如"100"
    :param big: 传入空字符串表示普通表如bbs_100，传入"big"表示历史大表 如bigbbs_100
    :return:
    """
    sql = """
CREATE TABLE `{big}bbs_{boardid}` (
  `id` int(11) NOT NULL,
  `lc` int(255) NOT NULL,
  `posttime` datetime NOT NULL,
  `edittime` datetime NOT NULL,
  `user` varchar(66) NOT NULL,
  `content` longtext NOT NULL,
  `gettime` datetime NOT NULL,
  PRIMARY KEY (`id`,`lc`,`edittime`,`posttime`,`user`),
  KEY `a1` (`posttime`),
  KEY `a2` (`user`),
  KEY `a3` (`gettime`),
  KEY `a4` (`id`),
  KEY `a5` (`lc`),
  KEY `a6` (`edittime`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
""".format(big=big, boardid=boardid)
    global conn
    conn = db()  # 强制重新与数据库重新连接 TODO: 是否有必要？
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except:
        pass
    conn = db()


def getNewPost():
    """
    获得查看新帖5页所有的(帖子所在的板块id, 帖子id)
    :return: 如[["459","4726645"], ["562","4726644"], ...]
    """
    a = EasyLogin(cookie=COOKIE)
    result = []
    for page in range(5, 0, -1):
        a.get("{DOMAIN}/queryresult.asp?page={page}&stype=3".format(DOMAIN=DOMAIN, page=page))
        l = a.getList("dispbbs.asp")
        for i in l:
            if "http" in i:  # 跳过那些不是帖子链接的地址，如BUG反馈，其特征是包含http
                continue
            element = [getPart(i, "boardID=", "&"), getPart(i, "&ID=", "&")]
            if element not in result:  # 去重
                result.append(element)
    return result


def getHotPost():
    """
    获得现在的十大帖子（热门话题），返回list (帖子所在的板块id, 帖子id)
    :return: 如[["182", "4726542"], ["152", "4726482"], ...]
    """
    a = EasyLogin(cookie=COOKIE)
    result = []
    a.get("{DOMAIN}/hottopic.asp".format(DOMAIN=DOMAIN))
    l = a.getList("dispbbs.asp")
    for i in l:
        if "http" in i:
            continue
        element = [getPart(i, "boardid=", "&"), getPart(i, "&id=", "&")]
        if element not in result:
            result.append(element)
    return result


def getPart(source, left, right):
    """
    从字符串source中提取left与right之间的内容
    例如getPart("left-content-right", "left-", "-right")将返回"content"
    :param source: 源字符串
    :param left: 左边界 不会包含到结果中，假设左边界一定存在，否则将IndexError: list index out of range
    :param right: 右边界 同样也不会包含到结果中
    :return: left与right中间的内容
    """
    return source.split(left, maxsplit=2)[1].split(right)[0]


def getBoardSize(boardid):
    """
    返回板块共有多少页，例如 何钦铭老师答疑（boardid=413）有4页，对应提取的HTML如下：
    <td style="text-wrap: none; vertical-align: middle; margin: auto; text-align: left;">页次：<b>1</b>/<b>4</b>页 每页<b>20</b> 主题数<b>74</b></td>
    提取的是第二个<b>这里的4页
    :param boardid: 板块id
    :return: int类型，板块有多少页；如果没能找到，返回0
    """
    a = EasyLogin(cookie=COOKIE)
    a.get("{}/list.asp?boardid={}".format(DOMAIN, boardid))
    try:
        size = list(a.b.find('td', attrs={
            'style': 'text-wrap: none; vertical-align: middle; margin: auto; text-align: left;'}).find_all('b'))[1].text
    except:
        size = 0
    return int(size)


def getBoardPage_detailed(boardid, page):
    """
    输入板块的boardid和第page页，返回该页的帖子列表 list类型 (帖子所在的板块id, 帖子id)
    提取对应的HTML是<a href='dispbbs.asp?boardID=182&ID=4725833&star=1&page=1'>
    可以看出getNewPost, getHotPost和这里的getBoardPage返回类型是统一的，可以方便地加在一起
    :param boardid: 板块id 如182
    :param page: 第多少页，如1
    :return: 如[["182", "4725833"], ["182", "4726027"], ...]
    """
    # global myredis
    # redis_pipe = myredis.pipeline()
    a = EasyLogin(cookie=COOKIE)
    a.get("{}/list.asp?boardid={}&page={}".format(DOMAIN, boardid, page))
    result = set()
    for i in a.getList("dispbbs.asp?boardID=", returnType="element"):
        if "topic_" not in i.get("id", ""):
            continue
        body1 = i.find_next("td", {"class": "tablebody1"})
        try:
            reply, clicks = body1.text.strip().split("/")
        except:
            reply, clicks = str(-1), str(-1)
        lastpost = body1.find_next("td", {"class": "tablebody2"}).find("a").text.strip()
        postid = getPart(i["href"], "&ID=", "&")
        result.add((postid, reply, clicks, lastpost))
        # redis_pipe.set("reply_"+postid, reply).set("clicks_"+postid, clicks).set("lastpost_"+postid, lastpost)
    # print(redis_pipe.execute())
    return [(boardid, i[0], i[1], i[2], i[3]) for i in result]


def getBoardPage(boardid, page):
    return [(int(i[0]), int(i[1])) for i in getBoardPage_detailed(boardid, page)]


def getBBS(boardid, id, big, morehint=False):
    """
    核心的获取帖子内容函数
    result为[楼层lc, 用户名user, 发帖内容content, 发帖时间posttime, 最后编辑时间lastedittime]
    回帖的标题信息现在已经能获取，为保持之前数据的一致性，其楼层设置为负数

    :param boardid: 板块id
    :param id: 帖子id
    :param big: 一般为""，如果是全站爬取插入到历史大表中 则为"big"
    :param morehint: 是否显示更多进度显示
    :return: [boardid, id, result, big] 作为handler的传入参数
    """
    global myredis
    a = EasyLogin(cookie=COOKIE)
    result = []
    star = 1
    html = a.get("{}/dispbbs.asp?BoardID={}&id={}&star=1".format(DOMAIN, boardid, id))
    myredis.incr("clicks_" + str(id))
    try:
        number = int(a.b.find("span", attrs={"id": "topicPagesNavigation"}).find(
            "b").text)  # 假设<span id="topicPagesNavigation">本主题贴数 <b>6</b>
    except:
        return [boardid, id, [], big]
    pages = (number // 10 + 1) if number % 10 != 0 else number // 10  # 假设每页只有10个楼层
    lastpage = number - 10 * (pages - 1)
    startpage = 1
    if morehint == False and pages > 100:
        startpage = pages - 10
    for star in range(startpage, pages + 1):
        if star != 1:
            if morehint:
                print("page {star}".format(star=star))
            html = a.get("{}/dispbbs.asp?BoardID={}&id={}&star={}".format(DOMAIN, boardid, id, star))
            myredis.incr("clicks_" + str(id))
        else:
            title = a.b.title.text.strip(" » CC98论坛")  # 帖子标题使用页面标题，假设页面标题的格式为"title &raquo; CC98论坛"
            result.append([0, "", title, "1970-01-01 08:00:01", "1970-01-01 08:00:01"]) # dummy value fixed afterwards
            # print(title)
        floor_i = -1
        i = 0
        #for i in range(1, 11 if star != pages else lastpage + 1):  # 最后一页没有第lastpage+1个楼层
            # print(star,i)
        for floorpart in html.split("<!-- Execute Floor:"):
            floor_i += 1
            if floor_i == 0 or """<span style="color: red">热门回复""" in floorpart:
                continue
            i += 1
            floorpart = "<!-- Execute Floor:" + floorpart
            soup = BeautifulSoup(floorpart, "html.parser")
            
            lc = (star - 1) * 10 + i
            floorstart = soup.find("a")
            table = floorstart.next_sibling.next_sibling  # 假设楼层内容开始的table前都有<a name="1"></a>
            table_part2 = None
            lastedit = None
            for t in list(table.next_siblings)[0:20]:  # 由于BeautifulSoup太渣,事实上table还有一部分
                if "IP" in str(t):
                    table_part2 = t
                    break
                    # print(table_part2)
            user = table.find('b').text  # 假设表格中第一个加粗<b>的就是发帖用户名
            # print("{},{},{},{}".format(id,star,user,i))
            if table_part2 is not None:
                lastedit = table_part2.find("span", attrs=dict(
                    style="color: gray;"))  # 假设本楼层发生了编辑，最后的编辑时间<span style="color: gray;">本贴由作者最后编辑于 2016/10/28 21:33:56</span>

            lastedittime = " ".join(lastedit.text.split()[-2:]).replace("/",
                                                                        "-") if lastedit is not None else "1970-01-01 08:00:01"  # 没有编辑就返回0
            # print(lastedittime)
            posttime = table.find_next("td", attrs={"align": "center"}).get_text(strip=True).replace("/",
                                                                                                     "-")  # 发帖时间，注意find_next有可能找到下个楼层，希望没错         <td class="tablebody1" valign="middle" align="center" width="175">
            # 假设发帖时间的HTML：
            #               <a href=#>
            #               <img align="absmiddle" border="0" width="13" height="15" src="pic/ip.gif" title="点击查看用户来源及管理&#13发贴IP：*.*.*.*"></a>
            #       2016/10/28 21:32:45
            #           </td>

            # print(posttime)
            content_article = table.find('article')
            content_b = content_article.find('b')
            content_title = content_b.text.strip()
            if content_title != "":
                result.append([-lc, user, content_title, posttime, lastedittime])
            contentdiv = content_article.find('div')
            content = contentdiv.text if contentdiv is not None else ">>>No Content<<<"
            # print(content)
            result.append([lc, user, content, posttime, lastedittime])
            # break
        result[0][1], result[0][3], result[0][4] = result[1][1], result[1][3], result[1][4] # fix title user, posttime, edittime
    return [boardid, id, result, big]


def handler(meta, boardid, id, result, big):
    """
    将得到的数据插入数据库，本函数全局只会运行一份
    :param meta: 见mpms文档
    :param boardid: 板块id
    :param id: 帖子id
    :param result: 爬取的帖子内容 list类型 [楼层lc, 用户名user, 发帖内容content, 发帖时间posttime, 最后编辑时间lastedittime]
    :param big: 是否大表 ""或"big"
    :return: 无返回值
    """
    if len(result) == 0:
        return
    if len(result) > 1000:  # avoid too long sql
        handler(meta, boardid, id, result[1000:], big)
        result = result[:1000]
    if result[0][0] == 0:  # 由于避免太长sql的特性，result[0]可能不是帖子标题，判断不是标题就不要显示了
        try:
            showline = [boardid, id, result[0][2], len(result)]
            if myip != "":
                showline.insert(0, myip)  # if enables multiple ip, print IP first
            print(" ".join(str(i) for i in (showline)))
        except:
            try:
                print(" ".join(str(i) for i in (boardid, id, pformat(result[0][2]), len(result))))
            except:
                print("Something cannot print")
    global conn
    sql = "insert ignore into {}bbs_{}(id,lc,user,content,posttime,edittime,gettime) values ".format(big, boardid)
    for i in result:
        sql += "({},{},\"{}\",\"{}\",\"{}\",\"{}\",now()),".format(id, i[0],
                                                                   pymysql.escape_string(i[1]),
                                                                   pymysql.escape_string(i[2]), i[3], i[4])
    # print(sql)
    sql = sql[:-1]
    # 将数据库改为utf8mb4编码后，现在不再替换emoji表情
    cur = conn.cursor()
    try:
        cur.execute(
            "SET NAMES utf8mb4;SET CHARACTER SET utf8mb4; SET character_set_connection=utf8mb4;")  # 相应的这里要处理好编码问题
    except:
        conn = db()
        cur.execute("SET NAMES utf8mb4;SET CHARACTER SET utf8mb4; SET character_set_connection=utf8mb4;")
    try:
        cur.execute(sql)
        conn.commit()
    except pymysql.err.ProgrammingError as e:  # 这种错误就是还没有建表，先调用建表函数再插入
        createTable(boardid, big=big)
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)


def spyBoard_dict(boardid_dict, pages_input=None, sleeptime=86400, processes=2, threads=2):
    """
    对给定的板块id列表进行监测
    """
    m = MPMS(getBBS, handler, processes=processes, threads_per_process=threads)
    for boardid in boardid_dict:
        if pages_input is not None:
            pages = pages_input
        else:
            pages = getBoardSize(boardid)
        print("[board {}]Try to get {} pages".format(boardid, pages))
        for j in range(pages, 0, -1):
            thispage = getBoardPage(boardid, j)
            if thispage == []: break
            for i in thispage:
                m.put([boardid, i[1], "big"])
    sleep(sleeptime)
    return


def spyBoard(boardid=182, pages_input=None, sleeptime=86400, processes=2, threads=2):
    spyBoard_dict([boardid], pages_input, sleeptime, processes, threads)


myprint = lambda s: print("[{showtime}] {s}".format(showtime=time.strftime("%Y-%m-%d %H:%M:%S"), s=s))


def plus1(filename):
    """
    文件内容为一个int，打开文件+1，写入
    用于统计发生次数
    """
    try:
        with open(filename, "r") as fp:
            data = fp.read()
        result = int(data)
    except:
        result = 0
    result += 1
    with open(filename, "w") as fp:
        fp.write(str(result))

import random
def filter_pass(boardid, postid, reply, clicks, lastpost):
    global myredis, ignore_counts
    redis_pipe = myredis.pipeline()
    if (int(boardid), int(postid)) in CONFIG_IGNORE_POSTS:
        return False  # CONFIG_IGNORE_POSTS不会爬取
    oldclicks = myredis.get("clicks_" + str(postid))
    if bytes(clicks, encoding="utf-8") == oldclicks and clicks != "-1":  # 如果点击量为-1表示这是投票贴 没有好方法只能强制抓取
        ignore_counts += 1
        return False
    if oldclicks is not None:
        oldclicks_int = int(oldclicks.decode())
        clicks_int = int(clicks)
        if clicks_int - oldclicks_int < 2:
            ignore_counts += 1
            return False
        oldlastpost = myredis.get("lastpost_" + str(postid))
        if bytes(lastpost, encoding="utf-8") == oldlastpost:
            if random.randint(0,100)>30:
                ignore_counts += 1
                return False
    redis_pipe.set("reply_" + postid, reply).set("clicks_" + postid, clicks).set("lastpost_" + postid,
                                                                                 lastpost).execute()
    return True


def spyNew(sleeptime=100, processes=3, threads=3):
    """
    对热门、新帖以及额外配置的板块列表进行监测，这是直接运行代码将调用的函数
    """
    global ignore_counts
    ignore_counts = 0
    starttime = time.time()
    myprint("start")
    m = MPMS(getBBS, handler, processes=processes, threads_per_process=threads)
    t = 0
    workload = set()
    thenew = getHotPost() + getNewPost()
    boardlist = set([int(i[0]) for i in thenew])
    myprint("get new finished, len(thenew)={}, len(boardlist)={}".format(len(thenew), len(boardlist)))
    boardlist.update(CONFIG_INTERESTING_BOARDS)

    newclicksdata = []
    for boardid in boardlist:
        newclicksdata += getBoardPage_detailed(boardid, 1)

    for boardid, postid, reply, clicks, lastpost in newclicksdata:
        if filter_pass(boardid, postid, reply, clicks, lastpost):
            if postid not in workload:
                m.put([boardid, postid, ""])
                workload.add(postid)
    myprint("Check {} boards, ignore {} posts, using {} seconds".format(len(boardlist), ignore_counts,
                                                                        int(time.time() - starttime)))
    if time.time() - starttime > 10:
        myprint("too slow! add wait time")
        sleeptime += time.time() - starttime - 10
    while len(m) > 0:
        myprint("Remaning queue length: {len}".format(len=len(m)))
        sleep(2)
    myprint("All done! wait 5 seconds to clean up")
    sleep(5)
    myprint("Try close the queue... If this hang on, you have to kill the python process")
    plus1("tryclose.log")
    m.close()
    myprint("Try join the queue... If this hang on, you have to kill the python process")
    plus1("tryjoin.log")
    m.join()
    plus1("join_success.log")
    myprint("All child process exited succesfully")
    sleeptime = max(0, starttime + sleeptime - time.time())
    print("Sleep a while ( {sleeptime:.0f}s )...".format(sleeptime=sleeptime))
    sleep(sleeptime)
    myprint("Sleep done! wake up and exit...")
    return


def main():
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "all":
            # 所有版块id列表
            workset = [7, 16, 17, 19, 20, 21, 23, 26, 28, 36, 39, 41, 42, 47, 48, 49, 50, 52, 57, 58, 60, 67, 74, 75,
                       77, 83, 84, 115, 119, 126, 129, 140, 149, 151, 155, 157, 164, 165, 169, 170, 176, 178, 179, 180,
                       183, 187, 189, 190, 192, 193, 194, 195, 203, 204, 206, 207, 208, 211, 213, 214, 216, 217, 222,
                       224, 231, 232, 233, 234, 236, 241, 246, 247, 248, 252, 254, 255, 256, 258, 262, 263, 264, 266,
                       267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 281, 282, 283, 284, 285, 286,
                       287, 288, 290, 292, 295, 296, 303, 304, 306, 307, 308, 310, 311, 312, 315, 316, 319, 321, 325,
                       326, 328, 330, 331, 334, 339, 341, 344, 346, 347, 351, 352, 353, 355, 361, 362, 369, 371, 374,
                       375, 377, 383, 391, 392, 393, 401, 402, 403, 404, 405, 406, 410, 411, 414, 415, 416, 417, 418,
                       424, 425, 426, 428, 429, 430, 431, 432, 434, 436, 437, 440, 443, 444, 445, 446, 447, 448, 449,
                       450, 451, 452, 454, 457, 460, 462, 464, 468, 469, 471, 472, 473, 474, 475, 476, 477, 478, 479,
                       480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494, 495, 496, 497, 498,
                       499, 501, 502, 503, 504, 505, 506, 507, 509, 511, 513, 514, 515, 516, 517, 518, 519, 520, 535,
                       538, 540, 544, 545, 546, 548, 549, 550, 551, 552, 553, 554, 555, 557, 559, 560, 562, 563, 564,
                       568, 572, 574, 576, 578, 579, 583, 584, 585, 587, 588, 589, 590, 591, 592, 593, 595, 596, 597,
                       598, 599, 600, 601, 602, 603, 610, 611, 613, 615, 618, 620, 621, 622, 623, 624, 625, 626, 628,
                       629, 631, 632, 633, 634, 636, 637, 640, 642, 710, 711, 712, 713, 714, 716, 717, 718, 719, 720,
                       721, 722, 723, 724, 725, 726, 727, 728, 734, 735, 741, 742, 743, 747, 748, 749, 750, 752, 754,
                       758]
            spyBoard_dict(workset, sleeptime=864000, processes=4, threads=5)  # get all post in 10 days
        else:
            spyBoard(boardid=int(sys.argv[1]))
    else:
        spyNew()


def test(boardid=182, id=4702474, big=""):
    result = getBBS(boardid, id, big)
    pprint(result)
    meta = {}
    handler(meta, *result)


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "test":
                test(sys.argv[2], sys.argv[3])
            elif sys.argv[1] == "allboard":
                CONFIG_INTERESTING_BOARDS = [513, 514, 515, 516, 517, 518, 519, 520, 7, 15, 16, 17, 19, 20, 21, 23, 535,
                                             25, 26, 538, 28, 537, 30, 540, 544, 545, 546, 36, 548, 549, 39, 551, 41,
                                             42, 550, 552, 553, 554, 47, 560, 48, 50, 562, 563, 49, 52, 559, 57, 58,
                                             569, 572, 60, 67, 68, 581, 580, 583, 584, 585, 74, 75, 77, 80, 593, 594,
                                             595, 596, 597, 598, 599, 81, 85, 86, 91, 83, 84, 88, 610, 611, 100, 101,
                                             102, 103, 104, 105, 613, 99, 620, 616, 623, 624, 114, 115, 630, 119, 122,
                                             126, 129, 135, 136, 139, 140, 142, 144, 145, 146, 147, 148, 149, 151, 152,
                                             154, 155, 157, 158, 164, 165, 169, 170, 173, 176, 178, 180, 182, 184, 186,
                                             188, 190, 191, 192, 193, 194, 198, 713, 714, 717, 208, 211, 212, 214, 217,
                                             736, 226, 229, 232, 233, 234, 235, 236, 744, 748, 239, 555, 241, 557, 756,
                                             758, 247, 248, 760, 761, 759, 252, 254, 255, 256, 258, 261, 263, 264, 266,
                                             269, 270, 273, 274, 277, 278, 279, 281, 283, 284, 285, 294, 296, 304, 308,
                                             312, 314, 315, 318, 319, 320, 321, 323, 324, 326, 328, 329, 330, 331, 334,
                                             339, 341, 344, 346, 347, 351, 352, 353, 355, 357, 361, 362, 369, 371, 372,
                                             374, 377, 383, 391, 392, 393, 399, 401, 402, 403, 404, 405, 406, 410, 411,
                                             413, 414, 415, 416, 417, 418, 422, 424, 425, 426, 428, 429, 430, 431, 432,
                                             434, 436, 437, 440, 445, 446, 447, 448, 449, 451, 452, 454, 455, 457, 459,
                                             462, 464, 465, 467, 468, 469, 471, 472, 473, 474, 475, 476, 477, 478, 479,
                                             480, 481, 482, 483, 484, 485, 486, 487, 488, 489, 490, 491, 492, 493, 494,
                                             495, 496, 497, 498, 499, 501, 502, 503, 504, 505, 506, 507, 509, 511]
                spyNew(sleeptime=864000, processes=2, threads=3)
            else:
                main()
        else:
            main()
    except KeyboardInterrupt:
        print("Quit!!!")
        os._exit(1)  # Attention! this does not terminate child process!
    except Exception as e:
        traceback.print_exc()
    finally:
        print("Quit!!!")
        os._exit(0)
