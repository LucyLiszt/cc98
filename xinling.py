# coding:utf-8
# stable version on 2017-09-15

from EasyLogin import EasyLogin  # 安装依赖：pip install bs4 requests pymysql
from time import sleep
from mpms import MPMS  # 虽然本项目里面已经包含mpms的代码，你也可以pip install mpms，项目地址：https://github.com/aploium/mpms
from pprint import pprint, pformat
import socket, requests, sys, pymysql, re, os, time, random
from config import COOKIE, db, enable_multiple_ip, CONFIG_INTERESTING_BOARDS, CONFIG_IGNORE_POSTS

"""
欢迎阅读chenyuan写的cc98爬虫代码, 我假设你已经安装了以下软件/阅读了以下文档：
1) python3 https://python.org/
2) mysql https://www.mysql.com/
3) requests: 让 HTTP 服务人类 http://cn.python-requests.org/zh_CN/latest/
4) Beautiful Soup 文档 https://www.crummy.com/software/BeautifulSoup/bs4/doc.zh/
5) PyMySQL documentation: https://pymysql.readthedocs.io/en/latest/
6) EasyLogin: 这个是我为了简化对requests和BeautifulSoup的调用而写的库 https://github.com/zjuchenyuan/EasyLogin
7) mpms: 多进程多线程并发 https://github.com/aploium/mpms

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


def getBoardPage(boardid, page):
    """
    输入板块的boardid和第page页，返回该页的帖子列表 list类型 (帖子所在的板块id, 帖子id)
    提取对应的HTML是<a href='dispbbs.asp?boardID=182&ID=4725833&star=1&page=1'>
    可以看出getNewPost, getHotPost和这里的getBoardPage返回类型是统一的，可以方便地加在一起
    :param boardid: 板块id 如182
    :param page: 第多少页，如1
    :return: 如[["182", "4725833"], ["182", "4726027"], ...]
    """
    a = EasyLogin(cookie=COOKIE)
    a.get("{}/list.asp?boardid={}&page={}".format(DOMAIN, boardid, page))
    result = set()
    for i in a.getList("dispbbs.asp?boardID="):
        result.add(getPart(i, "&ID=", "&"))
    return [(boardid, i) for i in result]


def getBBS(boardid, id, big, morehint=False):
    """
    核心的获取帖子内容函数
    result为[楼层lc, 用户名user, 发帖内容content, 发帖时间posttime, 最后编辑时间lastedittime]
    TODO: 发帖内容获取的不完整，丢失了回帖的标题信息

    :param boardid: 板块id
    :param id: 帖子id
    :param big: 一般为""，如果是全站爬取插入到历史大表中 则为"big"
    :param morehint: 是否显示更多进度显示
    :return: [boardid, id, result, big] 作为handler的传入参数
    """
    a = EasyLogin(cookie=COOKIE)
    result = []
    star = 1
    a.get("{}/dispbbs.asp?BoardID={}&id={}&star=1".format(DOMAIN, boardid, id))
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
            a.get("{}/dispbbs.asp?BoardID={}&id={}&star={}".format(DOMAIN, boardid, id, star))
        else:
            title = a.b.title.text.strip(" » CC98论坛")  # 帖子标题使用页面标题，假设页面标题的格式为"title &raquo; CC98论坛"
            result.append([0, "", title, "1970-01-01 08:00:01", "1970-01-01 08:00:01"])
            # print(title)
        for i in range(1, 11 if star != pages else lastpage + 1):  # 最后一页没有第lastpage+1个楼层
            # print(star,i)
            lc = (star - 1) * 10 + i
            floorstart = a.b.find("a", attrs={"name": "{}".format(i if i != 10 else 0)})
            if floorstart is None:
                result.append([lc, "98Deleter", ">>>No Content<<<", "1970-01-01 08:00:01", "1970-01-01 08:00:01"])
                continue
            table = floorstart.next_sibling.next_sibling  # 假设楼层内容开始的table前都有<a name="1"></a>

            # print(table)
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
            contentdiv = table.find('article').find('div')
            content = contentdiv.text if contentdiv is not None else ">>>No Content<<<"
            # print(content)
            result.append([lc, user, content, posttime, lastedittime])
            # break
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

myprint = lambda s:print("[{showtime}] {s}".format(showtime=time.strftime("%Y-%m-%d %H:%M:%S"),s=s))

def plus1(filename):
    """
    文件内容为一个int，打开文件+1，写入
    用于统计发生次数
    """
    try:
        with open(filename,"r") as fp:
            data = fp.read()
        result = int(data)
    except:
        result = 0
    result += 1
    with open(filename,"w") as fp:
        fp.write(str(result))
    

def spyNew(sleeptime=100, processes=5, threads=4):
    """
    对热门、新帖以及额外配置的板块列表进行监测，这是直接运行代码将调用的函数
    """
    starttime = time.time()
    m = MPMS(getBBS, handler, processes=processes, threads_per_process=threads)
    t = 0
    workload = set(CONFIG_IGNORE_POSTS)  # CONFIG_IGNORE_POSTS预先加入到workload，被视为已经爬取过就不会爬取
    thenew = getHotPost() + getNewPost()
    for boardid in CONFIG_INTERESTING_BOARDS:
        thenew += getBoardPage(int(boardid), 1)
    random.shuffle(thenew)  # 随机打乱一下
    for boardid, i in thenew:
        boardid, i = int(boardid), int(i)
        if (boardid, i) not in workload: # 去重 不要爬取多次
            workload.add((boardid, i))
            m.put([boardid, i, ""])
    while time.time() - starttime < sleeptime and len(m) > 0:
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

        ##print(getBoardSize(182))
        ##GetBBS
        # for j in range(2,100):
        #    for i in getBoardPage(100,j):
        #        getBBS(100,4661275)

        ##GetBoardID
        """
        def getBoardID(bigboardid=0):
            a = EasyLogin(cookie=COOKIE)
            a.get("{}/list.asp?boardid={}".format(DOMAIN, bigboardid) if bigboardid != 0 else DOMAIN)
            l = a.getList("list.asp?boardid")
            result = set()
            for i in l:
                if '&' in i:
                    continue
                result.add(getPart(i, "boardid=", "&"))
            return [int(i) for i in result]
        """
        # print(sorted(getBoardID()))
        # result = []
        # for i in getBoardID():
        #    result.extend(getBoardID(i))
        # print(result)
        # return result
        ##CreateTable
        # boardlist = [284,7, 15, 16, 17, 20, 21, 23, 25, 26, 28, 30, 36, 38, 39, 40, 41, 42, 47, 48, 49, 50, 52, 58, 60, 67, 68, 75, 77, 80, 81, 83, 84, 85, 86, 91, 99, 100, 101, 102, 103, 104, 105, 114, 115, 119, 122, 129, 135, 136, 139, 140, 142, 144, 145, 146, 147, 148, 149, 151, 152, 154, 155, 157, 158, 164, 165, 169, 170, 173, 176, 179, 180, 182, 183, 184, 186, 187, 188, 190, 191, 192, 193, 194, 195, 198, 203, 204, 205, 206, 207, 208, 211, 212, 213, 214, 216, 217, 221, 222, 224, 226, 229, 232, 233, 234, 235, 236, 239, 241, 245, 246, 247, 249, 254, 256, 258, 261, 262, 263, 264, 266, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 281, 282, 283, 285, 286, 287, 288, 290, 292, 294, 295, 296, 298, 303, 304, 306, 307, 308, 310, 311, 312, 314, 315, 316, 318, 319, 320, 321, 323, 324, 325, 326, 328, 329, 330, 334, 339, 341, 344, 346, 347, 351, 352, 353, 355, 357, 361, 362, 363, 367, 368, 369, 371, 372, 375, 377, 383, 391, 392, 393, 399, 401, 402, 403, 404, 405, 406, 408, 412, 417, 422, 423, 427, 430, 431, 432, 433, 434, 436, 437, 440, 442, 446, 447, 448, 449, 450, 451, 452, 454, 455, 457, 459, 460, 461, 462, 465, 467, 468, 469, 471, 472, 473, 475, 480, 481, 482, 483, 484, 485, 486, 489, 491, 492, 493, 494, 498, 499, 501, 502, 503, 504, 505, 506, 507, 511, 513, 514, 515, 518, 519, 520, 535, 537, 538, 540, 545, 546, 548, 549, 550, 551, 559, 560, 562, 563, 564, 568, 569, 572, 574, 575, 576, 578, 579, 580, 581, 582, 587, 588, 589, 590, 591, 592, 593, 594, 595, 596, 597, 598, 599, 600, 601, 602, 603, 605, 606, 607, 610, 611, 613, 614, 615, 616, 618, 620, 621, 622, 623, 624, 625, 626, 628, 629, 630, 631, 632, 633, 634, 635, 640, 642, 710, 711, 713, 714, 723, 724, 726, 727, 728, 733, 736, 741, 742, 743, 744, 745, 747, 748, 749, 750, 751, 752, 753, 754]
        # for i in boardlist:
        #    createTable(i)
        ##GetNewPost
        # return getNewPost()
        ##SpyBoard
        # spyBoard(boardid=182,spytimes=1)
        ##GetHotPost
        # print(getHotPost())


def test(boardid=182, id=4702474, big=""):
    result = getBBS(boardid, id, big)
    pprint(result)
    meta = {}
    handler(meta, *result)


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "test":
            test()
        else:
            main()
    except KeyboardInterrupt:
        print("Quit!!!")
        os._exit(1)  # Attention! this does not terminate child process!
    finally:
        print("Quit!!!")
        os._exit(0)
