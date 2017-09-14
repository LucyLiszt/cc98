from config import db
from pprint import pprint
import traceback

def get_boardid(conn, prefix="bbs"):
    """
    返回当前数据库中以prefix前缀开头的表名称list
    """
    cur = conn.cursor()
    cur.execute("show tables")
    sqlresult = cur.fetchall()
    return [i[0] for i in sqlresult if i[0].startswith(prefix)]

def alter_to_utf8mb4():
    conn = db()
    names = get_boardid(conn)+get_boardid(conn,prefix="big") #以bbs开头的表和以bigbbs开头的表都要处理
    for tablename in names:
        print(tablename)
        # 修改列的字符集、表的字符集
        sql = """
ALTER TABLE `{tablename}`
MODIFY COLUMN `user`  varchar(66) CHARACTER SET utf8mb4 NOT NULL AFTER `edittime`,
MODIFY COLUMN `content`  longtext CHARACTER SET utf8mb4 NOT NULL AFTER `user`,
DEFAULT CHARACTER SET=utf8mb4;""".format(tablename=tablename)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            traceback.print_exc() #发生了异常，就显示报错信息，停下来等待处理
            input("Wait...") # 如果不严重可以继续处理下一个表 按回车；终止整个程序 按Ctrl+C
    
if __name__ == "__main__":
    alter_to_utf8mb4()