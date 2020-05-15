import os
import traceback
from filehelper import FileHelper
from dbhelper import DBHelper

def main():
    fh = FileHelper()
    dbh = DBHelper()
    sql_fetch = """
    select id, hash, backupgroup_id from ITEMS i
    where i.buffer_status = 1 and i.FILESIZE = 0
    """
    sql_update = "UPDATE ITEMS SET FILESIZE = %s WHERE ID = %s"
    db = dbh.getDictCursor()
    cursor = db['cursor']
    try:
        cursor.execute(sql_fetch)
        result = cursor.fetchall()
        for item in result:
            bufferpath = fh.buffer_path_from_hash(item['hash'], item['backupgroup_id'])
            size = os.stat(bufferpath).st_size
            cursor.execute(sql_update, (size, item['id']))
            print("Udated %s %s" % (size, item['id']))
    except Exception as e:
        print("Exception")  # sql error
        print(e)
        tb = e.__traceback__
        traceback.print_tb(tb)
    # hash = fh.hash_file('D:\\backup\\test\\dir1234\\test.txt')
    # bufferpath = fh.buffer_path_from_hash(hash, 1)
    # fh.create_parent_if_not_exist(bufferpath)
    # print(hash)
    # print(bufferpath)


if __name__ == "__main__":
    main()