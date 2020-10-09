import os
import traceback
from filehelper import FileHelper
from dbhelper import DBHelper

def main():
    fh = FileHelper()
    dbh = DBHelper()

    sql_savedbuffer = "select * from ITEMS where buffer_status = 88 order by id"
    sql_updatebufferstatus = "UPDATE ITEMS SET BUFFER_STATUS = 89 WHERE ID = %s"
    usage = fh.bufferusage()
    print(usage)

    try:
        db = dbh.getDictCursor()
        cursor = db["cursor"]
        cursor.execute(sql_savedbuffer)
        result = cursor.fetchall()

        for file in result:
            # if usage <= 0.8:
            #    break
            fh.removefrombuffer(file["HASH"], file["BACKUPGROUP_ID"])
            usage = fh.bufferusage()
            cursor.execute(sql_updatebufferstatus, (file["ID"]))
            print("removed %s from buffer for BG %s " % (file["HASH"], file["BACKUPGROUP_ID"]))
            print(usage)


    except Exception as e:
        print("Exception")  # sql error
        print(e)
        tb = e.__traceback__
        traceback.print_tb(tb)


    # sql_fetch = """
    # select id, hash, backupgroup_id from ITEMS i
    # where i.buffer_status = 1 and i.FILESIZE = 0
    # """
    # sql_update = "UPDATE ITEMS SET FILESIZE = %s WHERE ID = %s"
    # db = dbh.getDictCursor()
    # cursor = db['cursor']
    # try:
    #    cursor.execute(sql_fetch)
    #    result = cursor.fetchall()
    #    for item in result:
    #        bufferpath = fh.buffer_path_from_hash(item['hash'], item['backupgroup_id'])
    #        size = os.stat(bufferpath).st_size
    #        cursor.execute(sql_update, (size, item['id']))
    #        print("Udated %s %s" % (size, item['id']))
    # except Exception as e:
    #    print("Exception")  # sql error
    #    print(e)
    #    tb = e.__traceback__
    #    traceback.print_tb(tb)
    # hash = fh.hash_file('D:\\backup\\test\\dir1234\\test.txt')
    # bufferpath = fh.buffer_path_from_hash(hash, 1)
    # fh.create_parent_if_not_exist(bufferpath)
    # print(hash)
    # print(bufferpath)


if __name__ == "__main__":
    main()