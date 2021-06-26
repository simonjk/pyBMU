import os
import traceback
from filehelper import FileHelper
from dbhelper import DBHelper

def main():
    fh = FileHelper()
    dbh = DBHelper()
    sql_runs = "Select * from RUNS " \
               "where id in (select distinct run_id from BACKUPITEMS) " \
               "and TIME_STARTED < DATE_SUB(NOW(), INTERVAL 60 DAY)"

    runs = []
    target_folder = 'b:/current/archive/bmu/'
    try:
        db = dbh.getDictCursor()
        cursor = db["cursor"]
        cursor.execute(sql_runs)
        result = cursor.fetchall()
        for r in result:
            print(r)
            runs.append(r["ID"])



    except Exception as e:
        print("Exception")  # sql error
        print(e)
        tb = e.__traceback__
        traceback.print_tb(tb)

    for run_id in runs:
        print("Run: %s" % run_id)


        target_file = "%s%s.bmu" % (target_folder, run_id)


        sql_run = "select * from RUNS where id = %s"
        sql_bui = "SELECT B.*, F.path FROM BACKUPITEMS B inner join FILES F ON B.file_id = F.id where run_id = %s"

        sql_DELETE = "DELETE FROM BACKUPITEMS WHERE run_id = %s"

        rundata = {}

        try:
            cursor.execute(sql_run, run_id)
            result = cursor.fetchone()
            rundata["run"] = result

            cursor.execute(sql_bui, run_id)
            result = cursor.fetchall()

            buis = []
            for bui in result:
                buis.append(bui)

            # print(buis)
            rundata["backupitems"] = buis

            # print(rundata)
            fh.save_dict_to_file(rundata, target_file)

            cursor.execute(sql_DELETE, run_id)

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