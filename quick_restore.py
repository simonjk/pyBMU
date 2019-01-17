import os
import traceback

from dbhelper import DBHelper
from filehelper import FileHelper
from loghelper import LogHelper

def main():
    # get db cursor
    db_helper = DBHelper()
    file_helper = FileHelper()
    log_helper = LogHelper()
    log = log_helper.getLogger()
    db_data = db_helper.getDictCursor()
    cursor = db_data["cursor"]

    log.info({'action': 'Restore started','BMU_PATH_SEARCH': os.getenv('BMU_PATH_SEARCH'),
              'BMU_PATH_REPLACE': os.getenv('BMU_PATH_REPLACE'),'BMU_PATH_RUNID': os.getenv('BMU_PATH_RUNID'),
              'BMU_PATH_DELIM': os.getenv('BMU_PATH_DELIM'),'BMU_PATH_DEPTH': os.getenv('BMU_PATH_DEPTH'),
              'BMU_PATH_SELECT': os.getenv('BMU_PATH_SELECT')})

    sql = """
        select REPLACE(PATH, '%s', '%s') AS PATH, d.NAME as DRIVE, FILESIZE, i.HASH from BACKUPITEMS b
        inner join ITEMS i
        on b.item_id = i.id
        inner join DRIVES d
        on COALESCE(DRIVE1_ID, DRIVE2_ID) = d.ID
        where b.run_id = %s
        and SUBSTRING_INDEX(path,'%s',%s) = '%s'
        order by COALESCE(DRIVE1_ID, DRIVE2_ID) asc, filesize desc
    """ % (os.getenv('BMU_PATH_SEARCH'), os.getenv('BMU_PATH_REPLACE'), os.getenv('BMU_PATH_RUNID'),
           os.getenv('BMU_PATH_DELIM'), os.getenv('BMU_PATH_DEPTH'), os.getenv('BMU_PATH_SELECT'))
    print(sql)
    cursor.execute(sql)
    files_to_restore = cursor.fetchall()

    count = 0
    errors = ""
    error_list =[]
    for file_to_restore in files_to_restore:
        # print(file_to_restore)
        unescaped_path =  file_to_restore['PATH'].replace('\\\\','\\')
        # dirty hack: adds second backslash if path starts with backslash
        if str.startswith(unescaped_path,'\\'):
            unescaped_path = '\\'+unescaped_path
        file_to_restore['PATH'] = unescaped_path
        tgt = file_to_restore['PATH']
        src = file_helper.path_from_hash(os.getenv('BMU_INT_ROOT'), file_to_restore['DRIVE'], file_to_restore['HASH'])
        if not file_helper.file_exists(tgt):
            while not file_helper.file_exists(src):
                print("Missing: " + src)
                input("Press Enter to continue...")
            if file_helper.file_exists(src):
                try:
                    file_helper.create_parent_if_not_exist(tgt)
                    file_helper.copy_file(src,tgt)
                except Exception as e:
                    print("Exception")  # sql error
                    print(e)
                    tb = e.__traceback__
                    traceback.print_tb(tb)
                    errors += "Could not Copy " + src + " to " + tgt + ": " +str(e)
                    error_list.append({"source": src, "target": tgt, "exception": str(e)})
                count += 1
                print(tgt + " sucessfully restored ["+str(count)+"]")
        else:
            print(tgt + "allready exists, skipping")
        if count%1000 == 0:
            log.info({'action': 'Restore finished', 'BMU_PATH_SELECT': os.getenv('BMU_PATH_SELECT'),
                               'BMU_PATH_RUNID': os.getenv('BMU_PATH_RUNID'), 'count': count,
                               'total': len(files_to_restore)})

    log.info({'action': 'Restore finished', 'BMU_PATH_SEARCH': os.getenv('BMU_PATH_SEARCH'),
              'BMU_PATH_REPLACE': os.getenv('BMU_PATH_REPLACE'), 'BMU_PATH_RUNID': os.getenv('BMU_PATH_RUNID'),
              'BMU_PATH_DELIM': os.getenv('BMU_PATH_DELIM'), 'BMU_PATH_DEPTH': os.getenv('BMU_PATH_DEPTH'),
              'BMU_PATH_SELECT': os.getenv('BMU_PATH_SELECT'), 'count': count, 'errors': error_list })


if __name__ == "__main__":
    main()