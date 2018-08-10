import os
import traceback


from dbhelper import DBHelper
from filehelper import FileHelper
from loghelper import LogHelper

class BackupFiles:
    log_helper = LogHelper()
    log = log_helper.getLogger()
    db_helper = DBHelper()
    db_data = db_helper.getDictCursor()
    cursor = db_data["cursor"]
    file_helper = FileHelper()

    def __init__(self):
        pass


    def backup_files(self, external , run_id=-1):
        cursor = self.cursor
        tracking_field = 'DRIVE1_ID'
        if external:
            tracking_field = 'DRIVE2_ID'
        # get unfinised runs
        select_run_id_only=''
        if run_id <1:
            select_run_id_only = 'and run_id = %s' % run_id
        sql_get_unfinished_runs = 'SELECT * FROM RUNS where sucessful = 1 '+ select_run_id_only
        result = []
        try:
            cursor.execute(sql_get_unfinished_runs)
            result = cursor.fetchall()

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)


        # run backup for each run
        sql_getfilesforrun = 'Select b.id, b.run_id, b.item_ID, b.path, i.hash, b.filesize as size, b.lastmodified, ' \
                             'i.drive1_id, i.drive2_id ' \
                             'from BACKUPITEMS b ' \
                             'inner join ITEMS i ' \
                             'on (b.item_id = i.id) ' \
                             'where i.'+tracking_field+' is null or i.'+tracking_field+' < 1 and b.run_id = %s ' \
                             'order by filesize desc'

        for run in result:
            try:
                cursor.execute(sql_getfilesforrun,(run['ID']))
                files = cursor.fetchall()
                self.backup_run(run_id, files)


            except Exception as e:
                print("Exception")  # sql error
                print(e)
                tb = e.__traceback__
                traceback.print_tb(tb)


    def backup_run(self, run_id, files, external):
        smallest_file = files[-1]['size']
        (drivename, remainingsize) = self.get_drive(run_id, external)

    def get_drive(self, run_id, external):
        pass



def main():
    backup_files = BackupFiles()
    backup_files.backup_filess(False)
    backup_files.backup_filess(True)

if __name__ == "__main__":
        main()