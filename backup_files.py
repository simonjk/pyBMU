import os
import sys
import traceback


from dbhelper import DBHelper
from filehelper import FileHelper
from loghelper import LogHelper


# stati:
# -1 hash changed
# -2 invalid buffer hash
# -9 Could not copy File
# -12 new hash in external run set for internal to prevent loop
# -99 manual set for not saved without bui link


class BackupFiles:
    drivepathinternal = os.getenv('BMU_INT_ROOT')
    drivepathexternal = os.getenv('BMU_EXT_ROOT')
    log_helper = LogHelper()
    log = log_helper.getLogger()
    db_helper = DBHelper()
    db_data = db_helper.getDictCursor()
    cursor = db_data["cursor"]
    file_helper = FileHelper()

    def __init__(self):
        pass


    def backup_files(self,  backupgroup_id, external):
        logger = self.log
        filehelper = self.file_helper
        if external:
            drivepath = self.drivepathexternal
        else:
            drivepath = self.drivepathinternal



        drive_info = self.get_drive(backupgroup_id, external)

        logger.info({'action': 'Starting Backuping Files',  'backup_group': backupgroup_id,
                           'external': external, 'Drive Info': drive_info})

        free_disk_space, free_quota = self.get_free_space(drive_info, drivepath)
        logger.info({'action': 'Free Space', 'backup_group': backupgroup_id,
                     'external': external, 'Drive Info': drive_info, 'free_quota': free_quota,
                     'free_space': free_disk_space})

        if free_disk_space <= 0 or free_quota <= 0:
            logger.warn({'action': 'Disk Full, Aborting', 'backup_group': backupgroup_id,
                         'external': external, 'Drive Info': drive_info, 'free_quota': free_quota,
                         'free_space': free_disk_space})
            return drive_info["id"]
        files_to_save = self.get_filestosave(backupgroup_id, external)
        total_files = len(files_to_save)
        files_saved = 0
        logger.info({'action': 'Files To backup', 'backup_group': backupgroup_id,
                     'external': external, 'files_to_backup': total_files})
        skip_big = 0
        for file_to_save in files_to_save:
            # # temporaray code for testing
            #
            # if file_to_save["filesize"] > 5000000000:
            #    logger.info("Skipping File to big because of temporary file Size limit 5GB : %s" % file_to_save)
            #    continue
            # # End of Temporary Code
            if free_disk_space < file_to_save["filesize"] or free_quota < file_to_save["filesize"]:
                logger.info({'action': 'Skipping File to big for remaining Space', 'backup_group': backupgroup_id,
                             'external': external, 'file_to_backup': file_to_save})
                skip_big += 1
                continue
            target = filehelper.path_from_hash(drivepath, drive_info["name"], file_to_save["hash"])
            source = filehelper.buffer_path_from_hash(file_to_save["hash"], backupgroup_id)

            logger.info({'action': 'Copying File', 'backup_group': backupgroup_id,
                         'external': external, 'file_to_backup': file_to_save})
            if not filehelper.copy_file(source, target):
                logger.error({'action': 'Copying File', 'backup_group': backupgroup_id,
                             'external': external, 'file_to_backup': file_to_save, 'source': source, 'target': target})
                self.mark_item(backupgroup_id, file_to_save["hash"], external, -9)
                continue
            hash_tgt = filehelper.hash_file(target)
            if hash_tgt != file_to_save["hash"]:
                logger.error({'action': 'Hash not Matching', 'backup_group': backupgroup_id,
                              'external': external, 'file_to_backup': file_to_save, 'hash_target': hash_tgt,
                              'target': target})
                hash_src_new = filehelper.hash_file(source)
                if file_to_save["hash"] == hash_src_new:
                    filehelper.delete_file(target)
                    self.mark_item(backupgroup_id, file_to_save["hash"], external, -1)
                    logger.error("File changed during copying from buffer %s : %s != %s" % (target, hash_tgt, hash_src_new))
                    logger.error({'action': 'File changed during copying from buffer', 'backup_group': backupgroup_id,
                                  'external': external, 'file_to_backup': file_to_save, 'hash_target': hash_tgt,
                                  'target': target, 'hash_src_new': hash_src_new})
                    continue
                else:
                    filehelper.delete_file(target)
                    self.mark_item(backupgroup_id, file_to_save["hash"], external, -2)
                    logger.error({'action': 'Buffered File does not produce correct hash',
                                  'backup_group': backupgroup_id, 'external': external,
                                  'file_to_backup': file_to_save, 'hash_target': hash_tgt,
                                  'target': target, 'hash_src_new': hash_src_new})
                    continue
            else:
                self.mark_item(backupgroup_id, file_to_save["hash"], external, drive_info["id"])
                logger.info({'action': 'Backup File Successful', 'backup_group': backupgroup_id,
                             'external': external, 'file_to_backup': file_to_save, 'hash_target': hash_tgt,
                             'target': target})
                files_saved += 1

            free_quota = free_quota - file_to_save["filesize"]
            free_disk_space = filehelper.freespace(drivepath)
            logger.info({'action': 'Remaining Free Space', 'backup_group': backupgroup_id,
                         'external': external, 'Drive Info': drive_info, 'free_quota': free_quota,
                         'free_space': free_disk_space})
        logger.info({'action': 'Finished Backup', 'backup_group': backupgroup_id,
                     'external': external, 'Drive Info': drive_info, 'free_quota': free_quota,
                     'free_space': free_disk_space, 'Files_To_Save': total_files, 'Files_Saved': files_saved})
        if skip_big > 0:
            return drive_info["id"]
        else:
            return 0





    def get_filestosave(self, backupgroup_id: int, external: bool):
        cursor = self.cursor
        tracking_field = 'DRIVE1_ID'
        if external:
            tracking_field = 'DRIVE2_ID'
        sql_getfilesforrun = """
        Select i.id as item_id, i.hash as hash,
            i.filesize as filesize,
            i.drive1_id as drive1_id, i.drive2_id as drive2_id, i.buffer_status
            from ITEMS i
            where (i.%s is null or i.%s = 0)
            and i.buffer_status = 1
            and i.backupgroup_id = %s
            order by filesize desc
        """ % (tracking_field, tracking_field, backupgroup_id)

        # print(sql_getfilesforrun)


        try:
            cursor.execute(sql_getfilesforrun)
            files = cursor.fetchall()
            return files


        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)


    def get_drive(self, backupgroup_id, external):
        cursor = self.cursor
        sql_getdrive = """SELECT id, name, drivefull, extern, maxsize, drive_id, group_id FROM DRIVES d
            inner join DRIVES_GROUPS dg
            on d.id = dg.drive_id
            where group_id = %s and drivefull = false and extern = %s limit 1
        """ % (backupgroup_id, external)

        try:
            cursor.execute(sql_getdrive)
            result = cursor.fetchone()

            return result

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)
            return {}


    def get_free_space(self, drive_info: dict, drivepath: str):
        filehelper = self.file_helper
        cursor = self.cursor
        disk = filehelper.freespace(drivepath)
        sql_getusedspace = """
        select sum(size) size from (
        select max(filesize) as size, i.hash  from ITEMS i
        where
        i.backupgroup_id = %s and (i.DRIVE1_ID = %s or i.DRIVE2_ID = %s)
        group by i.hash) x
        """ % (drive_info["group_id"], drive_info["id"], drive_info["id"])
        # print(sql_getusedspace)

        try:
            cursor.execute(sql_getusedspace)
            result = cursor.fetchone()
            # print(result)
            if result["size"] is None:
                logical = int(drive_info["maxsize"])
            else:
                logical = int(drive_info["maxsize"]) - int(result["size"])
            return disk, logical

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)
            return disk, 0


    def mark_item(self, bg_id, hash, external, status):
        tracking_field = 'DRIVE1_ID'
        if external:
            tracking_field = 'DRIVE2_ID'
        cursor = self.cursor
        sql_updateitem = 'update ITEMS i set %s = %s where backupgroup_id= %s and hash = "%s" ' % \
                             (tracking_field, status, bg_id, hash)


        try:
            cursor.execute(sql_updateitem)

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def is_hash_known(self, hash, backup_group):
        cursor = self.cursor
        sql_updateitem = 'select id from ITEMS where backupgroup_id = %s and hash = \'%s\'' % \
                         (backup_group, hash)

        try:
            cursor.execute(sql_updateitem)
            data = cursor.fetchall()
            if len(data) == 0:
                return 0
            else:
                return data[0]["id"]

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)
            return 0

    def change_item_in_bui(self, bui_id, item_id, hash):
        cursor = self.cursor
        sql_updatebuitem = 'update BACKUPITEMS  set item_id  = %s, hash = \'%s\' where id = %s ' % \
                           (item_id, hash, bui_id)
        print(sql_updatebuitem)

        try:
            cursor.execute(sql_updatebuitem)

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def create_item(self, bg_id, hash, external, status, size):
        tracking_field = 'DRIVE1_ID'
        sql_insertitem = 'insert into ITEMS (backupgroup_id, hash, %s, filesize) values (%s, \'%s\', %s, %s)' % \
                         (tracking_field, bg_id, hash, status, size)
        if external:
            tracking_field = 'DRIVE2_ID'
            sql_insertitem = 'insert into ITEMS (backupgroup_id, hash, DRIVE1_ID, DRIVE2_ID, filesize) values (%s, \'%s\',  -12, %s, %s)' % \
                             (bg_id, hash, status, size)
        cursor = self.cursor



        try:
            cursor.execute(sql_insertitem)

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def close_finished_runs(self):
        sql_get_finished = """
        Select id, coalesce(x.count, 0) as count from RUNS r
        LEFT OUTER JOIN (
            Select run_id, count(*) as count
            from BACKUPITEMS b
            inner join ITEMS i
            on (b.item_id = i.id)
            where i.DRIVE1_ID < 0 or i.DRIVE2_ID < 0
            group by run_id
        ) x
        on r.id = x.run_id
        where
        (ALL_SAVED IS NULL or ALL_SAVED = 0)
        and
        id not in (
            Select distinct b.run_id as run_id
            from BACKUPITEMS b
            inner join ITEMS i
            on (b.item_id = i.id)
            where ((i.DRIVE1_ID is null or i.DRIVE1_ID = 0) or (i.DRIVE2_ID is null or i.DRIVE2_ID = 0)) )
        """
        sql_update_run =  "UPDATE RUNS SET ALL_SAVED = 1, ERRORS_SAVING = %s where ID = %s"

        cursor = self.cursor

        try:
            cursor.execute(sql_get_finished)
            runs = cursor.fetchall()
            logger = self.log
            for run in runs:
                cursor.execute(sql_update_run, (run["count"], run["id"]))
                logger.info("Saved Run %s with %s Errors" %( run["id"], run["count"]))
                logger.info({'action': 'Saved Runs',
                             'run_id':  run["id"], 'Errors': run["count"]})

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def cleanupBuffer(self):
        fh = FileHelper()
        dbh = DBHelper()
        logger = self.log

        sql_savedbuffer = "select * from ITEMS where (DRIVE1_ID > 0  and DRIVE2_ID > 0) and buffer_status = 1 order by id "
        sql_updatebufferstatus = "UPDATE ITEMS SET BUFFER_STATUS = 2 WHERE ID = %s"
        usage = fh.bufferusage()
        print(usage)

        try:
            db = dbh.getDictCursor()
            cursor = db["cursor"]
            cursor.execute(sql_savedbuffer)
            result = cursor.fetchall()

            for file in result:
                if usage <= 0.8:
                    break
                fh.removefrombuffer(file["HASH"], file["BACKUPGROUP_ID"])
                usage = fh.bufferusage()
                cursor.execute(sql_updatebufferstatus, (file["ID"]))
                print("removed %s from buffer for BG %s " % (file["HASH"], file["BACKUPGROUP_ID"]))
                print(usage)
                logger.info({'action': 'Removed from Buffer',
                             'hash': file["HASH"], 'bachup_group': file["BACKUPGROUP_ID"], "size": file["FILESIZE"]})


        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def set_drive_full(self, id):

        cursor = self.cursor
        sql_updateitem = 'update DRIVES set drivefull = 1 where id=%s ' % id

        try:
            cursor.execute(sql_updateitem)

        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

def main():
    backup_files = BackupFiles()
    backup_group = int(sys.argv[1])
    external = False
    if sys.argv[2] == 'E':
        external = True
    finished = -1
    while finished != 0:
        finished = backup_files.backup_files(backup_group, external)
        if finished > 0:
            backup_files.set_drive_full(finished)
    backup_files.close_finished_runs()
    backup_files.cleanupBuffer()


if __name__ == "__main__":
        main()