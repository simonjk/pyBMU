import os
import sys
import traceback
import re
import time
import warnings

from dbhelper import DBHelper
from filehelper import FileHelper
from loghelper import LogHelper

class ScanFiles:
    backup_group = 1
    run_id = -1
    log_helper = LogHelper()
    log = log_helper.getLogger()
    db_helper = DBHelper()
    db_data = db_helper.getDictCursor()
    cursor = db_data["cursor"]
    file_helper = FileHelper()
    file_filter = []
    dir_filter = []

    def __init__(self, backup_group_id):
        self.backup_group = backup_group_id
        self.create_run()
        self.load_filters()

    def load_filters(self):
        cursor = self.cursor
        sql_loadfilefilter = 'Select expression from FILTERS ' \
                             'where (BACKUPGROUP_ID = %s OR BACKUPGROUP_ID is null) ' \
                             'and file = 1'
        sql_loaddirfilter = 'Select expression from FILTERS ' \
                            'where (BACKUPGROUP_ID = %s OR BACKUPGROUP_ID is null) ' \
                            'and dir = 1'
        try:
            cursor.execute(sql_loaddirfilter,(self.backup_group))
            result = cursor.fetchall()
            self.dir_filter = self.compile_filters(result)

            cursor.execute(sql_loadfilefilter, (self.backup_group))
            result = cursor.fetchall()
            self.file_filter = self.compile_filters(result)


        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def compile_filters(self,result_set):
        result = []
        for data in result_set:
            raw_filter = '^(?=.*'+data["expression"].replace('*', '(.*)') + ').*'
            print (raw_filter)
            filter = re.compile(raw_filter)
            result.append(filter)
        return result

    def check_filter(self, filters, path):
        for filter in filters:
            match = filter.match(path)
            if match:
                return True
        return False


    def create_run(self):
        cursor = self.cursor

        sql = "INSERT INTO RUNS (BACKUPGROUP_ID, TIME_STARTED) VALUES (%s, CURRENT_TIMESTAMP)"
        try:
            cursor.execute(sql,(self.backup_group))
            self.run_id = cursor.lastrowid

            self.log.info({'action': 'Create Run_ID', 'run_id': self.run_id, 'backup_group': self.backup_group})
        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)



    def scan_for_files(self):
        cursor = self.cursor

        sql_insert_file = 'INSERT IGNORE INTO FILES (backupgroup_id, path, path_hash) ' \
                          'VALUES (%s, %s, md5(concat(%s, "-", %s)))'
        sql_insert_bu = """
        INSERT INTO BACKUPITEMS (RUN_ID, FILE_ID, FILESIZE, LASTMODIFIED, BACKUPGROUP_ID)
        Select %s, id, %s, %s, %s
        from FILES where path_hash = md5(concat(%s, '-', %s))
        """

        dirs = self.get_basedirs(cursor)

        # ---------------- Scan Dirs
        totalfiles = 0
        for dir in dirs:
            filesperdir = 0
            filterdfiles = 0
            started = int(round(time.time() * 1000))
            self.log.info({'action': 'Start scanning Dir', 'run_id': self.run_id, 'backup_group': self.backup_group,
                           'dir': dir['PATH']} )
            for root, dirs, files in os.walk(dir['PATH']):
                for file in files:
                    filesperdir += 1
                    file_hash = ""

                    if filesperdir%1000 == 0:
                        cursor = self.new_connection()

                    try:
                        filedata = {}
                        filedata['filepath'] = os.path.join(root, file)
                        filedata['mtime'] = int(round( os.path.getmtime(filedata['filepath']) * 1000))
                        filedata['size'] = os.stat(filedata['filepath']).st_size

                        # file filter
                        filename = self.file_helper.get_filename(filedata['filepath'])
                        if self.check_filter(self.file_filter,filename):
                            print("Filtered (file) out "+ filedata['filepath'] + ' (' + filename + ')' )
                            filterdfiles += 1
                            continue

                        # dir filter
                        parent = self.file_helper.get_parent(filedata['filepath'])
                        if self.check_filter(self.dir_filter, parent):
                            print("Filtered (dir) out " + filedata['filepath'] + ' (' + parent + ')')
                            filterdfiles += 1
                            continue

                        totalfiles += 1
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            cursor.execute(sql_insert_file, (self.backup_group, filedata['filepath'],
                                                         self.backup_group, filedata['filepath']))
                        cursor.execute(sql_insert_bu, (self.run_id, filedata['size'],
                                                       filedata['mtime'], self.backup_group,
                                                       self.backup_group, filedata['filepath']))

                        new_id = cursor.lastrowid

                        affected_rows, file_hash = self.map_unchganged(cursor, filedata, new_id)



                        if affected_rows > 0:
                            self.log.debug({'action': 'Unchanged File', 'path': filedata['filepath'],
                                            'run_id': self.run_id, 'backup_group': self.backup_group,
                                           'count': affected_rows})
                        else:
                            file_hash = self.hash_match_or_create_item(cursor, filedata, new_id)

                        if file_hash is not None:

                            buffer_status = self.check_buffer_status(cursor, file_hash)

                            
                            if buffer_status <= 0:

                                self.buffer_file(cursor, filedata, file_hash, new_id)
                            else:
                                self.log.debug({'action': 'File already Buffered', 'path': filedata['filepath'],
                                           'run_id': self.run_id, 'backup_group': self.backup_group,
                                            'hash': file_hash, 'backup item': new_id})


                    except Exception as e:
                        cursor = self.new_connection()
                        print("Exception")  # sql error
                        print(e)
                        tb = e.__traceback__
                        traceback.print_tb(tb)

                    if totalfiles % 10000 == 0:
                       print("%s Files Scanned. Last Scanned: %s" % (totalfiles, filedata))

                    # print(filedata)
            finished = int(round(time.time() * 1000))
            duration = finished - started
            divider = 1
            if filesperdir > 0:
                divider = filesperdir
            per_file = duration / divider
            self.log.info({'action': 'End scanning Dir', 'run_id': self.run_id, 'backup_group': self.backup_group,
                           'dir': dir['PATH'], 'count': filesperdir, 'duration': duration, 'per_file': per_file, 'filtered': filterdfiles})
            cursor = self.new_connection()

        self.log.info({'action': 'End scanning Dirs', 'run_id': self.run_id, 'backup_group': self.backup_group,
                        'count': totalfiles})


        # ------------------ SET Hashing Complete
        cursor = self.new_connection()
        sql_sethashingsuccess = 'UPDATE RUNS SET SUCESSFUL = 1 WHERE ID = %s'

        try:
            cursor.execute(sql_sethashingsuccess,(self.run_id))
            self.log.info(
                {'action': 'Scanning and Hashing successful', 'run_id': self.run_id, 'backup_group': self.backup_group}
            )



        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)

    def buffer_file(self, cursor, filedata, new_hash, new_id):
        sql_update_buffer_status = "Update ITEMS Set BUFFER_STATUS=%s where hash = %s and backupgroup_id = %s"
        sql_check_hash_exists = "select count(*) as count, max(id) as item_id from ITEMS where hash = %s and backupgroup_id = %s"
        sql_updatebuitem = 'update BACKUPITEMS  set item_id  = %s, hash = %s where id = %s '
        # Build Target Path
        bufferpath = self.file_helper.buffer_path_from_hash(new_hash, self.backup_group)
        self.file_helper.create_parent_if_not_exist(bufferpath)
        # Copy File
        self.file_helper.copy_file(filedata['filepath'], bufferpath)
        # Validate Hash
        tgt_hash = self.file_helper.hash_file(bufferpath)
        if tgt_hash == new_hash:
            # Set Bufferstatus to 1
            cursor.execute(sql_update_buffer_status, (1, new_hash, self.backup_group))
            self.log.info({'action': 'File Buffered Successfully', 'path': filedata['filepath'],
                           'run_id': self.run_id, 'backup_group': self.backup_group,
                           'hash': new_hash, 'backup item': new_id})

        else:
            # hash original again
            src_hash = self.file_helper.hash_file(filedata['filepath'])

            if src_hash != tgt_hash:
                # delete target and  set buffer code to -1
                self.file_helper.delete_file(bufferpath)
                cursor.execute(sql_update_buffer_status, (-1, new_hash, self.backup_group))
                self.log.info(
                    {'action': 'Could not Buffer: Fast Changing', 'path': filedata['filepath'],
                     'run_id': self.run_id, 'backup_group': self.backup_group,
                     'hash': new_hash, 'backup item': new_id})
            else:
                # Check if entry for new Hash exists
                cursor.execute(sql_check_hash_exists, (tgt_hash, self.backup_group))
                rs2 = cursor.fetchone()
                if rs2["count"] == 0:
                    # set orig Item Entry to -2
                    cursor.execute(sql_update_buffer_status, (-2, new_hash, self.backup_group))
                    # create items entry
                    sql_insertitems = "Insert into ITEMS(backupgroup_id, hash, filesize) VALUES (%s, %s, %s)"
                    cursor.execute(sql_insertitems, (self.backup_group, tgt_hash, os.stat(bufferpath).st_size))
                    # move file
                    tgtpath2 = self.file_helper.buffer_path_from_hash(tgt_hash, self.backup_group)
                    self.file_helper.create_parent_if_not_exist(tgtpath2)
                    self.file_helper.move_file(bufferpath, tgtpath2)
                    moved_hash = self.file_helper.hash_file(tgtpath2)
                    if tgt_hash == moved_hash:
                        # update BUI with new item and set buffer_status = 1
                        cursor.execute(sql_updatebuitem, (rs2["item_id"], tgt_hash, new_id))
                        cursor.execute(sql_update_buffer_status, (1, tgt_hash, self.backup_group))
                        self.log.info({'action': 'File Buffered Successfully but in Changed Version',
                                       'path': filedata['filepath'],
                                       'run_id': self.run_id,
                                       'backup_group': self.backup_group,
                                       'hash': tgt_hash, 'old hash': new_hash,
                                       'backup item': new_id})
                    else:
                        # Delete file and update  item bufferstatus -4
                        self.file_helper.delete_file(tgtpath2)
                        cursor.execute(sql_update_buffer_status, (-4, new_hash, self.backup_group))
                        self.log.info(
                            {'action': 'Could not Buffer: Changed and Fast Changing',
                             'path': filedata['filepath'],
                             'run_id': self.run_id, 'backup_group': self.backup_group,
                             'hash': new_hash,
                             'backup item': new_id})
                else:
                    buffer_status = self.check_buffer_status(tgt_hash)
                    if buffer_status > 0:
                        # delete target and change bui entry
                        self.file_helper.delete_file(bufferpath)
                        cursor.execute(sql_updatebuitem, (rs2["item_id"], tgt_hash, new_id))
                        cursor.execute(sql_update_buffer_status,
                                       (1, tgt_hash, self.backup_group))
                        self.log.info(
                            {'action': 'File Buffered Successfully Changed Version already in Buffer',
                             'path': filedata['filepath'],
                             'run_id': self.run_id,
                             'backup_group': self.backup_group,
                             'hash': tgt_hash, 'old hash': new_hash,
                             'backup item': new_id})
                    else:
                        # move target
                        tgtpath2 = self.file_helper.buffer_path_from_hash(tgt_hash,
                                                                          self.backup_group)
                        self.file_helper.create_parent_if_not_exist(tgtpath2)
                        self.file_helper.move_file(bufferpath, tgtpath2)
                        moved_hash = self.file_helper.hash_file(tgtpath2)
                        # validate new target
                        if tgt_hash == moved_hash:
                            cursor.execute(sql_updatebuitem, (rs2["item_id"], tgt_hash, new_id))
                            self.log.info(
                                {
                                    'action': 'File Buffered Successfully Changed Version in existing Item',
                                    'path': filedata['filepath'],
                                    'run_id': self.run_id,
                                    'backup_group': self.backup_group,
                                    'hash': tgt_hash,
                                    'old hash': new_hash,
                                    'backup item': new_id})
                        else:
                            # Delete target and set buffer status -3
                            self.file_helper.delete_file(tgtpath2)
                            cursor.execute(sql_update_buffer_status,
                                           (-3, new_hash, self.backup_group))
                            self.log.info(
                                {'action': 'Could not Buffer: Fast Changing in existing item',
                                 'path': filedata['filepath'],
                                 'run_id': self.run_id, 'backup_group': self.backup_group,
                                 'hash': new_hash,
                                 'backup item': new_id})

    def check_buffer_status(self, cursor, new_hash):
        sql_check_buffer_status = "SELECT BUFFER_STATUS FROM ITEMS I where hash = %s and backupgroup_id = %s"
        # print('[%s | %s]' % (new_hash, self.backup_group))
        cursor.execute(sql_check_buffer_status, (new_hash, self.backup_group))
        rs = cursor.fetchone()
        buffer_status = rs["BUFFER_STATUS"]
        return buffer_status

    def hash_match_or_create_item(self, cursor, filedata, new_id):
        sql_insertitems = "Insert into ITEMS(backupgroup_id, hash, filesize) VALUES (%s, %s, %s)"
        # set hash and create item where necesarry                            #
        sql_sethash = 'UPDATE BACKUPITEMS SET HASH = %s WHERE id = %s'
        new_hash = self.file_helper.hash_file(filedata['filepath'])
        if new_hash is None:
            self.log.warn({'action': 'Could not hash', 'path': filedata['filepath'],
                           'run_id': self.run_id, 'backup_group': self.backup_group,
                           })
            return new_hash
        cursor.execute(sql_sethash, (new_hash, new_id))
        sql_matchwithitems = """
                                     UPDATE BACKUPITEMS t
                                     inner join BACKUPITEMS b
                                     on t.id = b.id
                                     inner join ITEMS i
                                     on i.hash = b.hash
                                     SET b.ITEM_ID = i.id
                                     where b.id = %s and i.backupgroup_id = %s
                                 """
        matched = cursor.execute(sql_matchwithitems, (new_id, self.backup_group))
        if matched == 0:

            inserted = cursor.execute(sql_insertitems, (self.backup_group, new_hash, filedata['size']))
            matched = cursor.execute(sql_matchwithitems, (new_id, self.backup_group))
        else:
            self.log.info({'action': 'File Unchanged', 'path': filedata['filepath'],
                           'run_id': self.run_id, 'backup_group': self.backup_group,
                           'count': matched, 'hash': new_hash})
        return new_hash

    def map_unchganged(self, cursor, filedata, new_id):
        # check if file is unchanges
        sql_updateunchanged = """
                                           Update BACKUPITEMS t
                                           inner join
                                           BACKUPITEMS as n
                                           on  t.id = n.id
                                           inner join BACKUPITEMS as c
                                           on c.file_id = n.file_id and c.FILESIZE = n.FILESIZE
                                           and c.lastmodified = n.lastmodified
                                           inner join (select max(id) as id from BACKUPITEMS
                                           where file_id =
                                              (Select id from FILES where path_hash = md5(concat(%s, '-', %s)))
                                           and hash is not null) x
                                           on c.id = x.id
                                           SET t.item_id = c.item_id, t.hash=c.hash
                                           where n.id = %s
                                       """
        sql_gethash = "select hash from BACKUPITEMS as b where b.id = %s"
        affected_rows = cursor.execute(sql_updateunchanged, (self.backup_group, filedata['filepath'],
                                                             new_id))
        mapped_hash = None
        if affected_rows > 0:
            cursor.execute(sql_gethash, new_id)
            rs = cursor.fetchone()
            mapped_hash = rs["hash"]
        return affected_rows, mapped_hash

    def get_basedirs(self, cursor):
        sql_dirs = 'Select PATH from DIRECTORY where BACKUPGROUP_ID = %s'
        # ---------------- Get Rlevant Base Dirs
        try:
            cursor.execute(sql_dirs, (self.backup_group))
            dirs = cursor.fetchall()
        except Exception as e:
            print("Exception")  # sql error
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)
        return dirs

    def new_connection(self):
        self.db_helper.close(self.db_data)
        self.db_data = self.db_helper.getDictCursor()
        self.cursor = self.db_data["cursor"]
        return self.cursor


def main():
    scan_files = ScanFiles(int(sys.argv[1]))
    scan_files.scan_for_files()


if __name__ == "__main__":
        main()
