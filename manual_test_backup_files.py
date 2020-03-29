from backup_files import BackupFiles
import random
import string


def main():

    bf = BackupFiles()
    drivepath = 'D:\\backup\\test'

    print('------')
    print("get files_to_save")
    fts_i = bf.get_filestosave(1, False)
    fts_e = bf.get_filestosave(1, True)
    print("internal length: %s" % len(fts_i))
    print("external length: %s" % len(fts_e))
    print(fts_i)
    print(fts_e)
    print()

    print('------')
    print("get drive")
    drive_i = bf.get_drive(1, False)
    drive_e = bf.get_drive(1, True)
    print("internal drive: %s" % drive_i)
    print("external drive: %s" % drive_e)
    print()

    print('------')
    print("get free Space")
    disk, logical = bf.get_free_space(drive_i, drivepath)

    print("disk: %s" % disk)
    print("logical: %s" % logical)
    print()

    print('------')
    print("get create item check for item")
    a_hash = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
    print("Hash: %s" % a_hash)
    i_id = bf.is_hash_known(a_hash, 1)
    print("item_id before: %s" % i_id)
    bf.create_item(1, a_hash, False, -99)
    i_id = bf.is_hash_known(a_hash, 1)
    print("item_id after: %s" % i_id)
    bf.mark_item(1, a_hash, True, -33)
    bf.change_item_in_bui(68219837, i_id, a_hash)
    print()


if __name__ == "__main__":
    main()
