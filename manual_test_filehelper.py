from filehelper import FileHelper
import os
import random
import string


def main():
    fh = FileHelper()
    src = 'D:\\backup\\test\\dir1234\\test.txt'

    print('------')
    print("get parent test")
    parent = fh.get_parent(src)
    print(parent)
    print()

    print('------')
    print("get filename test")
    basename = fh.get_filename(src)
    print(basename)
    print()

    print('------')
    print("hash test")
    fhash = fh.hash_file(src)
    print(fhash)
    print()

    print('------')
    print("path from hash test")
    tgt = fh.path_from_hash('D:\\backup\\test', 'TST0001', fhash)
    print(tgt)
    print()

    print('------')
    print("move test")
    fh.move_file(src, tgt)
    if os.path.isfile(tgt):
        print("OK: Target Exists")
    else:
        print("Error: Target does not Exists")
    if os.path.isfile(src):
        print("Error: Source Exists")
    else:
        print("OK: Source does not Exists")
    print()

    print('------')
    print("copy test")
    fh.copy_file(tgt, src)
    if os.path.isfile(tgt):
        print("OK: Target Exists")
    else:
        print("Error: Target does not Exists")
    if os.path.isfile(src):
        print("OK: Source Exists")
    else:
        print("Error: Source does not Exists")
    print()

    print('------')
    print("delete test")
    fh.delete_file(tgt)
    if os.path.isfile(tgt):
        print("Error: Target Exists")
    else:
        print("OK: Target does not Exists")
    if os.path.isfile(src):
        print("OK: Source Exists")
    else:
        print("Error: Source does not Exists")
    print()

    print('------')
    print("create parent test")
    basedir = 'D:\\backup\\test\\'
    dir1 = basedir+''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    dir2 = dir1+'\\'+''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    filename = dir2+"\\"+"test.txt"
    print(dir1)
    print(dir2)
    print(filename)
    fh.create_parent_if_not_exist(filename)
    if os.path.isdir(dir1):
        print("OK: Dir1 Exists")
    else:
        print("Error: Dir1 does not Exists")
    if os.path.isfile(src):
        print("OK: Dir2 Exists")
    else:
        print("Error: Dir2 does not Exists")


if __name__ == "__main__":
    main()
