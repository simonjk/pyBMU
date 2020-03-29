import hashlib
import string
import traceback
from pathlib import Path
import shutil
import os


class FileHelper():


    def freespace(self, path):

        total, used, free = shutil.disk_usage(path)
        # print("Remaining Space in %s is %s" % (path, free))
        return free

    def path_from_hash(self, root, drive, hash):
        reserved = ['com1','com2','com3','com4','com5','com6','com7','com8','com9','lpt1','lpt2','lpt3','lpt4','lpt5','lpt6','lpt7','lpt8','lpt9']
        fourset = hash[:4]
        if fourset in reserved:
            fourset = fourset + '_'
        return root+'\\'+drive+'\\'+hash[:2]+'\\'+fourset+'\\'+hash[:6]+'\\'+hash[:8]+'\\'+hash

    def file_exists(self, file):
        my_file = Path(file)
        return my_file.is_file()


    def create_parent_if_not_exist(self, file):
        my_file = Path(file)
        parent = my_file.parent
        if not parent.is_dir():
            os.makedirs(parent)


    def copy_file(self, src, tgt):
        try:
            self.create_parent_if_not_exist(tgt)
            shutil.copy(src,tgt)
            return True
        except Exception as e:
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)
            return False

    def move_file(self, src, tgt):
        self.create_parent_if_not_exist(tgt)
        shutil.move(src, tgt)

    def delete_file(self, tgt):
        os.remove(tgt)

    def hash_file(self, path):
        hash = ''
        BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


        sha256 = hashlib.sha256()
        try:
            with open(path, 'rb') as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    sha256.update(data)
            hash = self.hex_to_base36(sha256.hexdigest())
        except Exception as e:
            print(e)
            tb = e.__traceback__
            traceback.print_tb(tb)
            return None

        return hash

    def hex_to_base36(self, hex):
        ALPHABET = string.digits + string.ascii_lowercase
        n = int(hex, 16)

        s = []
        while True:
            n, r = divmod(n, 36)
            s.append(ALPHABET[r])
            if n == 0: break
        return ''.join(reversed(s))


    def get_parent(self, path):
        return os.path.dirname(path)

    def get_filename(self, path):
        return os.path.basename(path)