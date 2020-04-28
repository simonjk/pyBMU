from filehelper import FileHelper

def main():
    fh = FileHelper()
    hash = fh.hash_file('D:\\backup\\test\\dir1234\\test.txt')
    bufferpath = fh.buffer_path_from_hash(hash, 1)
    fh.create_parent_if_not_exist(bufferpath)
    print(hash)
    print(bufferpath)


if __name__ == "__main__":
    main()