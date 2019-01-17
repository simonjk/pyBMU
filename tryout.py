from filehelper import FileHelper

def main():
    fh = FileHelper()
    hash = fh.hash_file('D:\\backup\\test\\dir1234\\test.txt')
    print(hash)


if __name__ == "__main__":
    main()