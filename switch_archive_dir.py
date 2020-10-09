import os, stat
from datetime import datetime
from pathlib import Path


def main():
    basepath = "/share/CACHEDEV1_DATA/backup/"
    currentMonth = str(datetime.now().month).rjust(2, '0')
    currentYear = str(datetime.now().year).rjust(4, '0')

    targetstr = basepath+currentYear+"-"+currentMonth
    target = Path(targetstr)

    if not target.is_dir():
        os.makedirs(target)
    os.chmod(target, 0o777)
    link = Path(basepath+"current")
    os.unlink(link)
    os.symlink(target, link)
    mksubdir(targetstr, 'db')
    mksubdir(targetstr, 'Evernote')
    mksubdir(targetstr, 'Archive')


def mksubdir(targetpath, subdir):
   subdir = Path(targetpath+'/'+subdir)
   if not subdir.is_dir():
        os.makedirs(subdir)
   os.chmod(subdir, 0o777)

if __name__ == "__main__":
        main()
:complex