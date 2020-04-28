import os
import pymysql

class DBHelper():
    def getDictCursor(self):
        # Open database connection
        db = pymysql.connect(os.getenv('BMU_DB_HOST'), os.getenv('BMU_DB_USR'), os.getenv('BMU_DB_PWD'),
                             os.getenv('BMU_DB_NAME'), use_unicode=True, charset="utf8mb4")
        db.autocommit(True)

        # prepare a cursor object using cursor() method
        cursor = db.cursor(pymysql.cursors.DictCursor)

        return {"db":db, "cursor":cursor}

    def getTestDictCursor(self):
        # Open database connection
        db = pymysql.connect(os.getenv('BMU_TST_DB_HOST'), os.getenv('BMU_TST_DB_USR'), os.getenv('BMU_TST_DB_PWD'),
                             os.getenv('BMU_TST_DB_NAME'), use_unicode=True, charset="utf8mb4")

        db.autocommit(True)
        # prepare a cursor object using cursor() method
        cursor = db.cursor(pymysql.cursors.DictCursor)

        return {"db":db, "cursor":cursor}

    def getCursor(self):
        # Open database connection
        db = pymysql.connect(os.getenv('BMU_DB_HOST'), os.getenv('BMU_DB_USR'), os.getenv('BMU_DB_PWD'),
                             os.getenv('BMU_DB_NAME'), use_unicode=True, charset="utf8mb4")

        db.autocommit(True)
        # prepare a cursor object using cursor() method
        cursor = db.cursor()

        return {"db":db, "cursor":cursor}

    def close(self, db_data):
        db_data["cursor"].close()
        db_data["db"].close()
