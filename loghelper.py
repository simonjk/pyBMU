import os
import sys
import logging
import logging.handlers
from cmreslogging.handlers import CMRESHandler


class LogHelper():

    def getLogger(self):
        handler = logging.handlers.SysLogHandler(address=('logsene-receiver-syslog.sematext.com', 514))
        handler2 = CMRESHandler(hosts=[{'host': os.getenv('BMU_ES_HOST'), 'port': 9200}],
                               auth_type=CMRESHandler.AuthType.BASIC_AUTH,
                               auth_details=(os.getenv('BMU_ES_USR'), os.getenv('BMU_ES_PWD')),
                               es_index_name="i_bmu")
        formater = logging.Formatter("4c971c88-73a9-4557-b534-4fb4cebc6d48:%(message)s")
        console_formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formater)
        console = logging.StreamHandler(sys.stdout)
        log = logging.getLogger("BackupMeUp")
        log.setLevel(logging.INFO)
        log.addHandler(console)
        log.addHandler(handler)
        log.addHandler(handler2)
        return log

