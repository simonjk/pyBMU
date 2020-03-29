import sys
import logging
import logging.handlers

from cmreslogging.handlers import CMRESHandler


class LogHelper():

    def getLogger(self):
        handler = logging.handlers.SysLogHandler(address=('logsene-receiver-syslog.sematext.com', 514))
        formater = logging.Formatter("837308c4-fd41-4e26-83ed-93038b0a086a:%(message)s")
        handler.setFormatter(formater)
        console = logging.StreamHandler(sys.stdout)
        log = logging.getLogger("BackupMeUp")
        log.setLevel(logging.INFO)
        log.addHandler(console)
        log.addHandler(handler)
        return log

