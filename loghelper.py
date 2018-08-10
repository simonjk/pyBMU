import logging
import logging.handlers

from cmreslogging.handlers import CMRESHandler


class LogHelper():

    def getLogger(self):
        handler = logging.handlers.SysLogHandler(address=('logsene-receiver-syslog.sematext.com', 514))
        formater = logging.Formatter("5c17a0d4-1603-4fa1-bb19-b35818b9a77e:%(message)s")
        handler.setFormatter(formater)
        log = logging.getLogger("BackupMeUp")
        log.setLevel(logging.INFO)
        log.addHandler(handler)
        return log

