import logging

from scrapy import logformatter


class PoliteLogFormatter(logformatter.LogFormatter):
    def dropped(self, item, exception, *_):
        return {
            'level': logging.DEBUG,
            'msg': logformatter.DROPPEDMSG,
            'args': {
                'exception': exception,
                'item': item,
            }
        }
