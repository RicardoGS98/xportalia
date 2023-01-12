import logging

from scrapy.http import HtmlResponse, Request

logger = logging.getLogger(__name__)


class RetryMiddleware(object):
    def process_response(self, request: Request, response: HtmlResponse, spider):
        if response.status == 500:
            return self._retry(request)
        elif response.status == 404:
            return response
        return response

    def _retry(self, request: Request):
        retry_req = request.copy()
        retry_req.dont_filter = True
        return retry_req
