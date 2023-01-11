import scrapy


class XportaliaSpider(scrapy.Spider):
    name = 'xportalia'

    def start_requests(self):
        for url in start_urls:
            
