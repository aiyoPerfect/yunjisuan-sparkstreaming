import scrapy
import logging
from mystackoverflow_spider.items import MystackoverflowSpiderItem

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('monitor')
logger.setLevel(logging.INFO)





class StackoverflowSpider(scrapy.Spider):
    name = 'stackoverflow'

    def __init__(self):
        self.count = 1

    def start_requests(self):
        _url = 'https://stackoverflow.com/questions?tab=Newest&page={page}&pagesize=50'
        urls = [_url.format(page=page) for page in range(1, 1000)]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for index in range(1, 51):
            self.count += 1
            if self.count % 100 == 0:
                logger.info(self.count)

            sel = response.xpath('//*[@id="questions"]/div[{index}]'.format(index=index))
            item = MystackoverflowSpiderItem()
            item['votes'] = sel.xpath(
                'div[1]/div[2]/div[1]/div[1]/span/strong/text()').extract()
            item['answers'] = sel.xpath(
                'div[1]/div[2]/div[2]/strong/text()').extract()
            item['views'] = "".join(
                sel.xpath('div[1]/div[3]/@title').extract()).split()[0].replace(",", "")
            item['questions'] = sel.xpath('div[2]/h3/a/text()').extract()
            item['links'] = "".join(
                sel.xpath('div[2]/h3/a/@href').extract()).split("/")[2]
            tags=[]
            # for num in range(1,3):
            #     strresponse = sel.xpath('//ul/li[@class="d-inline mr4 js-post-tag-list-item"][1]/a/text()')
            #     tags = tags + strresponse.extract()
            item['tags'] = sel.xpath('//*[@id="questions"]/div[{index}]'.format(index=index)+'//ul/li[@class="d-inline mr4 js-post-tag-list-item"]/a/text()').extract()
            yield(item)
