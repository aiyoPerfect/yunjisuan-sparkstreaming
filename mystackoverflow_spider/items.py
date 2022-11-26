# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MystackoverflowSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    links = scrapy.Field()
    views = scrapy.Field()
    votes = scrapy.Field()
    answers = scrapy.Field()
    tags = scrapy.Field()
    questions = scrapy.Field()
