# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class RacingpostScraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class RaceCardItem(scrapy.Item):
    title = scrapy.Field()
    course_name = scrapy.Field()
    track_type = scrapy.Field()
    time = scrapy.Field()
    url = scrapy.Field()
