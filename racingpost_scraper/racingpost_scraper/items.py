# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class RaceItem(scrapy.Item):
    title = scrapy.Field()
    course_name = scrapy.Field()
    track_type = scrapy.Field()
    race_date = scrapy.Field()  # YYYY/MM/DD
    race_time = scrapy.Field()
    url = scrapy.Field()


class RaceRunnerItem(scrapy.Item):
    no_form = scrapy.Field()
    horse = scrapy.Field()
    horse_url = scrapy.Field()
    age = scrapy.Field()
    wgt = scrapy.Field()
    _or = scrapy.Field()
    jockey = scrapy.Field()
    jockey_url = scrapy.Field()
    trainer = scrapy.Field()
    trainer_url = scrapy.Field()
