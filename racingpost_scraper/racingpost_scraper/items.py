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


class HorseItem(scrapy.Item):
    name = scrapy.Field()
    age = scrapy.Field()
    breeder = scrapy.Field()
    trainer = scrapy.Field()
    owners = scrapy.Field()
    sire = scrapy.Field()
    sire_url = scrapy.Field()
    dam_sire = scrapy.Field()
    dam_sire_url = scrapy.Field()


# field names are the same as the json keys that the website api uses
class SireProgenyStatsItem(scrapy.Item):
    horse_name = scrapy.Field()
    broodmareCategory = scrapy.Field()
    category = scrapy.Field()
    percentWinnersRunners = scrapy.Field()
    percentWinsRuns = scrapy.Field()
    place2ndNumber = scrapy.Field()
    place3rdNumber = scrapy.Field()
    runners = scrapy.Field()
    runs = scrapy.Field()
    totalPrizeMoney = scrapy.Field()
    winPrizeMoney = scrapy.Field()
    winners = scrapy.Field()
    wins = scrapy.Field()
