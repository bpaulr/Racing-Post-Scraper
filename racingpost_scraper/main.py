from racingpost_scraper.spiders.racecard_spider import RaceCardSpider

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())

process.crawl("racecard-spider")
process.start()
