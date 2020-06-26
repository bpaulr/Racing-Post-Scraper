import sqlite3

from racingpost_scraper.pipelines import DATABASE
from racingpost_scraper.spiders.horse_spider import HorseSpider
from racingpost_scraper.spiders.race_spider import RaceSpider
from racingpost_scraper.spiders.racecard_spider import RaceCardSpider
from racingpost_scraper.spiders.sire_spider import SireSpider
from racingpost_scraper.spiders.jockey_spider import JockeySpider
from racingpost_scraper.spiders.trainer_spider import TrainerSpider
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer

configure_logging()
runner = CrawlerRunner()

# (spider_name, table, query)
crawlers = [
    (RaceSpider, ""),
    (RaceCardSpider, "SELECT url FROM Races"),
    (HorseSpider, "SELECT horse_url FROM Race_Runners"),
    (TrainerSpider, "SELECT trainer_url FROM Race_Runners"),
    (JockeySpider, "SELECT jockey_url FROM Race_Runners"),
]


@defer.inlineCallbacks
def crawl():
    for i in range(len(crawlers)):
        spider, query = crawlers[i]
        if i > 0:
            connection = sqlite3.connect(DATABASE)
            cursor = connection.cursor()
            start_urls = [t[0] for t in cursor.execute(query)]
            connection.close()
            yield runner.crawl(spider, start_urls=start_urls)
        else:
            yield runner.crawl(spider)

    connection = sqlite3.connect(DATABASE)
    cursor = connection.cursor()
    cursor.execute(f"SELECT sire, sire_url, dam_sire, dam_sire_url FROM Horses")
    rows = cursor.fetchall()
    horses = []
    for row in rows:
        horses.append((row[0], row[1]))
        horses.append((row[2], row[3]))

    yield runner.crawl(SireSpider, start_urls=horses)

    reactor.stop()


def main():
    crawl()
    reactor.run()

    print("JOB COMPLETE!")


def main2():
    process = CrawlerProcess(get_project_settings())

    process.crawl("jockey-spider")
    process.start()


if __name__ == '__main__':
    main()
