import sqlite3

from racingpost_scraper.pipelines import DATABASE
from racingpost_scraper.spiders.horse_spider import HorseSpider
from racingpost_scraper.spiders.race_spider import RaceSpider
from racingpost_scraper.spiders.racecard_spider import RaceCardSpider
from racingpost_scraper.spiders.sire_spider import SireSpider
from racingpost_scraper.spiders.trainer_spider import TrainerSpider
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer

configure_logging()
runner = CrawlerRunner()

# (spider_name, table, url_col)
crawlers = [
    (RaceSpider, "Races", "url"),
    (RaceCardSpider, "Race_Runners", "horse_url"),
    (HorseSpider, "Horses", "name"),
]


@defer.inlineCallbacks
def crawl():
    for i in range(len(crawlers)):
        spider, table, url_col = crawlers[i]
        if i > 0:
            connection = sqlite3.connect(DATABASE)
            cursor = connection.cursor()
            past_table = crawlers[i - 1][1]
            past_url_col = crawlers[i - 1][2]
            url_query = f"SELECT {past_url_col} FROM {past_table}"
            start_urls = [t[0] for t in cursor.execute(url_query)]
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

    yield runner.crawl(TrainerSpider, start_urls=[s[0] for s in cursor.execute(f"SELECT trainer_url from Race_Runners")])

    reactor.stop()


def main():
    crawl()
    reactor.run()

    print("JOB COMPLETE!")


def main2():
    process = CrawlerProcess(get_project_settings())

    process.crawl("trainer-spider")
    process.start()


if __name__ == '__main__':
    main()
