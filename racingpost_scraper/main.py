import sqlite3

from racingpost_scraper.pipelines import DATABASE
from racingpost_scraper.spiders.race_spider import RaceSpider
from racingpost_scraper.spiders.racecard_spider import RaceCardSpider
from racingpost_scraper.spiders.horse_spider import HorseSpider
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scripts.progeny_stats import generate_sire_table

configure_logging()
runner = CrawlerRunner()

# (spider_name, table, url_col)
crawlers = [
    (RaceSpider, "Races", "url"),
    (RaceCardSpider, "Race_Runners", "horse_url"),
    (HorseSpider, "Horses", "tmp"),
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

    reactor.stop()


def main():
    crawl()
    reactor.run()
    connection = sqlite3.connect(DATABASE)
    cursor = connection.cursor()
    cursor.execute(f"SELECT sire, sire_url, dam_sire, dam_sire_url FROM Horses")
    rows = cursor.fetchall()
    horses = []
    for row in rows:
        horses.append((row[0], row[1]))
        horses.append((row[2], row[3]))
    generate_sire_table(horses)

    print("JOB COMPLETE!")


def main2():
    process = CrawlerProcess(get_project_settings())

    connection = sqlite3.connect(DATABASE)
    cursor = connection.cursor()
    url_query = "SELECT horse_url FROM Race_Runners"
    start_urls = [t[0] for t in cursor.execute(url_query)]
    connection.close()

    process.crawl("horse-spider", start_urls=start_urls)
    process.start()


if __name__ == '__main__':
    main()
