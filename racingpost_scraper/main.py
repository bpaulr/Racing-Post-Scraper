from racingpost_scraper.spiders.race_spider import RaceSpider

from datetime import datetime
from pathlib import Path
from typing import List

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def load_start_urls(file: str) -> List[str]:
    with open(file, "r") as f:
        lines = f.readlines()
    i = 0
    while i < len(lines):
        if lines[i] is None or lines[i] == "":
            lines.pop(i)
        else:
            i += 1
    return lines


def main1():
    date = str(datetime.now()).replace(" ", "_").split(".")[0]

    path = Path(__file__).parent.absolute()
    data_path = Path.joinpath(path, Path("/data"))
    file_suffix = date + ".csv"
    feed_uri = str(data_path) + "/{}_" + file_suffix

    spiders = [
        "race-spider",
    ]

    settings = get_project_settings()
    settings["FEED_FORMAT"] = "csv"

    last_file = None
    for spider in spiders:
        settings["FEED_URI"] = "file:///" + feed_uri.format(spider)
        last_file = feed_uri.format(spider)
        process = CrawlerProcess(settings)
        if last_file is not None:
            process.crawl(spider, start_urls=load_start_urls(last_file))
        else:
            process.crawl(spider)
        process.start()


def main2():
    process = CrawlerProcess(get_project_settings())

    process.crawl("race-spider")
    process.start()


if __name__ == '__main__':
    main2()
