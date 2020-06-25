import json
import os


# bug in treq/twisted (https://stackoverflow.com/questions/34358935/python-treq-fails-with-twisted-openssl-error-due-to-empty-trust-store-on-windows)
import certifi
os.environ["SSL_CERT_FILE"] = certifi.where()

import scrapy
import treq
from racingpost_scraper.items import SireProgenyStatsItem

API_TAG = "tab"
API_SANDWICH_STR = "profile/horse"  # what two paths to place the api tag in between
API_SANDWICH_STR_FORMAT = "profile/{}/horse"
PROG_SUFFIX = "progeny-statistics"


def get_prog_json_url(profile_url: str) -> str:
    return profile_url.replace(API_SANDWICH_STR, API_SANDWICH_STR_FORMAT.format(API_TAG)) + "/" + PROG_SUFFIX


class SireSpider(scrapy.Spider):
    name = "sire-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.SireProgenyStatsItemPipeline': 300,
        }
    }

    # start_urls = ["https://www.racingpost.com/profile/horse/738816/society-rock"]

    def start_requests(self):
        for horse_url in self.start_urls:
            horse_name, url = horse_url
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'horse_name': horse_name})

    async def parse(self, response, horse_name):
        full_url = get_prog_json_url(response.url)
        a = await treq.get(full_url)
        stats = await treq.json_content(a)

        if stats is None or not stats:
            return

        if "progenyStatistics" not in stats:
            return

        stats = stats["progenyStatistics"]

        lowest_key = None
        lowest_num = None
        for key in stats:
            num: str = key[:4]
            if not num.isdigit():
                continue
            if lowest_key is None or int(num) < lowest_num:
                lowest_num = int(num)
                lowest_key = key

        stats = stats[lowest_key]

        for stat in stats:
            sire_stats = SireProgenyStatsItem()
            sire_stats["horse_name"] = horse_name
            for k, v in stat.items():
                sire_stats[k] = v
            yield sire_stats
