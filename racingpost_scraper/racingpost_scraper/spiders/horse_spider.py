import json
import re

import scrapy
from racingpost_scraper.items import HorseItem

HORSE_PROFILE_URL = "https://www.racingpost.com/profile/horse/{}"  # uid


class HorseSpider(scrapy.Spider):
    name = "horse-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.HorseItemPipeline': 300,
        }
    }

    def parse(self, response):
        javascript: str = response.xpath('/html/body/script[1]/text()').get(default="")
        if javascript is None or javascript == "":
            return None

        start = javascript.find("{")
        start = javascript.find("{", start + 1)
        rev = javascript[::-1]
        end = rev.find("}")
        end = rev.find("}", end + 1)
        end = len(javascript) - end

        horse = HorseItem()
        horse_details = javascript[start:end]
        horse_details_json = json.loads(horse_details)
        profile = horse_details_json["profile"]
        horse["name"] = profile["horseName"]
        horse["age"] = re.sub("[^0-9]", "", profile["age"])  # only keep digits
        horse["breeder"] = profile["breederName"]
        horse["owners"] = profile["ownerName"]
        horse["trainer"] = profile["trainerName"]
        horse["sire"] = profile["sireHorseName"]
        horse["sire_url"] = HORSE_PROFILE_URL.format(profile["sireUid"])
        horse["dam_sire"] = profile["damSireHorseName"]
        horse["dam_sire_url"] = HORSE_PROFILE_URL.format(profile["damSireUid"])

        yield horse
