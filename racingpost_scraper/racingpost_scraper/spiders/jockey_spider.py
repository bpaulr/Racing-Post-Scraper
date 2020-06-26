import json

import scrapy
from racingpost_scraper.items import JockeyRecordItem
from racingpost_scraper.items import JockeyStatsItem

STAT_SUFFIX = "stats"
WANTED_DETAIL_FIELDS = set(["profile", "statisticalSummary", "recordsByType"])
WANTED_RECORD_TYPES = set(["recByTypeGBFlat"])


class JockeySpider(scrapy.Spider):
    name = "jockey-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.JockeyItemPipeline': 300,
        }
    }

    start_urls = ["https://www.racingpost.com/profile/jockey/98719/baptiste-le-clerc"]

    def parse(self, response):
        javascript: str = response.xpath('/html/body/script[1]/text()').get()

        start = javascript.find("{")
        start = javascript.find("{", start + 1)
        rev = javascript[::-1]
        end = rev.find("}")
        end = rev.find("}", end + 1)
        end = len(javascript) - end

        jockey_details = javascript[start:end]
        jockey_details_json = json.loads(jockey_details)
        for key in list(jockey_details_json.keys()):
            if key not in WANTED_DETAIL_FIELDS:
                del jockey_details_json[key]

        jockey_uid = jockey_details_json["profile"]["jockeyUid"]
        jockey_name = jockey_details_json["profile"]["jockeyName"]

        jockey_stats_list = jockey_details_json["statisticalSummary"]
        if jockey_stats_list is not None:
            for stat in jockey_stats_list:
                jockey_stats = JockeyStatsItem()
                jockey_stats["jockey_uid"] = jockey_uid
                jockey_stats["jockey_name"] = jockey_name
                for k, v in stat.items():
                    jockey_stats[k] = v
                yield jockey_stats

        jockey_records = jockey_details_json["recordsByType"]
        for record_type in jockey_records.keys():
            # at the moment only care about GB flat races
            if record_type == "recByTypeGBFlat":
                record_years = jockey_records[record_type]
                for year in record_years.keys():
                    if str(year).isdigit():
                        record_json = record_years[year]["data"]["recordByRaceType"]
                        for category, record in record_json.items():
                            if str(category) == "total":
                                continue
                            jockey_record = JockeyRecordItem()
                            jockey_record["jockey_uid"] = jockey_uid
                            jockey_record["jockey_name"] = jockey_name
                            jockey_record["n_years"] = str(year)
                            for k, v in record.items():
                                val = v if v is not None else ""
                                jockey_record[k] = val
                            yield jockey_record
