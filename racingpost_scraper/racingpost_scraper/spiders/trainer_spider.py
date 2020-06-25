import json

import scrapy
from racingpost_scraper.items import TrainerRecordItem
from racingpost_scraper.items import TrainerStatsItem

STAT_SUFFIX = "stats"
WANTED_FIELDS = set(["profile", "statisticalSummary", "recordsByType"])


class TrainerSpider(scrapy.Spider):
    name = "trainer-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.TrainerItemPipeline': 300,
        }
    }

    start_urls = ["https://www.racingpost.com/profile/trainer/4336/john-gosden"]

    def parse(self, response):
        javascript: str = response.xpath('/html/body/script[1]/text()').get()

        start = javascript.find("{")
        start = javascript.find("{", start + 1)
        rev = javascript[::-1]
        end = rev.find("}")
        end = rev.find("}", end + 1)
        end = len(javascript) - end

        trainer_details = javascript[start:end]
        trainer_details_json = json.loads(trainer_details)
        for key in list(trainer_details_json.keys()):
            if key not in WANTED_FIELDS:
                del trainer_details_json[key]

        trainer_uid = str(response.url).split("/")[-2]
        trainer_name = trainer_details_json["profile"]["trainerName"]

        trainer_stats_list = trainer_details_json["statisticalSummary"]
        for stat in trainer_stats_list:
            trainer_stats = TrainerStatsItem()
            trainer_stats["trainer_uid"] = trainer_uid
            trainer_stats["trainer_name"] = trainer_name
            for k, v in stat.items():
                trainer_stats[k] = v
            yield trainer_stats

        trainer_records = trainer_details_json["recordsByType"]
        for record_type in trainer_records.keys():
            # at the moment only care about GB flat races
            if record_type == "recByTypeGBFlat":
                record_years = trainer_records[record_type]
                for year in record_years.keys():
                    if str(year).isdigit():
                        record_json = record_years[year]["data"]["recordByRaceType"]
                        for category, record in record_json.items():
                            if str(category) == "total":
                                continue
                            trainer_record = TrainerRecordItem()
                            trainer_record["trainer_uid"] = trainer_uid
                            trainer_record["trainer_name"] = trainer_name
                            trainer_record["n_years"] = str(year)
                            for k, v in record.items():
                                trainer_record[k] = v
                            yield trainer_record
