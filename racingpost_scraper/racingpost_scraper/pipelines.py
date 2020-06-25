# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import json
import sqlite3
from hashlib import md5
from typing import List

DATABASE = "Racing_Post_Scrapes.db"

from racingpost_scraper.items import TrainerStatsItem
from racingpost_scraper.items import TrainerRecordItem

def hash_dict(d: dict, dont_hash_keys: List[str] = "") -> str:
    new_dict = {}
    for k, v in d.items():
        if k not in dont_hash_keys:
            new_dict[k] = v
    # sorting keys enforces deterministic hashes
    json_dict = json.dumps(new_dict, sort_keys=True).encode("utf-8")
    return md5(json_dict).hexdigest()


class UtilPipeline:
    def __init__(self, table: str, primary_keys: List[str], use_hash=False):
        if len(primary_keys) < 1:
            raise Exception("At least one primary key needs to be specified.")
        if use_hash and len(primary_keys) != 1:
            raise Exception("Only a single primary key can be specified when using a row hash primary key.")
        self.table = table
        self.primary_keys = primary_keys
        self.connection = sqlite3.connect(DATABASE)
        self.use_hash = use_hash
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table}'")
        self.__table_exists = len(self.cursor.fetchall()) == 1

    def process_item(self, item):
        item_dict = dict(item.deepcopy())

        # add the hash primary key to the item
        if self.use_hash:
            item_dict[self.primary_keys[0]] = hash_dict(item_dict)

        if not self.__table_exists:
            create_statement = self.generate_table_statement(item_dict.keys())
            self.cursor.execute(create_statement)
            self.connection.commit()
            self.__table_exists = True

        try:
            keys = [str(k) for k in item_dict.keys()]
            values = [str(v) for v in item_dict.values()]
            insert_statement = self.generate_insert_statement(keys)  # TODO: make this into a replace statement
            self.cursor.execute(insert_statement, values)
        except sqlite3.IntegrityError:
            pass  # means item is already in database
        except sqlite3.OperationalError:
            print(item)

    def close(self):
        self.connection.commit()
        self.connection.close()

    def generate_table_statement(self, fields: List[str]) -> str:
        cols = [field + " TEXT" for field in fields]
        cols_str = ', '.join(cols)
        pks_str = ', '.join(self.primary_keys)
        return f"CREATE TABLE IF NOT EXISTS {self.table} ({cols_str}, PRIMARY KEY ({pks_str}))"

    def generate_insert_statement(self, fields: List[str]) -> str:
        values = []
        for _ in range(len(fields)):
            values.append("?")
        keys_str = ', '.join(fields)
        values_str = ', '.join(values)
        return f"INSERT INTO {self.table} ({keys_str}) VALUES ({values_str})"


class RaceCardItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Races", ["race_id"], use_hash=True)

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


class RaceRunnerItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Race_Runners", ["runner_id"], use_hash=True)

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


class HorseItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Horses", ["name"])

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


class SireProgenyStatsItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Sires", ["horse_name", "category"])

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


# processes 2 different types of items, TrainerStats and TrainerRecord
class TrainerItemPipeline:
    def open_spider(self, spider):
        self.trainer_stats_pipeline = UtilPipeline("Trainer_Stats", ["trainer_uid", "seasonStartDate", "seasonEndDate"])
        self.trainer_record_pipeline = UtilPipeline("Trainer_Records", ["trainer_uid", "n_years", "groupName"])

    def close_spider(self, spider):
        self.trainer_stats_pipeline.close()
        self.trainer_record_pipeline.close()

    def process_item(self, item, spider):
        if isinstance(item, TrainerStatsItem):
            self.trainer_stats_pipeline.process_item(item)
            self.trainer_stats_pipeline.connection.commit()
        elif isinstance(item, TrainerRecordItem):
            self.trainer_record_pipeline.process_item(item)
            self.trainer_record_pipeline.connection.commit()
        else:
            raise Exception(f"Unexpected Item Type - {type(item)}")
        return item
