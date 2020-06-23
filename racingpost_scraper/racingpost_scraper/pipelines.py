# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import json
import sqlite3
from hashlib import md5

DATABASE = "Racing_Post_Scrapes.db"


def hash_dict(d: dict) -> str:
    json_dict = json.dumps(d, sort_keys=True).encode("utf-8")
    return md5(json_dict).hexdigest()


def sort_dict(d: dict) -> dict:
    return dict(sorted(d.items(), key=lambda t: t[0]))


def generate_table_statement(table_name: str, pk_name: str, item: dict) -> str:
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({pk_name} " + \
           f"TEXT PRIMARY KEY, {', '.join([str(k) + ' TEXT' for k in item.keys() if str(k) != pk_name])})"


def generate_insert_statement(table: str, item: dict, pk_name: str, use_hash: bool) -> str:
    keys, values = [], []
    for k, v in item.items():
        if k != pk_name:
            keys.append(k)
        values.append("?")
    if use_hash:
        values.append("?")  # extra one for hash primary key
    keys_str = ', '.join(keys)
    values_str = ', '.join(values)
    return f"INSERT INTO {table} ({pk_name + ', ' + keys_str}) VALUES ({values_str})"


class UtilPipeline:
    def __init__(self, table: str, pk_name: str, use_hash=False):
        self.table = table
        self.pk_name = pk_name
        self.connection = sqlite3.connect(DATABASE)
        self.use_hash = use_hash
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table}'")
        self.table_exists = len(self.cursor.fetchall()) == 1

    def process_item(self, item):
        item_dict = sort_dict(item) if self.use_hash else item.deepcopy()

        if not self.table_exists:
            create_statement = generate_table_statement(self.table, self.pk_name, item)
            self.cursor.execute(create_statement)
            self.connection.commit()
            self.table_exists = True

        try:
            insert_statement = generate_insert_statement(self.table, item_dict, self.pk_name, self.use_hash)
            values = None
            if self.use_hash:
                values = [hash_dict(item_dict)] + list(item_dict.values())
            else:
                values = list(item_dict.values())
            self.cursor.execute(insert_statement, values)
        except sqlite3.IntegrityError:
            pass  # means item is already in database
        except sqlite3.OperationalError:
            print(item)

    def close(self):
        self.connection.commit()
        self.connection.close()


class RaceCardItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Races", "race_id", use_hash=True)

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


class RaceRunnerItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Race_Runners", "runner_id", use_hash=True)

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


class HorseItemPipeline:
    def open_spider(self, spider):
        self.pipeline = UtilPipeline("Horses", "name")

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item
