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
           f"TEXT PRIMARY KEY, {', '.join([str(k) + ' TEXT' for k in item.keys()])})"


def generate_insert_statement(table: str, item: dict, pk_name: str) -> str:
    keys, values, statement_vals = [], [], []
    for k, v in item.items():
        keys.append(k)
        values.append(f"'{v}'")
    keys_str = ', '.join(keys)
    values_str = ', '.join(values)
    pk_value = f"'{hash_dict(item)}'"
    return f"INSERT INTO {table} ({pk_name + ', ' + keys_str}) VALUES ({pk_value + ', ' + values_str})"


class Util:
    def __init__(self, table: str, pk_name: str):
        self.table = table
        self.pk_name = pk_name
        self.connection = sqlite3.connect(DATABASE)
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table}'")
        self.table_exists = len(self.cursor.fetchall()) == 1

    def process_item(self, item):
        sorted_dict = sort_dict(item)

        if not self.table_exists:
            create_statement = generate_table_statement(self.table, self.pk_name, item)
            self.cursor.execute(create_statement)
            self.connection.commit()
            self.table_exists = True

        try:
            insert_statement = generate_insert_statement(self.table, sorted_dict, self.pk_name)
            self.cursor.execute(insert_statement)
        except sqlite3.IntegrityError:
            pass

    def close(self):
        self.connection.commit()
        self.connection.close()


class RaceCardItemPipeline:
    def open_spider(self, spider):
        self.pipeline = Util("Races", "race_id")

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item


class RaceRunnerItemPipeline:
    def open_spider(self, spider):
        self.pipeline = Util("Race_Runners", "runner_id")

    def close_spider(self, spider):
        self.pipeline.close()

    def process_item(self, item, spider):
        self.pipeline.process_item(item)
        return item
