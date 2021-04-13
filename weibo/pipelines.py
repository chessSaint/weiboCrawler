# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import copy
import csv
import os

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings

settings = get_project_settings()

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class MongoPipeline(object):
    def open_spider(self, spider):
        try:
            from pymongo import MongoClient
            self.client = MongoClient(settings.get('MONGO_URI'))
            self.db = self.client['weibo']
            self.collection = self.db['weibo']
        except ModuleNotFoundError:
            spider.pymongo_error = True

    # insert_flag = 0
    def process_item(self, item, spider):
        try:
            import pymongo

            new_item = copy.deepcopy(item)
            if not self.collection.find_one({'id': new_item['id']}):
                self.collection.insert_one(dict(new_item))
            else:
                self.collection.update_one({'id': new_item['id']},
                                           {'$set': dict(new_item)})
        except pymongo.errors.ServerSelectionTimeoutError:
            print('error')
            spider.mongo_error = True

    def close_spider(self, spider):
        try:
            self.client.close()
        except AttributeError:
            pass

class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()
    # flag = 1
    def process_item(self, item, spider):
        # print(item['id'] + ',' + str(self.flag))
        # self.flag += 1
        if item['id'] in self.ids_seen:
            raise DropItem("过滤重复微博: %s" % item)
        else:
            self.ids_seen.add(item['id'])
            return item