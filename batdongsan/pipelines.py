# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import pymongo
import json
# from bson.objectid import ObjectId
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import csv
import os #

class MongoDBUnitopPipeline:

    def __init__(self):

        # econnect = str(os.environ['Mongo_HOST']) 
        # self.client = pymongo.MongoClient('mongodb://'+econnect+':27017')

        self.client = pymongo.MongoClient('mongodb://localhost:27017')

        self.db = self.client['db_BDS'] #Database      
        pass
    def process_item(self, item, spider):
        
        collection =self.db['coll_BDS'] #Collections
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")       
        pass
