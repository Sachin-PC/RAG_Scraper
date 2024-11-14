# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
# from langchain.document_loaders import HTMLLoader
from langchain_unstructured import UnstructuredLoader
from langchain.schema import Document
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
import os
import time
import scrapy
import config
# import nltk
# nltk.download('punkt')


class UrlMetadataPipeline:

    def __init__(self):
        self.allowed_domains_set = config.INITIAL_ALLOWED_DOMAINS
        self.visited_urls = []
        self.blacklisted_domains = set()
        self.max_urls_to_parse = 100
        self.urls_parsed_count = 0
        self.mod_val = 100
        self.visited_urls_written_to_file = 0
        self.visited_urls_file_path = os.path.join("data_files/url_metadata","visited_urls.txt")
        self.visited_urls_metadata_path = os.path.join("data_files/url_metadata","visited_urls_metadata.txt")
        print("Initializing UrlMetadataPipeline")

    @classmethod
    def from_crawler(cls, crawler):
        
        # Create the UrlMetadataPipeline instance
        pipeline = cls()

        def set_spider_attr(spider):
            setattr(spider, 'urlMetadataPipeline',pipeline)
            print("ZZZZZZ\n\nurlMetadataPipeline set on Spider\n\n\n\n\n")
        
        # Set a reference to the UrlMetadataPipeline on the spider
        crawler.signals.connect(set_spider_attr, signal=scrapy.signals.spider_opened)
        # crawler.signals.connect(lambda spider: setattr(spider, 'urlMetadataPipeline', urlMetadataPipeline), signal=scrapy.signals.spider_opened)
        
        # crawler.signals.connect(pipeline.spider_closed, signal=scrapy.signals.spider_closed)
        return pipeline

    def process_item(self, item, spider):
        web_url=item["url"]
        print("XXXXXXXXXXXX\n\n\n")
        print("WEB URL = ",web_url)
        print("XXXXXXXXXXXX\n\n\n")
        self.visited_urls.append(item['url'])
        self.urls_parsed_count += 1
        if (self.urls_parsed_count %  self.mod_val) == 0:
            self.update_metadata_file()    


    def update_metadata_file(self):
        with open(self.visited_urls_file_path,"wb") as file:  
            for i in range(self.visited_urls_written_to_file, self.urls_parsed_count):
                file.write(self.visited_urls[i])
            
        self.visited_urls_written_to_file = self.urls_parsed_count

        with open(self.visited_urls_metadata_path,"wb") as file:  
            file.write("urls parsed : "+self.urls_parsed_count+"\n")
            file.write("Total Domains Accessed : "+len(self.allowed_domains_set)+"\n")


                

            
