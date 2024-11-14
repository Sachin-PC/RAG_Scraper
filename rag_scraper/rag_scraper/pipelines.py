from itemadapter import ItemAdapter
# from langchain.document_loaders import HTMLLoader
from langchain_unstructured import UnstructuredLoader
from langchain_community.document_loaders import UnstructuredHTMLLoader
from langchain.schema import Document
from bs4 import BeautifulSoup
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
import os
import time
import scrapy
import logging
import time
import rag_scraper.config as config
from urllib.parse import urlparse
# import nltk
# nltk.download('punkt')



class RagScraperPipeline:

    def __init__(self):
        print("InitializinG RagScrapperPipeline")
        self.data_file_directory = config.DATA_FILES_DIRECTORY_PATH
        model_name = config.EMBEDDING_MODEL_NAME
        self.hf_embeddings = HuggingFaceEmbeddings(
            model_name=model_name,
        )

    def process_item(self, item, spider):
        url_hash = hash(item["url"])
        timestamp = int(time.time() * 1000)
        file_name = f"response_{url_hash}_{timestamp}.html"
        file_path = os.path.join(self.data_file_directory,file_name)
        with open(file_path,"w") as file:    
            file.write(item['html_content'])
        return item

    def chunk_data(self, documents):
        text_splitter = RecursiveCharacterTextSplitter(
            separators= config.CHUNK_SEPERATORS,
            chunk_size=config.CHUNK_SIZE,
            chunk_overlap=config.CHUNK_OVERLAP
        )
        split_docs = text_splitter.split_documents(documents)
        # with open("split_docs.txt","w") as file:    
        #     for split_doc in split_docs:
        #         file.write(split_doc.page_content+"\n")
        self.embed_data(split_docs)

    def embed_data(self, documents):
        split_texts = [document.page_content for document in documents]
        embeddings = self.hf_embeddings.embed_documents(split_texts)
        # print("ZZZZZZZZZZZZ")
        # print("len(embeddings) = ",len(embeddings))
        # for embedding in embeddings:
        #     print(embedding)
        # print("ZZZZZZZZZZZZ")



class UrlMetadataPipeline:

    def __init__(self):
        print("Initializing UrlMetadataPipeline")
        # self.allowed_domains_set = config.INITIAL_ALLOWED_DOMAINS
        self.allowed_domains_set = set()
        self.crawled_domains_count = 0
        self.visited_urls = []
        self.blacklisted_domains = set()
        self.max_urls_to_crawl = config.MAX_URLS_TO_CRAWL
        self.urls_crawled_count = 0
        self.mod_val = config.CHECKPOINT_MOD_VAL
        self.visited_urls_written_to_file = 0
        self.visited_urls_file_path = os.path.join(config.URL_METADATA_DIRECTORY_PATH,config.VISITED_URLS_FILENAME)
        self.visited_urls_metadata_path = os.path.join(config.URL_METADATA_DIRECTORY_PATH,config.VISITED_URLS_METADATA_FILENAME)
        os.makedirs(config.URL_METADATA_DIRECTORY_PATH, exist_ok=True)

    # @classmethod
    # def from_crawler(cls, crawler):
        
    #     # Create the UrlMetadataPipeline instance
    #     urlMetadataPipeline = cls()
    #     # Set a reference to the UrlMetadataPipeline on the spider
    #     crawler.signals.connect(lambda spider: setattr(spider, 'urlMetadataPipeline', urlMetadataPipeline), signal=scrapy.signals.spider_opened)
    #     return urlMetadataPipeline

    @classmethod
    def from_crawler(cls, crawler):
        # Create the UrlMetadataPipeline instance
        pipeline = cls()
        # # Set a reference to the UrlMetadataPipeline on the spider
        # crawler.signals.connect(lambda spider: setattr(spider, 'urlMetadataPipeline', urlMetadataPipeline), signal=scrapy.signals.spider_opened)
        return pipeline
    
    def open_spider(self, spider):
        setattr(spider, 'urlMetadataPipeline',self)
        logging.info("\nMetadataPipeline set on Spider\n")

    def process_item(self, item, spider):
        self.visited_urls.append(item['url'])
        self.urls_crawled_count += 1
        parsed_url = urlparse(item['url'])
        if parsed_url.netloc not in self.allowed_domains_set:
            self.allowed_domains_set.add(parsed_url.netloc)
            self.crawled_domains_count += 1
            with open("domains_test.txt","a") as file:  
                file.write(f"{self.crawled_domains_count} : "+parsed_url.netloc+"\n")
        
        if (self.urls_crawled_count %  self.mod_val) == 0:
            self.update_metadata_file()    
        return


    def update_metadata_file(self):
        with open(self.visited_urls_file_path,"a") as file:  
            for i in range(self.visited_urls_written_to_file, self.urls_crawled_count):
                file.write(str(i)+" : "+self.visited_urls[i]+"\n")
            
        self.visited_urls_written_to_file = self.urls_crawled_count

        with open(self.visited_urls_metadata_path,"w") as file:  
            file.write("urls crawled : "+str(self.urls_crawled_count)+"\n")
            file.write("Total Domains Accessed : "+str(self.crawled_domains_count)+"\n")
