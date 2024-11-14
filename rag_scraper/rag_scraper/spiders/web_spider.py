import scrapy
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from w3lib.url import canonicalize_url
from rag_scraper.url_frontier_priority_queue import URLFrontierPriorityQueue
from bs4 import BeautifulSoup
import logging
import time
import rag_scraper.config as config
import os

class WebSpiderSpider(scrapy.Spider):
    name = "web_spider"

    def __init__(self, name = None, **kwargs):
        super().__init__(name, **kwargs)
        self.allowed_domains_set = set()
        self.domains_not_crawled = set()
        self.start_urls = config.SEED_URLS
        self.url_frontier = URLFrontierPriorityQueue()
        self.visited_urls = set()
        self.urls_metadata = {}
        self.blacklisted_domains = set()
        self.max_urls_to_crawl = config.MAX_URLS_TO_CRAWL
        self.urls_parsed = 0
        self.max_urls_considered = 2*config.MAX_URLS_TO_CRAWL #This is to keep check on the number of URLs which can be present in the queue
        self.urls_considered_till_now = 0
        os.makedirs(config.UNSUCCESFUL_REQUESTS_LOGS_DIRECTORY, exist_ok=True)
        self.unsuccesful_request_logs_file_path = os.path.join(config.UNSUCCESFUL_REQUESTS_LOGS_DIRECTORY,config.UNSUCCESFUL_REQUESTS_LOGS_FILENAME)
        self.domains_not_crawled_file_path = os.path.join(config.UNSUCCESFUL_REQUESTS_LOGS_DIRECTORY,config.DOMAINS_NOT_CRAWLED_LOGS_FILENAME)
        self.domains_not_crawled_count=0
        for url in self.start_urls:
            is_valid, canonicalized_url, canonicalized_url_domain = self.custom_canonicalize_url(url, check_for_domain=False)
            if is_valid:
                if canonicalized_url_domain not in self.blacklisted_domains:
                    if canonicalized_url_domain not in self.allowed_domains_set:
                        self.allowed_domains_set.add(canonicalized_url_domain)
                    inlink, wavenumber = self.update_url_metadata(canonicalized_url, 1,canonicalized_url_domain)
                    self.url_frontier.add_url(canonicalized_url, inlink, wavenumber)

        print("1111111111111111")
        print("Domains = ",self.allowed_domains_set)
        print("1111111111111111")

    def start_requests(self):
        while not self.url_frontier.is_frontier_empty() and self.urls_parsed < self.max_urls_to_crawl:
            url = self.url_frontier.get_url()
            if url not in self.visited_urls:
                self.urls_parsed += 1
                self.visited_urls.add(url)
                yield scrapy.Request(url, callback=self.parse, errback=self.error_handler, meta={'original_url': url}, dont_filter=True)

    def parse(self, response):
        if 200 <= response.status < 300:
            url = response.meta.get('original_url')
            html_content = response.text
            yield {'url': response.url, 'html_content': html_content}
            # yield {'response': response}

            count = 0
            if self.urls_considered_till_now < self.max_urls_considered:
                for link in response.css('a::attr(href)').getall():
                    count += 1
                    #if link starts with "/", then append to the current url
                    #else append the url
                    if bool(urlparse(link).netloc):
                        absolute_url = link
                    else:
                        absolute_url = urljoin(response.url, link)

                    is_valid, canonicalized_url, canonicalized_url_domain = self.custom_canonicalize_url(absolute_url)
                    if is_valid:
                        if canonicalized_url not in self.visited_urls:
                            if canonicalized_url_domain not in self.blacklisted_domains:
                                inlink, wavenumber = self.update_url_metadata(canonicalized_url, self.urls_metadata[url][1]+1, canonicalized_url_domain)
                                self.url_frontier.add_url(canonicalized_url, inlink, wavenumber)

            while not self.url_frontier.is_frontier_empty() and self.urls_parsed < self.max_urls_to_crawl:
                url = self.url_frontier.get_url()
                if url not in self.visited_urls:
                    self.urls_parsed += 1
                    self.visited_urls.add(url)
                    yield scrapy.Request(url, callback=self.parse, errback=self.error_handler, meta={'original_url': url}, dont_filter=True)

            logging.info(f"\nself.urls_metadata = :{len(self.urls_metadata)}\n")
        else:
            logging.info(f"\nNon Succesful response Received: {response.status}\n")

    def error_handler(self, failure):
        url = failure.request.meta.get('original_url')
        self.log(f"\n\nFailed to retrieve data for URL: {url}\n\n")
        self.log(f"\n\nError details: {failure.value}\n\n")
        with open(self.unsuccesful_request_logs_file_path,"a") as file:  
            file.write("----------------------------------------------------------------\n")
            file.write(f"Failed to retrieve data for URL: {url}\n")
            file.write(f"Error details: {failure.value}\n")
            file.write("----------------------------------------------------------------\n")

    def custom_canonicalize_url(self, absolute_url, check_for_domain = True):

        #parse the url
        parsed_url = urlparse(absolute_url)
        if parsed_url.scheme not in ('http', 'https'):
            return False, None, None
        
        if check_for_domain:
            #if the domain is not allowed to be crawled
            if parsed_url.netloc not in self.allowed_domains_set:
                if parsed_url.netloc not in self.domains_not_crawled:
                    self.domains_not_crawled.add(parsed_url.netloc)
                    with open(self.domains_not_crawled_file_path,"a") as file:  
                        self.domains_not_crawled_count += 1
                        file.write(f"{self.domains_not_crawled_count} : {parsed_url.netloc}\n")

                return False, None, None

        #remove the framgments which are not necessary
        parsed_url = parsed_url._replace(fragment="")

        #remove unnecessary query parameters and and sort the remoanining parameters so that urls with same parameters and values are not duplicated
        query_parameters = parse_qsl(parsed_url.query)
        filtered_sorted_q_parameters = sorted([(key, value) for key, value in query_parameters if key not in ["utm_source", "sessionid"]])
        final_parsed_url = parsed_url._replace(query=urlencode(filtered_sorted_q_parameters))

        #use the standard canonicalize library to perform default canonicalize operations using w3lib
        canonicalized_url = canonicalize_url(urlunparse(final_parsed_url))

        return True, canonicalized_url, final_parsed_url.netloc
    
    def update_url_metadata(self, url, wave_number, url_domain):
        if url in self.urls_metadata:
            self.urls_metadata[url][0] += 1
            self.urls_metadata[url][1] = min(self.urls_metadata[url][1], wave_number)
        else:
            #add the domain if it is not already present
            # if url_domain not in self.allowed_domains_set:
            #     self.allowed_domains_set.add(url_domain)
            self.urls_considered_till_now += 1
            self.urls_metadata[url] = [1, wave_number]  #1st value is inlinks, 2nd values is wave number

        return  self.urls_metadata[url][0], self.urls_metadata[url][1]
    
    def close(self,reason):
       # Access the UrlMetadataPipeline's method using the reference stored on the spider
        if hasattr(self, 'urlMetadataPipeline'):
            self.urlMetadataPipeline.update_metadata_file()
            logging.info("\nUrl Metadata updated. Below are the url crawled details:")
            logging.info(f"Url Parsed: {self.urls_parsed}")
            logging.info(f"Url Frontier Size: {self.url_frontier.get_size()}")
            logging.info(f"Url Metadata Size: {len(self.urls_metadata)}")
            logging.info("Url Metadata updated. Closing the Spyder")
            logging.info("Closing the Spyder\n\n")




