from queue import PriorityQueue

class URLFrontierPriorityQueue:
    def __init__(self,start_urls = []):
        self.url_frontier = PriorityQueue()
        for url in start_urls:
            self.add_url(url,1,1)

    def add_url(self, url,  inlink, wavenumber):
        priority = -(inlink*10 - wavenumber)
        self.url_frontier.put((priority,url))

    def get_url(self):
        return self.url_frontier.get()[1]
    
    def is_frontier_empty(self):
        return self.url_frontier.empty()
    
    def get_size(self):
        return self.url_frontier.qsize()
    
