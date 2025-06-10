import random
from bs4 import BeautifulSoup
import requests

class APIconnector:
    def __init__(self, URL : str):
        self.URL = URL
        self.headers_list = [
                        "Java-http-client/ Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
                        'Java-http-client/ Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36']
        self.headers = {'User-Agent' : random.choice(self.headers_list),
                        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Sec-Fetch-Dest' : 'iframe'}
        self.response = None
        
    def make_soup(self):
        if not self.response:
            self.response = self.get_request()
        soup = BeautifulSoup(self.response.content, "html.parser")
        return soup
    
    def get_request(self):
        if not self.response:
            self.response = requests.get(self.URL, headers = self.headers, timeout=1000)
        return self.response