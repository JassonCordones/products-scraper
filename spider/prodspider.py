import scrapy
import math
import os
import sys 
    
from scrapy.crawler import CrawlerProcess

class ProductsSpider(scrapy.Spider):
    name = "products_spider"

    def start_requests(self):
        try:
            with open('products.csv', 'a+') as f:
                f.truncate(0)
        except IOError as identifier:
            # print("File already openend, please close products.csv file")
            sys.exit("File already openend, please close products.csv file")
        
        urls = ['https://sidip.gob.do/dn.php']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_urls)
            
    def parse_urls(self, response):
        links_to_follow = response.xpath('/html/body/div[1]/div/div/div[3]/div[1]/ul//li[not(contains(@class,"has-sub"))]//@href').extract()
        for url in links_to_follow:
            yield response.follow(url=url, callback=self.parse_products)
            
    def parse_products(self, response):

        filepath = "products.csv"

        data = response.xpath('/html/body/div[1]/div/div/div[3]/div[2]/table//tr//td[contains(@align,"left")]//text()')
        table_rows = range(int(len(data.extract())/3))
        rows = [[product.strip().replace(',',"") for index, product in enumerate(data.extract()) if math.floor(index/3) == n] for n in table_rows]
        
        try:
            myfile = open(filepath, 'a+')
            with myfile as f:
                f.writelines([', '.join(row) + "\n" for row in rows]).encode('utf-8')
        except IOError as identifier:
            # print("File already openend, please close products.csv file")
            sys.exit("File already openend, please close products.csv file")

        
process = CrawlerProcess()

process.crawl(ProductsSpider)

process.start()

