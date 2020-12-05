import scrapy
from datetime import datetime
import re
from scrapy.crawler import CrawlerProcess
import hashlib

year_month = '2020-11' 

class CNNSpider(scrapy.Spider):
    name = 'cnn'
    allowed_domains = ['www.cnn.com']
    now = datetime.now()
    start_urls = ['https://www.cnn.com/article/sitemap-'+year_month+'.html']

    def parse(self, response):
        articles = response.xpath('//div[@class = "sitemap-entry"]/ul/li')

        for art in articles:
            article_url = art.xpath('.//span[@class = "sitemap-link"]/a/@href').extract_first()

            yield scrapy.Request(article_url, callback=self.parse_article)
            
    def parse_article(self, response):
        title = response.xpath('//h1/text()').extract_first()
        body1 = ' '.join(response.xpath('//*[@id="body-text"]/div[1]/div[1]/p/text()').extract())
        body2 = ' '.join(response.xpath('//div[@class = "zn-body__paragraph speakable"]/text()').getall())
        body3 = ' '.join(response.xpath('//div[@class = "zn-body__paragraph"]/text()').getall())
        body = ' '.join([body1,body2,body3])
        language = 'english'
        url_short = 'www.cnn.com'
        url = response.url
        crawl_date = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        published_date = response.xpath('//article[@class = "pg-rail-tall pg-rail--align-right "]/meta[@itemprop = "datePublished"]/@content').extract_first()
        if published_date is None:
            published_date = year_month+'-15'       # default publish date = middle of month if 'None'
        else:
            published_date = published_date[:10]
        source = 'scrapy'
        site_category = response.xpath('//article[@class = "pg-rail-tall pg-rail--align-right "]/meta[@itemprop = "isPartOf"]/@content').extract_first()
        hashtag = ''
        #unique_id = abs(hash(url+crawl_date))
        unique_id = hashlib.md5((url+title).encode('utf-8')).hexdigest()

        yield{
            'unique_id': unique_id,
            'title': title,
            'body': body,
            'crawl_date': crawl_date,
            'published_date': published_date,
            'language': language,
            'url': url,
            'url_short': url_short,
            'site_category': site_category,
            'source': source
            #'hashtag': hashtag
        }

file_timestamp= datetime.now().strftime('%Y.%m.%d.%H%M%S')

# # # Run the Spider
# Run the Spider
process = CrawlerProcess(settings={
    'FEED_FORMAT': 'jsonlines',
#    'FEED_URI': 'articles/cnn_scrape'+file_timestamp+'.jsonl'})
    'FEED_URI': 'articles/cnn_scrape-'+year_month+'.jsonl'})
process.crawl(CNNSpider)
process.start()
