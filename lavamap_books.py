import datetime, time
import scrapy
import scrapy.crawler as crawler
from scrapy.utils.project import get_project_settings
from multiprocessing import Process, Queue
from twisted.internet import reactor

def create_url_list(url, pages):
    url_list = []
    url_list.append(url + 'index.html')
    ctr = 2
    while ctr <= pages:
        url_list.append(url + 'catalogue/page-' + str(ctr) + '.html')
        ctr += 1
    return url_list

def run_spider(spider,output_name,cat_site,ctr):
    settings = get_project_settings()
    settings.set('S3PIPELINE_URL','s3://data-candidate-submission/'+output_name)
    settings.set('FEED_FORMAT', 'csv')
    settings.set('FEED_URI','s3://data-candidate-submission/'+output_name)

    def f(q):
        try:
            runner = crawler.CrawlerRunner(settings)#={
            deferred = runner.crawl(spider, st_url=cat_site)
            deferred.addBoth(lambda _: reactor.stop())
            reactor.run()
            q.put(None)
        except Exception as e:
            q.put(e)

    q = Queue()
    p = Process(target=f, args=(q,))
    p.start()
    result = q.get()
    p.join()
    if result is not None:
        raise result

class SpiderSpider(scrapy.Spider):
    name = 'spider'
    allowed_domains = ['books.toscrape.com']
    start_urls = []#['http://books.toscrape.com/']
    base_url = 'http://books.toscrape.com/'

    def __init__(self, st_url = None):
        self.start_urls.append(st_url)  # main_site_to_scan passed as var

    def parse(self, response):
        all_books = response.xpath('//article[@class="product_pod"]')

        for book in all_books:
            book_partial_url = book.xpath('.//h3/a/@href').extract_first()
            if 'catalogue/' not in book_partial_url:
                book_url = self.base_url + 'catalogue/' + book_partial_url
            else:
                book_url = self.base_url + book_partial_url

            yield scrapy.Request(book_url, callback=self.parse_book)
 

    def parse_book(self, response):
        title = response.xpath('//div/h1/text()').extract_first()

        relative_image = response.xpath('//div[@class="item active"]/img/@src').extract_first()
        final_image = self.base_url + relative_image.replace('../..', '')

        price = response.xpath(
            '//div[contains(@class, "product_main")]/p[@class="price_color"]/text()').extract_first()
        stock = response.xpath(
            '//div[contains(@class, "product_main")]/p[contains(@class, "instock")]/text()').extract()[1].strip()
        stars = response.xpath(
            '//div/p[contains(@class, "star-rating")]/@class').extract_first().replace('star-rating ', '')
        description = response.xpath(
            '//div[@id="product_description"]/following-sibling::p/text()').extract_first()
        upc = response.xpath(
            '//table[@class="table table-striped"]/tr[1]/td/text()').extract_first()
        price_excl_tax = response.xpath(
            '//table[@class="table table-striped"]/tr[3]/td/text()').extract_first()
        price_inc_tax = response.xpath(
            '//table[@class="table table-striped"]/tr[4]/td/text()').extract_first()
        tax = response.xpath(
            '//table[@class="table table-striped"]/tr[5]/td/text()').extract_first()

        yield {
            'Title': title,
            'Image': final_image,
            'Price': price,
            'Stock': stock,
            'Stars': stars,
            'Description': description,
            'Upc': upc,
            'Price after tax': price_excl_tax,
            'Price incl tax': price_inc_tax,
            'Tax': tax,
        }

#****MAIN VARIABLES******
url = 'http://books.toscrape.com/' #MAIN_URL
pgs = 50 #CATALOGUE PAGES TO SCRAPE
t_del = 300 #SECONDS OF DELAY
#************************
urls = create_url_list(url,pgs)
scrape_start = datetime.datetime.now()
iterations = len(urls)
ctr = 0

#****RUN LOOP************
while True:
    time_delay = t_del
    cur_time = datetime.datetime.now()
    if ctr==0: cur_time = scrape_start
    time_dif = cur_time-scrape_start
    diff = time_dif.total_seconds() % time_delay
    if diff <=0.0001: 
        out_name = "tom_corsitto_books_" + '{:02}'.format(ctr+1) + "_of_" + str(iterations) + "_"+cur_time.strftime('%m-%d-%Y-%H-%M-%S') + ".csv"
        cat_site = urls[ctr]
        run_spider(SpiderSpider,out_name,cat_site,ctr)
        ctr += 1
    elif ctr == iterations:
        print("COMPLETE")
        break
