import csv
import scrapy
from datetime import datetime

class EbayCatSpider(scrapy.Spider):
    name = "ebay_cat"

    custom_settings = {
        'FEED_URI': f'outputs/ebay_categories_data_japan_new{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
        'FEED_FORMAT': 'csv',
        # 'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.3,
        'REDIRECT_ENABLED': False,
        'ZYTE_API_KEY': "a3ac097c634b437b8ce7eea576fa80d8",
        'ZYTE_API_TRANSPARENT_MODE': True,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
        },
        'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-full-version': '"126.0.6478.182"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    }

    def read_file(self):
        with open(f'input/search_names.csv', 'r')as file:
            data =list(csv.DictReader(file))
            return data

    def start_requests(self):
        input_file = self.read_file()
        for file_data in input_file[:1]:
            url = file_data.get('links', '')
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse
                                 , meta={'url': url})

    def parse(self, response):
        catgeory_url=response.meta.get('url','')
        category_name= catgeory_url.split('b/')[1].split('/')[0].strip()

        sub_cat= response.xpath("//div[@class='b-visualnav__grid']//span[@role='listitem'] | //ul[@class='carousel__list']//li[@class='carousel__snap-point']")
        for row in sub_cat[:]:
            url=row.xpath(".//a/@href").get('')
            sub_cat_name=row.xpath(".//div[@class='b-visualnav__title']/text() | .//p//text()").get('')
            url_sub_cat = f'{url}?_sop=10&mag=1&rt=nc&LH_Time=1&_ftrt=903&_ftrv=590&_salic=104&LH_LocatedIn=1'
            # print(url_sub_cat)
            yield scrapy.Request(url=url_sub_cat, headers=self.headers, callback=self.listing_page,
                                 meta={'url_sub_cat':url_sub_cat,'sub_cat_name':sub_cat_name,
                                       'catgeory_url':catgeory_url,
                                       'category_name':category_name})

    def listing_page(self, response):
        sub_cat_name=response.meta.get('sub_cat_name')
        url_sub_cat=response.meta.get('url_sub_cat')
        category_name=response.meta.get('category_name')
        catgeory_url=response.meta.get('catgeory_url')

        listing_div = response.xpath("//div[contains(@class,'s-item__info')] | //div[@class='brwrvr__item-card__body']//span[@class='bsig__title']")
        for each_div in listing_div[:]:
            detail_page_link= each_div.xpath("./a/@href").get('')
            listing_date= each_div.xpath('.//span[contains(@class,"s-item__listingDate")]/span/text()').get('')
            yield scrapy.Request(url=detail_page_link, headers=self.headers, callback=self.detail_page,
                                     meta={'url_sub_cat': url_sub_cat, 'sub_cat_name': sub_cat_name,
                                           'catgeory_url': catgeory_url,'category_name': category_name,
                                           'listing_date':listing_date,'detail_page_link':detail_page_link})


        next_page= response.xpath("//nav[@class='pagination']/a[@type='next']/@href").get('')
        if next_page:
            yield scrapy.Request(url=next_page, headers=self.headers,callback=self.listing_page,
                                 meta={'url_sub_cat': url_sub_cat, 'sub_cat_name': next_page,
                                       'catgeory_url': catgeory_url,
                                       'category_name': category_name,})

    def detail_page(self, response):
        item=dict()
        all_categories= " > ".join(response.xpath("//a[@class='seo-breadcrumb-text']/span/text()").getall()).strip()
        Watchers = response.xpath("//span[@class='x-watch-heart-btn-text']/text()").get('').strip()
        location = response.xpath("//span[contains(text(),'Located in')]/text()").get('')
        if 'Japan' in location and Watchers != '' and Watchers != '0':
            print("required", location)
            item['location'] = response.xpath("//span[contains(text(),'Located in')]/text()").get('').replace('Located in: ','')
            item['category_name']=response.meta.get('category_name','')
            item['sub_category_url']=response.meta.get('url_sub_cat','')
            item['sub_category_name']=response.meta.get('sub_cat_name','')
            item['all_categories']= " > ".join(response.xpath("//a[@class='seo-breadcrumb-text']/span/text()").getall()).strip()
            item['Title']= response.xpath("//h1[@class='x-item-title__mainTitle']/span/text()").get('').strip()
            item['Number of Watchers']= response.xpath("//span[@class='x-watch-heart-btn-text']/text()").get('').strip()
            item['Price']= response.xpath("//div[@class='x-price-primary']/span/text()").get('').strip()
            item['Condtion']= response.xpath("//span[contains(text(),'Condition:')]/following::div[1]//span[@class='clipped']/text()").get('').strip()
            item['Page_url']= response.meta.get('detail_page_link','')
            item['item_number']= str(response.xpath("//span[contains(text(),'eBay item number:')]/following-sibling::span/text()").get('').strip())
            item['item_number2']= int(response.xpath("//span[contains(text(),'eBay item number:')]/following-sibling::span/text()").get('').strip())

            item['Start date of listing']= response.meta.get('listing_date','')
            item['End date of listing']= "".join(response.xpath("//div[@class='x-end-time']//span//text()").getall()).strip()
            yield item
        else:
            print(location)

