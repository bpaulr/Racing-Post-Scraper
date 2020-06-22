from datetime import date

import scrapy
from racingpost_scraper.items import RaceItem


class RaceSpider(scrapy.Spider):
    name = "race-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.RaceCardItemPipeline': 300,
        }
    }

    allowed_domains = ["www.racingpost.com"]

    start_urls = ["https://www.racingpost.com/racecards/time-order"]

    def parse(self, response):
        races = response.xpath('//main/section/div[@class="RC-meetingList RC-meetingList_byTime"]/div')
        for card in races:
            rel_url = card.xpath('./a/@href').get()
            abs_url = response.urljoin(rel_url)

            time = card.xpath('./a/div[@class="RC-meetingItem__time"]/span/text()').get()
            course_box = card.xpath('./a/div[@class="RC-meetingItem__content RC-meetingItem__content_time"]/div[@class="RC-meetingItem__wrapper"]')
            course_data = course_box.xpath('./div[@class="RC-meetingItem__head"]')
            course_name = course_data.xpath('./span[@class="RC-meetingItem__title"]/text()').get().strip()
            track_type = course_data.xpath('./span[@class="RC-meetingItem__subText"]/text()').get().strip()
            title = course_box.xpath('./div[@class="RC-meetingItem__body"]/div[@class="RC-meetingItem__section_n"]/span[@class="RC-meetingItem__info"]/text()').get().strip()

            race = RaceItem()
            race["title"] = title
            race["course_name"] = course_name
            race["track_type"] = track_type
            race["race_date"] = date.today().strftime("%Y-%m-%d")
            race["race_time"] = time
            race["url"] = abs_url

            yield race
