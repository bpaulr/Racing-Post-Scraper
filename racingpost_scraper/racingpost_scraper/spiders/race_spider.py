from datetime import date
from datetime import timedelta

import scrapy
from racingpost_scraper.items import RaceItem

# date syntax is `YYYY-MM-DD`
DATE_URL = "https://www.racingpost.com/racecards/{}/time-order"


class RaceSpider(scrapy.Spider):
    name = "race-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.RaceCardItemPipeline': 300,
        }
    }

    allowed_domains = ["www.racingpost.com"]

    start_urls = ["https://www.racingpost.com/racecards/time-order"]

    def start_requests(self):
        url = self.start_urls[0]
        race_date = date.today()
        yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'race_date': race_date.strftime("%Y-%m-%d")})
        for i in range(6):
            new_date = (race_date + timedelta(days=i + 1)).strftime("%Y-%m-%d")
            new_url = DATE_URL.format(new_date)
            yield scrapy.Request(url=new_url, callback=self.parse, cb_kwargs={'race_date': new_date})

    def parse(self, response, race_date):
        races = response.xpath('//main/section/div[@class="RC-meetingList RC-meetingList_byTime"]/div')
        for card in races:
            rel_url = card.xpath('./a/@href').get(default="")
            # there is no point in looking at race if a race page has not been set up yet
            if rel_url is None:
                return None
            abs_url = response.urljoin(rel_url)

            time = card.xpath('./a/div[@class="RC-meetingItem__time"]/span/text()').get(default="")
            course_box = card.xpath('./a/div[@class="RC-meetingItem__content RC-meetingItem__content_time"]/div[@class="RC-meetingItem__wrapper"]')
            course_data = course_box.xpath('./div[@class="RC-meetingItem__head"]')
            course_name = course_data.xpath('./span[@class="RC-meetingItem__title"]/text()').get(default="").strip()
            track_type = course_data.xpath('./span[@class="RC-meetingItem__subText"]/text()').get(default="").strip()
            title = course_box.xpath('./div[@class="RC-meetingItem__body"]/div[@class="RC-meetingItem__section_n"]/span[@class="RC-meetingItem__info"]/text()').get(default="").strip()

            race = RaceItem()
            race["title"] = title
            race["course_name"] = course_name
            race["track_type"] = track_type
            race["race_date"] = race_date
            race["race_time"] = time
            race["url"] = abs_url

            yield race
