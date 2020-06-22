import scrapy
from datetime import date

from racingpost_scraper.items import RaceCardItem


class RaceCardSpider(scrapy.Spider):
    name = "racecard-spider"

    allowed_domains = ["www.racingpost.com"]

    start_urls = ["https://www.racingpost.com/racecards/time-order"]

    def parse(self, response):
        racecards = response.xpath('//main/section/div[@class="RC-meetingList RC-meetingList_byTime"]/div')
        for card in racecards:
            rel_url = card.xpath('./a/@href').get()
            abs_url = response.urljoin(rel_url)

            time = card.xpath('./a/div[@class="RC-meetingItem__time"]/span/text()').get()
            course_box = card.xpath('./a/div[@class="RC-meetingItem__content RC-meetingItem__content_time"]/div[@class="RC-meetingItem__wrapper"]')
            course_data = course_box.xpath('./div[@class="RC-meetingItem__head"]')
            course_name = course_data.xpath('./span[@class="RC-meetingItem__title"]/text()').get().strip()
            track_type = course_data.xpath('./span[@class="RC-meetingItem__subText"]/text()').get().strip()
            title = course_box.xpath('./div[@class="RC-meetingItem__body"]/div[@class="RC-meetingItem__section_n"]/span[@class="RC-meetingItem__info"]/text()').get().strip()

            racecard = RaceCardItem()
            racecard["title"] = title
            racecard["course_name"] = course_name
            racecard["track_type"] = track_type
            racecard["race_date"] = date.today().strftime("%Y-%m-%d")
            racecard["race_time"] = time
            racecard["url"] = abs_url

            yield racecard