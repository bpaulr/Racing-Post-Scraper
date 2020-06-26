import scrapy

from racingpost_scraper.items import RaceRunnerItem


class RaceCardSpider(scrapy.Spider):
    name = "racecard-spider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'racingpost_scraper.pipelines.RaceRunnerItemPipeline': 300,
        }
    }

    start_urls = ["https://www.racingpost.com/racecards/257/santa-anita/2020-06-22/760716/"]

    def parse(self, response):
        runners = response.xpath('//main/section/div[@class="RC-runnerRowWrapper js-RC-runnerRowWrapper RC-runnerRowWrapper_clean"]/div')

        # whitespace and newlines matter in class names (blame the webdevs, not me)
        runner_table_class = """RC-runnerRowWrapper
            js-RC-runnerRowWrapper
                            RC-runnerRowWrapper_clean
            """
        runner_row_class = """RC-runnerRow
        js-RC-runnerRow
        js-PC-runnerRow
        """
        runners = response.xpath(f'//main/section/div[@class="{runner_table_class}"]/div[@class="{runner_row_class}"]')

        for runner in runners:
            runner_item = RaceRunnerItem()
            card = runner.xpath('./div[@class="RC-runnerCardWrapper"]')
            runner_item["no_form"] = card.xpath('./div[@class="RC-runnerRowHorseWrapper"]/div[@class="RC-runnerNumber"]/span[1]/text()').get().strip()
            runner_item["no_form"] += card.xpath('./div[@class="RC-runnerRowHorseWrapper"]/div[@class="RC-runnerNumber"]/span[2]/text()').get().strip()

            horse = card.xpath('./div[@class="RC-runnerRowHorseWrapper"]/div[@class="RC-runnerMainWrapper"]')
            runner_item["horse"] = horse.xpath('./a/text()').get().strip()
            runner_item["horse_url"] = response.urljoin(horse.xpath('./a/@href').get().strip())

            info = card.xpath('./div[@class="RC-runnerRowInfoWrapper"]')
            runner_item["age"] = info.xpath('./div[@class="RC-runnerInfo"]/span[2]/text()').get().strip()

            wgt = info.xpath('./div[@class="RC-runnerWgtorWrapper"]/div[@class="RC-runnerWgt"]/span[@class="RC-runnerWgt__carried"]')
            runner_item["wgt"] = wgt.xpath('./span[@class="RC-runnerWgt__carried_st"]/text()').get().strip()
            runner_item["wgt"] += "-"
            runner_item["wgt"] += wgt.xpath('./span[@class="RC-runnerWgt__carried_lb"]/text()').get().strip()

            runner_item["_or"] = info.xpath('./div[@class="RC-runnerWgtorWrapper"]/span[@class="RC-runnerOr"]/text()').get().strip()

            jockey_trainer = info.xpath('./div[@class="RC-runnerInfoWrapper"]')
            runner_item["jockey"] = jockey_trainer.xpath('./div[@class="RC-runnerInfo RC-runnerInfo_jockey"]/a/text()').get(default="").strip()
            runner_item["jockey_url"] = response.urljoin(jockey_trainer.xpath('./div[@class="RC-runnerInfo RC-runnerInfo_jockey"]/a/@href').get(default="").strip())

            runner_item["trainer"] = jockey_trainer.xpath('./div[@class="RC-runnerInfo RC-runnerInfo_trainer"]/a/text()').get(default="").strip()
            runner_item["trainer_url"] = response.urljoin(jockey_trainer.xpath('./div[@class="RC-runnerInfo RC-runnerInfo_trainer"]/a/@href').get(default="").strip())

            yield runner_item
