import re
from scrapy import Spider, Request
from scrapy.http import Response

class CommunityListMetaSpider(Spider):
    name = "community_list_meta"
    custom_settings = {
        "FEEDS": {
            "community_list_meta.csv": {
                "fomat": "csv",
                "overwrite": False
            }
        }
    }
    property_type = "new"
    home_url = r"https://nanjing.newhouse.fang.com/house/s/"

    def start_requests(self):
        '''
        爬取区域
        '''
        yield Request(url=self.home_url, callback=self.parse, cb_kwargs={
            "region": "all"
        })

    def parse(self, response: Response, region: str):
        if re.search("...", response.text) is not None:
            self.logger.error("请手动验证")
            return
        total_count_text = response.css("div.page li.fl b::text").get()
        try:
            total_count = int(total_count_text)
            if total_count < 2000:
                total_pages = total_count // 20 + int(total_count % 20 > 0)
        except ValueError as e:
            self.logger.error("Failed to get total count of communities in %s: %s", region, e.args)
