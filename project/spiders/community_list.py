import random
from time import sleep
from typing import Iterable
from scrapy.http import response
import scrapy
import scrapy.utils
import scrapy.utils.url
from project.items import CommunityItem
from scrapy import Spider
import pandas as pd
import json
from pathlib import Path

class CommunityListSpider(Spider):
    name = "community_list"
    custom_settings = {
        "FEEDS":{
            "community_list.jsonl":{
                "format":"jsonl",
                "overwrite": False
            }
        }
    }
    progress: dict[str, int] = {}
    targets: None | pd.DataFrame = None

    def save_progress(self):
        Path("progress.json").write_text(json.dumps(self.progress, ensure_ascii=False, indent=2), encoding="UTF-8")

    def find_next_target(self):
        for target in self.targets.itertuples():
            region_name = target[1]
            region_url = target[2]
            region_type = target[6]
            region_key = f"{region_name}_{region_type}"
            region_pages = target[4]
            if region_key in self.progress.keys():
                region_progress = self.progress[region_key]
                if region_progress["page"] >= region_pages:
                    print(f"注意： {region_key} 已经爬取完毕")
                else:
                    self.progress[region_key]["page"] += 1
                    next_path = region_progress["next"]
                    base_url = scrapy.utils.url.urlparse(region_url)
                    url = f"{base_url.scheme}://{base_url.netloc}{next_path}"
                    if url is None:
                        return (region_key, region_url)
                    else:
                        return (region_key, url)
            else:
                self.progress[region_key] = {
                    "page": 1,
                    "next": None
                }
                return (region_key, region_url)
        return None
  
    def start_requests(self):
        '''
        爬取所有目标网址
        '''
        progress_file = Path("progress.json")
        if progress_file.exists():
            self.progress = json.loads(progress_file.read_text(encoding="UTF-8"))
        self.targets = pd.read_csv(r"/Users/yigong/Code/test/fangtianxia/targets.csv")  # r后面是文件的绝对路径，右键copy path获取路径
        next_target = self.find_next_target()
        if next_target:
            region_key, url = next_target
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={
                "region_key": region_key
            })
        else:
            print(f"注意：所有区域已爬取完毕")

    def parse(self, response: scrapy.http.Response, region_key: str):
        _, region_type = region_key.split("_")
        if region_type == "old":
            house_list = response.css("div.houseList a.plotTit")
        elif region_type == "new":
            house_list = response.css("div.nhouse_list div.nlcd_name a")
        else:
            house_list = []
        if len(house_list) > 0:
            ''' 如果能获取到列表，表示正常情况，可以继续获取数据。
            '''
            for item in house_list:
                yield CommunityItem(
                    name=item.css("::text").get(),
                    link=item.css("::attr(href)").get(),
                    district=region_key,
                    page_on_list=self.progress[region_key]["page"]
                )
            ''' 获取下一页的链接
            '''
            next_page_link = None
            if region_type == "old":
                pagers: list = response.css("div.fanye a")
            else:
                pagers: list = response.css("div.page li.fr a")
            next_page = [p for p in pagers if len(p.re("下一页")) > 0]
            if len(next_page) > 0:
                next_page_link = next_page[0].css("::attr(href)").get()
            ''' 保存进度
            '''
            self.progress[region_key]["next"] = next_page_link
            self.save_progress()
            self.progress[region_key]["page"] = self.progress[region_key]["page"] + 1
            ''' 进入下一页
            '''
            sleep(1 + random.uniform(0, 1))
            if next_page_link is not None:
                yield response.follow(next_page_link, callback=self.parse, cb_kwargs={
                    "region_key": region_key
                })
            else:
                next_target = self.find_next_target()
                if next_target:
                    region_key, url = next_target
                    yield response.follow(url=url, callback=self.parse, cb_kwargs={
                        "region_key": region_key
                    })
                else:
                    print(f"注意：所有区域已爬取完毕")
        else:
            '''如果没有获取到小区列表，表明出现错误，不保存进度
            '''
            print(f"注意：爬虫中断")
