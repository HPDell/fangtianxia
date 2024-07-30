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

ROOT_DIR = Path(__file__).parent / ".." / ".."

class CommunityInfoSpider(Spider):
    name = "community_info"
    custom_settings = {
        "FEEDS":{
            "community_list.jsonl":{
                "format":"jsonl",
                "overwrite": False
            }
        }
    }
    regions: dict[str, str] = {}
    community_list: pd.DataFrame = pd.DataFrame()

    def get_url_house_detail(url: str):
        """获取小区详情的链接

        Args:
            url (str): 列表中爬取的小区链接

        Returns:
            _type_: 小区详情链接
        """
        url_parts = url.split("/")
        index_file = url_parts.pop()  # pop 函数将最后一项从列表中删除并返回
        cid, _ = index_file.split(".")  # cid 就是 community 编号
        url_parts.extend([cid, "housedetail.htm"])
        return "/".join(url_parts)  # 将 url 各个部分重新组合后返回

    def find_next(self):
        """查找下一个需要爬取的小区

        Returns:
            dict: 下一个小区的信息
            None: 当没有下一个小区时，返回 None
        """
        undone = self.community_list.where(self.community_list["undone"])
        if undone.shape[0] > 0:
            community = undone.head(1).to_dict(orient="records")[0]
            if community["district"].endswith("_old"):
                region_url = self.regions[community["district"]]
                community_link = community["link"]
                community["link"] = f"{region_url}{community_link}"
            community["detail_link"] = self.get_url_house_detail(community["link"])
            return community
        else:
            print("注意：所有数据已爬取完毕")
            return None
  
    def start_requests(self):
        """爬虫启动准备
        """
        ''' 提取每个区 URL 的协议和域名，因为获取的小区链接中只有路径，没有协议和域名
        '''
        targets = pd.read_csv(ROOT_DIR / "targets.csv")
        for row in targets.itertuples():
            region_key = f"{row.region}_{row.type}"
            url_com = scrapy.utils.url.urlparse(row.url)  # 将 URL 解析称为不同部分，提取协议和域名
            region_url = f"{url_com.scheme}://{url_com.netloc}"
            self.regions[region_key] = region_url
        ''' 读取小区列表，设置一列表示是否已经爬取的信息，以便于记录进度
        '''
        self.community_list = pd.read_json(ROOT_DIR / "community_list.jsonl", lines=True).reset_index()
        if "undone" not in self.community_list.columns:
            self.community_list["undone"] = True
        next_community = self.find_next()
        if next_community is not None:
            yield scrapy.Request(url=next_community["detail_link"], callback=self.parse, cb_kwargs={
                "community": next_community
            })
        

    def parse(self, response: scrapy.http.Response, community: dict):
        """解析小区详情页面

        Args:
            response (scrapy.http.Response): HTTP 响应
            community (dict): 小区其他信息
        """
        
