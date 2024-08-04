import random
from time import sleep
from typing import Iterable
from scrapy.http import response
import scrapy
import scrapy.http
import scrapy.utils
import scrapy.utils.url
from project.items import CommunityItem
from scrapy import Spider
import pandas as pd
import json
from pathlib import Path
from dataclasses import dataclass

ROOT_DIR = Path(__file__).parent / ".." / ".."

@dataclass
class CommunityTarget:
    """小区目标类。
    为了便于后面获取数据，这个类可以通过字典创建。
    """
    name: str
    link: str
    district: str
    page_on_list: str
    undone: bool
    detail_link: str

class CommunityInfoSpider(Spider):
    name = "community_info"
    custom_settings = {
        "FEEDS":{
            "community_info.jsonl":{
                "format":"jsonl",
                "overwrite": False
            }
        }
    }
    regions: dict[str, str] = {}
    community_list: pd.DataFrame = pd.DataFrame()

    def get_url_house_detail(self, url: str):
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
            CommunityTarget: 下一个小区的信息
            None: 当没有下一个小区时，返回 None
        """
        undone = self.community_list.loc[self.community_list["undone"], :]
        # print("undone", undone.head(1))
        if undone.shape[0] > 0:
            community = undone.head(1).to_dict(orient="records")[0]
            # print("community dict", community)
            if community["district"].endswith("_old"):
                region_url = self.regions[community["district"]]
                community_link = community["link"]
                full_link = f"{region_url}{community_link}"
                community["detail_link"] = self.get_url_house_detail(full_link)
            elif community["district"].endswith("_new"):
                community["detail_link"] = self.get_url_house_detail(community["link"])
            return CommunityTarget(**community)
        else:
            print("注意：所有数据已爬取完毕")
            return None
  
    def start_requests(self):
        """爬虫启动准备
        """
        '''提取每个区 URL 的协议和域名，因为获取的小区链接中只有路径，没有协议和域名
        '''
        targets = pd.read_csv(ROOT_DIR / "targets.csv")
        for row in targets.itertuples():
            region_key = f"{row.region}_{row.type}"
            url_com = scrapy.utils.url.urlparse(row.url)  # 将 URL 解析称为不同部分，提取协议和域名
            region_url = f"{url_com.scheme}://{url_com.netloc}"
            self.regions[region_key] = region_url
        '''读取小区列表，设置一列表示是否已经爬取的信息，以便于记录进度
        '''
        self.community_list = pd.read_json(ROOT_DIR / "community_list.jsonl", lines=True).set_index("link", drop=False)
        if "undone" not in self.community_list.columns:
            self.community_list["undone"] = True
        '''读取已爬取数据
        '''
        community_info_file = ROOT_DIR / "community_info.jsonl"
        if community_info_file.exists():  # 如果文件存在，读取文件
            community_info = pd.read_json(community_info_file, lines=True)
            if community_info.shape[0] > 0:  # 如果文件有内容，将对应小区设置为已爬取
                self.community_list.loc[community_info["link"].tolist(), "undone"] = False
        '''获取下一个要爬取的小区
        '''
        next_community = self.find_next()
        if next_community is not None:
            yield scrapy.Request(url=next_community.detail_link, callback=self.parse, cb_kwargs={
                "community": next_community
            })
        

    def parse(self, response: scrapy.http.Response, community: CommunityTarget):
        """解析小区详情页面

        Args:
            response (scrapy.http.Response): HTTP 响应
            community (CommunityTarget): 小区其他信息
        
        Yields:
            CommunityItem: 小区数据
            scrapy.http.Request: 下一个请求
        """
        info_dict: dict[str, str] = {}
        if community.district.endswith("old"):
            village_info = response.css("div.village_info.base_info")
            for part in village_info:
                info = part.css("li")
                for item in info:
                    info_key: str = item.css("span::text").get()
                    info_key = info_key.replace(" ", "")
                    info_value: str = item.css("p::text").get()
                    if info_key in info_dict.keys():
                        info_key += "2"
                    info_dict[info_key] = info_value
        elif community.district.endswith("new"):
            pass
        yield CommunityItem(
            name=community.name.strip(),
            link=community.link,
            district=community.district.split("_")[0],
            info=info_dict
        )
        self.community_list.loc[community.link, "undone"] = False
        '''获取下一页链接
        '''
        next_community = self.find_next()
        if next_community is not None:
            sleep(1 + random.uniform(0, 1))
            yield response.follow(url=next_community.detail_link, callback=self.parse, cb_kwargs={
                "community": next_community
            })
