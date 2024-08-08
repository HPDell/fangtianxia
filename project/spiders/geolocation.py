from typing import Any, Iterable
from scrapy import Spider, Request
import pandas as pd
from scrapy.http import Request, TextResponse
from urllib.parse import urlencode
from pathlib import Path
from dataclasses import dataclass
from time import sleep

ROOT_DIR = Path(__file__).parent / ".." / ".."

@dataclass
class CommunityTarget:
    index: int
    uuid: str
    name: str
    address: str
    waiting: bool
    lon: None | float = None
    lat: None | float = None


class CommunityGeoLocator(Spider):
    name = "community_geolocator"
    custom_settings = {
        "FEEDS":{
            "community_geolocation.jsonl":{
                "format":"jsonl",
                "overwrite": False
            }
        }
    }

    base_url: str = "https://restapi.amap.com/v3/geocode/geo"
    key: str = ""
    communities: pd.DataFrame = pd.DataFrame()

    def get_url(self, target: CommunityTarget):
        params = {
            "key": self.key,
            "city": "郑州",
            "address": target.address
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{self.base_url}?{query_string}"

    def find_next(self):
        waiting = self.communities.loc[self.communities["waiting"] == True, :]
        if waiting.shape[0] > 0:
            for irow in range(waiting.shape[0]):
                info = waiting.iloc[irow, :].to_dict()
                if len(info["address"]) > 4:
                    item = CommunityTarget(**info)
                    return (self.get_url(item), item)
                else:
                    self.logger.error("地址太短，无法解析: %s", info["name"])
                    self.communities.loc[info["index"], "waiting"] = False
        else:
            self.logger.info("所有地址已编码完毕")
            return (None, None)

    def start_requests(self) -> Iterable[Request]:
        if (ROOT_DIR / "key.txt").exists():
            self.key = (ROOT_DIR / "key.txt").read_text().strip()
        else:
            self.logger.error("无法读取秘钥")
            return

        if (ROOT_DIR / "community.csv").exists():
            self.communities = pd.read_csv(ROOT_DIR / "community.csv", index_col=0).reset_index(drop=False)
            self.communities["waiting"] = True
        else:
            self.logger.error("无法读取小区数据")
            return
        
        if (ROOT_DIR / "community_geolocation.jsonl").exists():
            finished = pd.read_json(ROOT_DIR / "community_geolocation.jsonl", lines=True)
            self.communities.loc[finished.index, "waiting"] = False
        
        target, community = self.find_next()
        if target is not None:
            yield Request(
                url=target,
                callback=self.parse,
                cb_kwargs={"community": community}
            )
    
    def parse(self, response: TextResponse, community: CommunityTarget) -> Any:
        data = response.json()
        if data["status"] == "1":
            count = int(data["count"])
            geocodes = data["geocodes"]
            if count > 0:
                location: str = geocodes[0]["location"]
                lon, lat = location.split(",")
                community.lon = float(lon)
                community.lat = float(lat)
                yield community
            else:
                self.logger.error("没有编码结果")
        else:
            error_info = data["info"]
            if error_info == "QUOTA_PLAN_RUN_OUT":
                self.logger.error("余额耗尽")
                return
            elif error_info in ["ABROAD_DAILY_QUERY_OVER_LIMIT", "CQPS_HAS_EXCEEDED_THE_LIMIT", "CKQPS_HAS_EXCEEDED_THE_LIMIT", "CUQPS_HAS_EXCEEDED_THE_LIMIT"]:
                self.logger.error("QPS超出限制")
                return
            elif error_info == "DAILY_QUERY_OVER_LIMIT":
                self.logger.error("访问已超出日访问量")
                return
            elif error_info == "ACCESS_TOO_FREQUENT":
                self.logger.error("单位时间内访问过于频繁")
                return
            else:
                self.logger.error("服务器返回错误: %s", error_info)
        '''Next
        '''
        self.communities.loc[community.index, "waiting"] = False
        next_target, next_community = self.find_next()
        if next_target is not None:
            yield response.follow(
                url=next_target,
                callback=self.parse,
                cb_kwargs={"community": next_community}
            )
