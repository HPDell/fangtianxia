# Define here the models for your scraped items
# 定义“小区类”
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CommunityItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # name小区名，link网页地址，type新房/旧房（这个在网页上没有显示，需要通过builtyear来加工），
    # district行政区，address位置坐标，builtyear建成年份，carpark_type停车场类型, carpark_nb停车位数量
    name = scrapy.Field()
    link = scrapy.Field()
    type = scrapy.Field()
    district = scrapy.Field()
    address = scrapy.Field()
    builtyear = scrapy.Field()
    carpark_type = scrapy.Field()
    carpark_nb = scrapy.Field()
    page_on_list = scrapy.Field()
    
    pass
