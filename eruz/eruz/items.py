# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class EruzItem(scrapy.Item):
    # define the fields for your item here like:
    url = scrapy.Field()
    id = scrapy.Field()
    status = scrapy.Field()
    type = scrapy.Field()
    regDt = scrapy.Field()
    regDtEnd = scrapy.Field()
    FullName = scrapy.Field()
    ShortName = scrapy.Field()
    Address = scrapy.Field()
    INN = scrapy.Field()
    KPP = scrapy.Field()
    OGRN = scrapy.Field()
    FIO = scrapy.Field()
    position = scrapy.Field()
    f_inn = scrapy.Field()
    PostalAddress = scrapy.Field()
    Email = scrapy.Field()
    Phone = scrapy.Field()
    # pass
