import scrapy
from eruz.items import EruzItem
import time, random, datetime

start_date = (datetime.date.today() - datetime.timedelta(days=1))
end_date = (datetime.date.today() - datetime.timedelta(days=1))


def generate_dates(start_date, end_date):
    return (start_date + datetime.timedelta(days=d) for d in range((end_date - start_date).days + 1))


def getvalue(tag):
    value = ''
    try:
        value = tag.replace('\n', '').replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '') \
            .replace('  ', '').replace(';', ',')
    except:
        value = ''
    return value


class EruzSpider(scrapy.Spider):
    name = "eruz"

    def start_requests(self):
        urls = [
            'http://zakupki.gov.ru/epz/eruz/extendedsearch/results.html?morphology=on&sortBy=BY_REGISTRY_DATE&participantType_0=on&participantType_1=on&participantType_2=on&participantType_3=on&participantType_4=on&participantType_5=on&participantType_6=on&participantType_7=on&participantType=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7&registered=on&registryDateFrom=' + current_date.strftime(
                '%d.%m.%Y') + '&registryDateTo=' + current_date.strftime(
                '%d.%m.%Y') + '&pageNumber=1&sortDirection=false&recordsPerPage=_500'
            for current_date in generate_dates(start_date, end_date)]
        urls.extend(
            [
                'http://zakupki.gov.ru/epz/eruz/extendedsearch/results.html?morphology=on&sortBy=BY_REGISTRY_DATE&participantType_0=on&participantType_1=on&participantType_2=on&participantType_3=on&participantType_4=on&participantType_5=on&participantType_6=on&participantType_7=on&participantType=0%2C1%2C2%2C3%2C4%2C5%2C6%2C7&registered=on&registryDateFrom=' + current_date.strftime(
                    '%d.%m.%Y') + '&registryDateTo=' + current_date.strftime(
                    '%d.%m.%Y') + '&pageNumber=2&sortDirection=false&recordsPerPage=_500'
                for current_date in generate_dates(start_date, end_date)])
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        links = list(set(response.xpath(
            '//a[contains(@href, "/epz/eruz/card/general-information.html?reestrNumber=")]/@href').extract()))
        for link in links:
            url = 'http://zakupki.gov.ru' + link
            print(url)
            yield response.follow(url, callback=self.parse_link)

    def parse_link(self, response):
        item = EruzItem()
        item['url'] = response.url
        elements = response.xpath('//td/text()').extract()
        for i in range(len(elements)):
            if elements[i] == 'Номер реестровой записи в ЕРУЗ':
                item['id'] = getvalue(elements[i + 1])
            if elements[i] == 'Статус регистрации':
                item['status'] = getvalue(elements[i + 1])
            if elements[i] == 'Тип участника закупки':
                item['type'] = getvalue(elements[i + 1])
            if elements[i] == 'Дата регистрации в ЕИС':
                item['regDt'] = getvalue(elements[i + 1])
            if elements[i] == 'Дата окончания срока регистрации в ЕИС':
                item['regDtEnd'] = getvalue(elements[i + 1])
            if elements[i] == 'Полное наименование':
                item['FullName'] = getvalue(elements[i + 1])
            if elements[i] == 'Сокращенное наименование':
                item['ShortName'] = getvalue(elements[i + 1])
            if elements[i] == 'Адрес в пределах места нахождения':
                item['Address'] = getvalue(elements[i + 1])
            if elements[i] == 'ИНН':
                item['INN'] = getvalue(elements[i + 1])
            if elements[i] == 'КПП':
                item['KPP'] = getvalue(elements[i + 1])
            if elements[i] == 'ОГРН':
                item['OGRN'] = getvalue(elements[i + 1])
            if elements[i] == 'Почтовый адрес':
                item['PostalAddress'] = getvalue(elements[i + 1])
            if elements[i] == 'Адрес электронной почты':
                item['Email'] = getvalue(elements[i + 1])
            if elements[i] == 'Телефон':
                item['Phone'] = getvalue(elements[i + 1])
        yield item
