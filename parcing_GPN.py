import requests as re
from bs4 import BeautifulSoup
import time
import pandas as pd


url = 'https://www.gpnbonus.ru/our_azs/?region_id=all&region_name=%D0%9F%D0%BE%D0%BA%D0%B0%D0%B7%D0%B0%D1%82%D1%8C+%D0%90%D0%97%D0%A1+%D0%B2%D1%81%D0%B5%D1%85+%D1%80%D0%B5%D0%B3%D0%B8%D0%BE%D0%BD%D0%BE%D0%B2&CenterLon=56.225679&CenterLat=59.117698&city='
headers = {'Accept': '*/*;q=0.8',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0) Gecko/20100101 Firefox/87.0'}


def get_html(url, params=None):
    r = re.get(url, params=params, headers=headers)
    l = r.content.decode('utf-8')
    return r


def get_content(html, region):
    soup = BeautifulSoup(html, 'lxml')
    items = soup.find_all('div', class_='oh pt10')
    azs = []
    for item in items:
        nobrs = item.find('div', class_='pt10').find_next_sibling().find_next_sibling()
        go = nobrs.find('nobr').find_all('div', class_='DinPro')
        fuel = []
        fuel.clear()
        for i in go:
            fuel.append(i.get_text())
        azs.append({
            'region': region,
            'address': item.find('div', class_='DinProMedium').get_text().strip(),
            'number': item.find('div', class_='inline-block').get_text().strip(),
            'GPS': item.find('nobr',).get_text().split('\n')[2].strip(),
            'fuel': fuel,
            'Services': item.find('span', class_='inline-block fs12 serviceText').get_text().strip()
        })
    return azs


def get_region(html):
    soup = BeautifulSoup(html, 'lxml')
    regions = soup.find('select', class_='select').get_text().split('\n')
    value = soup.findAll('option', )
    reg_id = []
    for x in value:
        if x.get('filterid') != '222362':
            reg_id.append(x.get('filterid'))
        else:
            break
    reg = []
    for region in regions:
        region = region.replace('/', '%2F')
        region = region.replace(' ', '+')
        reg.append(region)
    reg = reg[2:len(reg)-5]
    reg_id = reg_id[1:len(reg_id)]
    # reg = reg[::-1]
    return reg, reg_id


def parse():
    html = get_html(url)
    if html.status_code == 200:
        regions_gpn, id = get_region(html.text)
        gpn_azs = []

        for j, region in enumerate(regions_gpn):
            html1 = get_html(url, params={'region_id': id[j], 'region_name': region, "latlon":
                'CenterLon=35.445185&CenterLat=54.371800&city='})
            text1 = html1.text
            if html1.status_code == 200:
                gpn_azs.extend(get_content(text1, region))
                time.sleep(2)
            else:
                print('error')
                break
    return gpn_azs


gpn_parce = parse()
df = pd.DataFrame(gpn_parce)
df.to_excel('gpn_azs_parce.xlsx', index=False, encoding='utf-8')