from dadata import Dadata
from datetime import datetime
import pandas as pd


with open('C:\\!Равшан_работа\\OMT Consult\\Карты\\inn.txt', 'r',  encoding='utf-8-sig') as w:
    a = []
    for line in w:
        a.append(line.strip('\n'))

token = "95f4180a67114d0806e5b9afe92998e051eb8831"
dadata = Dadata(token)
d = {}

for i in a:
    result = dadata.find_by_id("party", query=i)
    d[i] = {}
    try:
        d[i]['name'] = result[0]['value']
    except Exception:
        d[i]['name'] = ''
    try:
        d[i]['type'] = result[0]['data']['type']
    except Exception:
        d[i]['type'] = ''
    try:
        d[i]['address'] = result[0]['data']['address']['unrestricted_value']
    except Exception:
        d[i]['address'] = ''
    try:
        d[i]['phone'] = result[0]['data']['phones']
    except Exception:
        d[i]['phone'] = ''
    try:
        d[i]['email'] = result[0]['data']['emails']
    except Exception:
        d[i]['email'] = ''
    if d[i]['type'] == 'INDIVIDUAL':
        try:
            d[i]['full_name_director'] = str(result[0]['data']['fio']['surname'] + ' ' + result[0]['data']['fio']['name']\
                                         + ' ' + result[0]['data']['fio']['patronymic'])
        except Exception:
            d[i]['full_name_director'] = ''
    elif d[i]['type'] == 'LEGAL':
        try:
            d[i]['full_name_director'] = result[0]['data']['management']['name']
        except Exception:
            d[i]['full_name_director'] = ''
    try:
        d[i]['status_itn'] = result[0]['data']['state']['status']
    except Exception:
        d[i]['status_itn'] = ''
    try:
        d[i]['updated_date'] = datetime.utcfromtimestamp(int(str(result[0]['data']['state']['actuality_date'])[:-3])).\
                                strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        d[i]['updated_date'] = ''
    try:
        d[i]['okved'] = result[0]['data']['okved']
    except Exception:
        d[i]['okved'] = ''

# with open('C:\\!Равшан_работа\\OMT Consult\\Карты\\data_inn.json', 'w', encoding='utf-8-sig') as f:
#     json.dump(d, f, ensure_ascii=False, indent=4)
df = pd.DataFrame.from_dict(d, orient='index')
dict_status = {
    'ACTIVE': 'действующая',
    'LIQUIDATING': 'ликвидируется',
    'LIQUIDATED': 'ликвидирована',
    'BANKRUPT': 'банкротство',
    'REORGANIZING': 'В процессе присоединения'
}

dict_opf = {
    'LEGAL': 'ЮЛ',
    'INDIVIDUAL': 'ИП'
}
df.type = df[df['type'] != ''].type.map(lambda x: dict_opf[x])
df.status_itn = df[df['status_itn'] != ''].status_itn.map(lambda x: dict_status[x])

df.reset_index(level=0, inplace=True)
df.rename(columns={'index': 'itn'}, inplace=True)
df.to_excel('C:\\!Равшан_работа\\OMT Consult\\Карты\\data_inn_112.xlsx', encoding='utf-8-sig', index=False)