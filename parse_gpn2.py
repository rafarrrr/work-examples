import requests as re
import pandas as pd
import json


cookies = {
    'session-cookie': '174fa74dbf710bf8f2fd458004983c47d749febd7153f435e8a9bda1a714ee29100b3895e3dd13d8bc52adaa6829c73f',
    'csrf-token-name': 'csrftoken',
    'csrf-token-value': '174fa86673a2a3f1e839a5a55c579bafea6719238d4d827f64a86183d7ecd680d1313272f98e557e',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://gpnbonus.ru/fuel/refuel-map',
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type': 'application/json;charset=utf-8',
    'X-csrftoken': '174fa8663b3612d02115e994199b7b698cc1a30d10a1621a4e015a64c8241881cff87ed6b90b918a',
    'X-Ajax-Token': '95b2c100517289cab973e6e7e9bc1fd9f152a94df0a7c5309996efb7cbf93df0',
    'Origin': 'https://gpnbonus.ru',
    'Connection': 'keep-alive',
    # 'Cookie': 'session-cookie=174fa74dbf710bf8f2fd458004983c47d749febd7153f435e8a9bda1a714ee29100b3895e3dd13d8bc52adaa6829c73f; csrf-token-name=csrftoken; csrf-token-value=174fa86673a2a3f1e839a5a55c579bafea6719238d4d827f64a86183d7ecd680d1313272f98e557e',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

json_data = {
    'open': False,
    'wash': False,
    'AZSShopTypeID': False,
    'services': {
        'car': {},
        'payment': {},
        'person': {},
        'station': {},
    },
}

response = re.post('https://gpnbonus.ru/api/stations/list', cookies=cookies, headers=headers, json=json_data)

# with open('result1.html', 'r') as file:
# 	content = file.read()

x = json.loads(response.text)
df = pd.DataFrame(x['stations'])

# Выделяем в отдельный ДФ виды топлива и создаём словарь для замены
df.loc[:, 'oils1'] = df['oils'].apply(lambda t: {y: x for x, y in zip(t, [i["id"] for i in t])})
df1 = df['oils1'].apply(pd.Series).reset_index().rename(columns={'index':'id_1', 12:'аи95', 62:'аи92', 421:'G95',
                                                                 461:'ДТ межсезонное ОПТИ', 374:'ДТ З Опти', 373:'Газ',
                                                                 100032:'G100', 372:'ДТ Л Опти', 512:'ДТ', 541:'ДТ ОПТИ',
                                                                 423:'G-ДТ З', 531:'КПГ', 21:'аи98', 511:'G-ДТ межсез.',
                                                                 100036:'аи100', 100034:'95 ОПТИ'})

for i in df1.columns[1::]:
    df3 = df1.loc[:, i].apply(pd.Series).reset_index().price.apply(pd.Series)
    df1[i] = df1.merge(df3['price'], left_on='id_1', right_on=df.index)['price']

df2 = df1.set_index('id_1').stack().reset_index()
result_dict = df2.groupby('id_1').apply(lambda x: x.set_index('level_1')[0].to_dict()).to_dict()

# Создаём новую колонку для цен с топливом и заменяем значениями из словаря
df['fuel_price'] = df.reset_index()['index']
df['fuel_price'] = df['fuel_price'].map(result_dict)
# Разворачиваем словарь с сервисами и добавляем в отдельные колонки
df5 = df.loc[:, 'services'].apply(pd.Series)
df4 = df.merge(df5, right_index=True, left_index=True)
brand = {31: "ГПН", 50000005: "ОПТИ"}
df4['brand'] = df4['brand_id'].replace(brand)
df4.drop(['services', 'oils', 'oils1', 'brand_id', 'rate', 'rateCount', 'promotions', 'workModeMessages', 'region_id',
          'PNPONumber', 'GPNAZSID'], axis=1, inplace=True)
df4.to_excel('gpn_azs_parce.xlsx', index=False, encoding='utf-8')