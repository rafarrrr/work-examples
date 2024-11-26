from dadata import Dadata
from datetime import datetime
import pandas as pd
from tqdm import tqdm


def safe_get(data, *keys, default=""):
    """
    Безопасное получение вложенного значения из словаря.
    :param data: словарь или список
    :param keys: путь к значению
    :param default: значение по умолчанию
    """
    for key in keys:
        try:
            data = data[key]
        except (KeyError, TypeError, IndexError):
            return default
    return data


with open('C:\\!Равшан_работа\\OMT Consult\\Карты\\inn.txt', 'r',  encoding='utf-8') as w:
    list_inn = []
    for line in w:
        list_inn.append(line.strip('\n'))

token = "**********"
dadata = Dadata(token)
dict_inn = {}

progress_bar = tqdm(desc="status inn parse", total=len(list_inn), leave=False)
for i in list_inn:
    result = dadata.find_by_id("party", query=i)
    dict_inn[i] = {
        'name': safe_get(result, 0, 'value'),
        'type': safe_get(result, 0, 'data', 'type'),
        'address': safe_get(result, 0, 'data', 'address', 'unrestricted_value'),
        'phone': safe_get(result, 0, 'data', 'phones'),
        'email': safe_get(result, 0, 'data', 'emails'),
        'status_itn': safe_get(result, 0, 'data', 'state', 'status'),
        'updated_date': safe_get(
            result, 0, 'data', 'state', 'actuality_date',
            default=None
        ),
        'okved': safe_get(result, 0, 'data', 'okved'),
        'kpp': safe_get(result, 0, 'data', 'kpp'),
    }

    if dict_inn[i]['updated_date']:
        try:
            dict_inn[i]['updated_date'] = datetime.utcfromtimestamp(
                int(str(dict_inn[i]['updated_date'])[:-3])
            ).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            dict_inn[i]['updated_date'] = ''

    if dict_inn[i]['type'] == 'INDIVIDUAL':
        dict_inn[i]['full_name_director'] = f"{safe_get(result, 0, 'data', 'fio', 'surname')} " \
                                     f"{safe_get(result, 0, 'data', 'fio', 'name')} " \
                                     f"{safe_get(result, 0, 'data', 'fio', 'patronymic')}".strip()
    elif dict_inn[i]['type'] == 'LEGAL':
        dict_inn[i]['full_name_director'] = safe_get(result, 0, 'data', 'management', 'name')
    else:
        dict_inn[i]['full_name_director'] = ''

    progress_bar.update(1)

progress_bar.close()
dadata.close()

df = pd.DataFrame.from_dict(dict_inn, orient='index')
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

now = datetime.now().strftime("%Y-%m-%d_%H%M%S")
df.reset_index(level=0, inplace=True)
df.rename(columns={'index': 'itn'}, inplace=True)
df.to_excel(f'C:\\!Равшан_работа\\OMT Consult\\Карты\\data_inn_data_{now}.xlsx', index=False)
