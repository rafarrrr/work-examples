import json
from datetime import timedelta, datetime
import pandas as pd
# from airflow.decorators import task
from airflow import DAG
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': datetime(2024, 11, 10),
    "retries": 2,
    'retry_delay': timedelta(seconds=30),
}

check_dic = {2: {'max': 97, 'min': 46.55}, 3: {'max': 97, 'min': 40.05}, 4: {'max': 97, 'min': 50.5},
             41: {'max': 97, 'min': 50.5}, 42: {'max': 85, 'min': 33}, 173: {'max': 97, 'min': 50.5},
             174: {'max': 97, 'min': 46.55}, 175: {'max': 97, 'min': 40.5}, 176: {'max': 110, 'min': 46},
             139: {'max': 95, 'min': 40.1}, 177: {'max': 110, 'min': 46}, 184: {'max': 110, 'min': 46},
             178: {'max': 40, 'min': 10}, 179: {'max': 45, 'min': 11}, 181: {'max': 110, 'min': 46.1},
             182: {'max': 110, 'min': 46.1}, 183: {'max': 110, 'min': 46.1}, 189: {'max': 75, 'min': 45.1},
             204: {'max': 70, 'min': 45.1}, 133: {'max': 70, 'min': 45.1}, 156: {'max': 60, 'min': 45.1},
             308: {'max': 50, 'min': 25}, 332: {'max': 80, 'min': 20},
             }
dict_fuel_comparison = {176: [177, 182, 183, 181, 204],
                        177: [176, 182, 183, 181, 204],
                        182: [176, 177, 183, 181, 204],
                        183: [176, 177, 182, 181, 204],
                        181: [176, 177, 182, 183, 204],
                        204: [176, 177, 182, 183, 181],
                        3: [175],
                        175: [3],
                        2: [174],
                        174: [2],
                        4: [173, 41],
                        173: [4, 41],
                        41: [4, 173]
                        }
true_source = ['api_azsgo', 'api_azsopti', 'api_yandex_fuel', 'api_benzuber', 'api_yandex_other', 'manual_app',
               'manual_manual', 'manual_ppr', 'manual_gazprom', 'api_licard', 'api_gpn', 'api_lukoil', 'api_sng']
not_true_source = ['api_ppr', 'api_rncard', 'manual_other', 'manual_phone', 'api_fuelup']


def generate_station_query() -> str:
    """
    Генерирует SQL-запрос для получения данных по АЗС.

    :return: SQL-запрос в виде строки.
    """
    return f'''
        WITH client_azs AS (
            SELECT CCS.station_id AS cl_id, CCS.id
            FROM customer__customer_station AS CCS
            LEFT JOIN customer__customer AS CC ON CC.id = CCS.customer_id
            WHERE cc.status IN('client', 'demo', 'trial')),
            competitors_azs AS (
            SELECT DISTINCT CCSC.station_id AS comp_id
            FROM client_azs AS CA
            LEFT JOIN customer__customer_station_competitor AS CCSC ON CCSC.customer_station_id = CA.id
            where CCSC.station_id is not null
            union
            SELECT DISTINCT cl_id FROM client_azs)
        SELECT GSP.station_id, GSP.product_id, GSP.price, GSP.source_type, 
            CASE WHEN GSP.show_in_report = 't' 
                THEN 't' 
                    ELSE 'f' END AS show_in_report, 
        DATE(GSP.updated_at) as date_upd
        FROM gs__station_product AS GSP
        LEFT JOIN gs__station as gs on gs.id = gsp.station_id
        WHERE not exists (SELECT 1 
                          FROM competitors_azs AS CA 
                          WHERE CA.comp_id = GSP.station_id)
        AND ((GSP.show_in_report = 't' AND GSP.updated_at > (CURRENT_DATE-60)::date) OR (GSP.show_in_report = 'f' AND 
        GSP.updated_at > (CURRENT_DATE-21)::date))
        and gs.region_id < 86
        ORDER BY 1, 3, 5, 2
        '''


def process_data_price_off_1(**kwargs):
    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data')
    df01 = pd.DataFrame(prices)

    # Отключение источников цен, по которым даты превысили срок действия по отношению к текущей дате
    # Формирование ДФ с датами обновления цены более 7 дней назад
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']
    df_1 = df01.loc[(df01['source'].isin(true_source)) & (df01['show_in_report'] == 't') &
                    (df01['date_upd'] < datetime.now().date() - timedelta(days=6))]
    df_1 = df_1.groupby(['station_id', 'source']).count().reset_index().drop(['product_id', 'price', 
                                                                             'show_in_report', 'date_upd'], axis=1)
    st_for_upd_01 = [f"""(station_id = {df_1.station_id.iloc[i]} AND source_type = '{df_1.source.iloc[i]}' \
    AND date(updated_at) < CURRENT_DATE - 6) OR""" for i in range(df_1.shape[0])]

    conv_string = ' '.join(st_for_upd_01)[:-3] + ';'
    price_off1 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string
    # if st_for_upd_01:
    ti.xcom_push(key='price_off_6_day', value=price_off1)
    ti.xcom_push(key='marker1', value=st_for_upd_01)

    ##########  Проверка работы триггеров в airflow ###############
    # else:
    #     ti.xcom_push(key='price_off_6_day', value=f"SELECT 'Данных для изменения нет'")


def check_6days_data(**kwargs):
    ti = kwargs['ti']
    if ti.xcom_pull(task_ids='off_price_6days', key='marker1'):
        return 'update_off_price_6days'
    else:
        return 'skip_6days_data'


def process_data_price_off_2(**kwargs):
    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data')
    df01 = pd.DataFrame(prices)

    # Формирование ДФ с датами обновления цены более 16 дней назад
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']
    df_2 = df01.loc[(df01['source'].isin(not_true_source)) & (df01['show_in_report'] == 't') &
                    (df01['date_upd'] < datetime.now().date() - timedelta(days=16))]
    print(df_2)
    df_2 = df_2.groupby(['station_id', 'source']).count().reset_index().drop(['product_id', 'price', 
                                                                            'show_in_report', 'date_upd'], axis=1)
    st_for_upd_02 = [
        f"""(station_id = {df_2.station_id.iloc[i]} AND source_type = '{df_2.source.iloc[i]}' \
        AND date(updated_at) < CURRENT_DATE - 16) OR""" for i in range(df_2.shape[0])]
    conv_string = ' '.join(st_for_upd_02)[:-3] + ';'
    price_off2 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string
    # if st_for_upd_02:
    ti.xcom_push(key='price_off_17_day', value=price_off2)
    ti.xcom_push(key='marker2', value=st_for_upd_02)
    ##########  Проверка работы триггеров в airflow ###############
    # else:
    #     ti.xcom_push(key='price_off_17_day', value=f"SELECT 'Данных для изменения нет'")


def check_17days_data(**kwargs):
    ti = kwargs['ti']
    if ti.xcom_pull(task_ids='off_price_17days', key='marker2'):
        return 'update_off_price_17days'
    else:
        return 'skip_17days_data'


def transform_data_price_on(**kwargs):
    def check_price(source, station_id):
        """ Получаем id АЗС и источник цены, возвращаем проверку или количество ошибок по цене """
        res = 0
        for i in list(df01[(df01['station_id'] == station_id) & (df01['source'] == source)]['product_id']):
            if df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                    (df01['product_id'] == i)]['product_id'].values[0] not in check_dic:
                if (35 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                               (df01['product_id'] == i)]['price']).values[0] or \
                        (110 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                    (df01['product_id'] == i)]['price']).values[0]:
                    res += 1
            else:
                if (check_dic[i]['min'] >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                (df01['product_id'] == i)]['price']).values[0] or \
                        (check_dic[i]['max'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                     (df01['product_id'] == i)]['price']).values[0]:
                    res += 1
        if res == 0:
            return 'OK'
        elif res != 0:
            try:
                if res / df005[(df005['station_id'] == station_id) & (df005['source'] == source)]['count_fuel'] \
                        .values[0] > 0.3:
                    return 'err'
                else:
                    return res
            except Exception:
                print(f'err in table: OK - err - count_err, station_id - {station_id}')

    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data_2')
    df01 = pd.DataFrame(prices)
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']
    # ------ Формируются два ДФ и выделяется список АЗС, по которым не включены цены -------
    df02 = pd.DataFrame({'station_id': df01[df01['show_in_report'] == 't']['station_id'].unique()})
    df03 = pd.DataFrame({'station_id': df01[(df01['show_in_report'] == 'f') & (df01['price'] > 0) & \
                                            (df01['date_upd'] == datetime.now().date())]['station_id'].unique()})
    df04 = df03[~df03['station_id'].isin(df02['station_id'])]
    #  Формируется датафрейм с полями: общее кол-во видов топлива по АЗС, источник цены и количество источников цен на АЗС
    df05 = pd.DataFrame(
        {'count_fuel': df01[df01['station_id'].isin(df04['station_id'])].groupby(['station_id', 'source', 'date_upd'])
        .size()}).reset_index()
    df005 = df05.groupby(['station_id', 'source'])['count_fuel'].sum().reset_index()
    df06 = df05[df05['date_upd'] == datetime.now().date()].reset_index()
    df06 = df06.drop(['index'], axis=1)
    df06.loc[:, ['check', 'count_err']] = ''
    df07 = df06.groupby(['station_id'])['source'].count().reset_index().rename({'source': 'amount'}, axis=1)
    df08 = df06.merge(df07, on='station_id')
    # Выделям АЗС с корректными ценами - ОК, выделяем где ошибок больше 30% - err, добавить всего ошибок по источнику
    for k, i in enumerate(df08['source']):
        if check_price(i, df08.loc[k, ['station_id']].values[0]) == 'OK':
            df08.loc[k, 'check'] = check_price(i, df08.loc[k, ['station_id']].values[0])
        elif check_price(i, df08.loc[k, ['station_id']].values[0]) == 'err':
            df08.loc[k, 'check'] = check_price(i, df08.loc[k, ['station_id']].values[0])
        else:
            df08.loc[k, 'count_err'] = check_price(i, df08.loc[k, ['station_id']].values[0])
            df08.loc[k, 'check'] = 'OK'

    source_dict = {'api_gpn': 1, 'api_azsopti': 2, 'api_benzuber': 3, 'api_yandex_fuel': 4, 'api_licard': 5,
                   'api_lukoil': 6, 'api_sng': 7, 'api_rncard': 10, 'api_fuelup': 11, 'api_ppr': 12, 'manual_manual': 13,
                   'manual_app': 14, 'manual_phone': 15, 'manual_ppr': 16, 'manual_gazprom': 17, 'manual_other': 18,
                   'api_yandex_other': 19, }

    # ---- Формируется ДФ с единственным источником цены для внесения изменений в БД ----
    df08['rate'] = df08['source'].replace(source_dict)

    try:
        df09 = df08.groupby(['station_id'], as_index=False).apply(lambda x: x.nsmallest(1, ['rate'])).reset_index(
            drop=True)
        df09 = df09.to_json()
        ti.xcom_push(key='price_on_new', value=df09)
    except TypeError:
        print('Dataframe df08 is empty, then df09 = df08')


def price_on_trans_update_1(**kwargs):
    ti = kwargs['ti']
    prices_on = ti.xcom_pull(task_ids='price_new_1', key='price_on_new', default=None)

    if prices_on is not None:
        prices_on = json.loads(prices_on)
        df09 = pd.DataFrame(prices_on)
        df09.date_upd = df09.date_upd.astype('datetime64[ms]')
        # df09.columns = ['station_id', 'source', 'date_upd', 'count_fuel', 'check', 'count_err', 'amount', 'rate']
        df__10 = df09[(df09['check'] == 'OK') & (df09['count_err'] == '') & (df09['source'].isin(true_source))]
        df__11 = df09[(df09['check'] == 'OK') & (df09['count_err'] == '') & (df09['source'].isin(not_true_source))]
        # новый блок - замена списка запросов на один
        st_for_upd1 = [f"""(station_id = {df__10.station_id.iloc[i]} AND source_type = '{df__10.source.iloc[i]}' \
            AND DATE(updated_at) = CURRENT_DATE) OR"""
                       for i in range(df__10.shape[0])]

        conv_string1 = ' '.join(st_for_upd1)[:-3] + ';'
        price_on1 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string1

        st_for_upd2 = [f"""(station_id = {df__11.station_id.iloc[i]} AND source_type = '{df__11.source.iloc[i]}' \
            AND DATE(updated_at) BETWEEN CURRENT_DATE-14 AND CURRENT_DATE) OR"""
                       for i in range(df__11.shape[0])]

        conv_string2 = ' '.join(st_for_upd2)[:-3] + ';'
        price_on2 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string2
        # Запись изменений по источникам цен в БД

        if st_for_upd1:
            ti.xcom_push(key='price_on_upd_1', value=price_on1)
        else:
            ti.xcom_push(key='price_on_upd_1', value=f"SELECT 'Данных для изменения нет'")
        if st_for_upd2:
            ti.xcom_push(key='price_on_upd_2', value=price_on2)
        else:
            ti.xcom_push(key='price_on_upd_2', value=f"SELECT 'Данных для изменения нет'")
    else:
        ti.xcom_push(key='price_on_upd_1', value=f"SELECT 'Данных для изменения нет'")
        ti.xcom_push(key='price_on_upd_2', value=f"SELECT 'Данных для изменения нет'")


def price_on_trans_update_2(**kwargs):
    def check_price_product(source, station_id, product_id):
        """ Получаем id АЗС и источник цены, возвращаем id продукта без ошибки """
        if df01[(df01['station_id'] == station_id) & (df01['source'] == source) & (df01['product_id'] == product_id)] \
                ['product_id'].values[0] not in check_dic:
            if (35 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                           (df01['product_id'] == product_id)]['price']).values[0] and \
                    (110 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id
        else:
            if (check_dic[product_id]['min'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                     (df01['product_id'] == product_id)]['price']).values[0] \
                    and \
                    (check_dic[product_id]['max'] >=
                     df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                          (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id

    ti = kwargs['ti']
    prices_on = ti.xcom_pull(task_ids='price_new_1', key='price_on_new', default=None)
    if prices_on is not None:
        prices_on = json.loads(prices_on)
        df09 = pd.DataFrame(prices_on)
        df09.date_upd = df09.date_upd.astype('datetime64[ms]')
        # df09.columns = ['station_id', 'source', 'date_upd', 'count_fuel', 'check', 'count_err', 'amount', 'rate']
        prices = ti.xcom_pull(task_ids='sql_get_data_2')
        df01 = pd.DataFrame(prices)
        df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']
        # Включаем цены по каждому продукту в тех АЗС, где есть ошибки у одной или более позиций
        df__12 = df09[(df09['count_err'] != '') & (df09['source'].isin(true_source))].reset_index(level=0, drop=True)
        st_for_upd3 = []
        for k, i in enumerate(df__12['station_id']):
            for j in (df01[(df01['station_id'] == df__12.loc[k, ['station_id']].values[0]) &
                           (df01['source'] == df__12.loc[k, ['source']].values[0])]['product_id']):
                if check_price_product(df__12.loc[k, ['source']].values[0], df__12.loc[k, ['station_id']].values[0],
                                       j) is not None:
                    st_for_upd3.append(
                        f"""(station_id = {i} AND source_type = '{df__12.source.iloc[k]}' \
                        AND product_id = {j} AND DATE(updated_at) = CURRENT_DATE) OR""")

        conv_string3 = ' '.join(st_for_upd3)[:-3] + ';'
        price_on3 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string3
        # Запись изменений по источникам цен в БД
        if st_for_upd3:
            ti.xcom_push(key='price_on_upd_3', value=price_on3)
        else:
            ti.xcom_push(key='price_on_upd_3', value=f"SELECT 'Данных для изменения нет'")
    else:
        ti.xcom_push(key='price_on_upd_3', value=f"SELECT 'Данных для изменения нет'")


def price_on_trans_update_3(**kwargs):
    def check_price_product(source, station_id, product_id):
        """ Получаем id АЗС и источник цены, возвращаем id продукта без ошибки """
        if df01[(df01['station_id'] == station_id) & (df01['source'] == source) & (df01['product_id'] == product_id)] \
                ['product_id'].values[0] not in check_dic:
            if (35 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                           (df01['product_id'] == product_id)]['price']).values[0] and \
                    (110 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id
        else:
            if (check_dic[product_id]['min'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                     (df01['product_id'] == product_id)]['price']).values[0] \
                    and \
                    (check_dic[product_id]['max'] >=
                     df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                          (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id

    ti = kwargs['ti']
    prices_on = ti.xcom_pull(task_ids='price_new_1', key='price_on_new', default=None)
    if prices_on is not None:
        prices_on = json.loads(prices_on)
        df09 = pd.DataFrame(prices_on)
        df09.date_upd = df09.date_upd.astype('datetime64[ms]')
        # df09.columns = ['station_id', 'source', 'date_upd', 'count_fuel', 'check', 'count_err', 'amount', 'rate']
        prices = ti.xcom_pull(task_ids='sql_get_data_2')
        df01 = pd.DataFrame(prices)
        df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']
        # Включаем цены по каждому продукту в тех АЗС, где есть ошибки у одной или более позиций источник ППР
        df__13 = df09[(df09['check'] == 'OK') & (df09['count_err'] != '') &
                      (df09['source'].isin(not_true_source))].reset_index(level=0, drop=True)
        st_for_upd4 = []
        for k, i in enumerate(df__13['station_id']):
            for j in (df01[(df01['station_id'] == df__13.loc[k, ['station_id']].values[0]) &
                           (df01['source'] == df__13.loc[k, ['source']].values[0])]['product_id']):
                if check_price_product(df__13.loc[k, ['source']].values[0], df__13.loc[k, ['station_id']].values[0],
                                       j) is not None:
                    st_for_upd4.append(
                        f"""(station_id = {i} AND source_type = '{df__13.source.iloc[k]}' AND product_id = {j} \
                        AND DATE(updated_at) BETWEEN CURRENT_DATE-14 AND CURRENT_DATE) OR""")

        conv_string4 = ' '.join(st_for_upd4)[:-3] + ';'
        price_on4 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string4
        # Запись изменений по источникам цен в БД
        if st_for_upd4:
            ti.xcom_push(key='price_on_upd_4', value=price_on4)
        else:
            ti.xcom_push(key='price_on_upd_4', value=f"SELECT 'Данных для изменения нет'")
    else:
        ti.xcom_push(key='price_on_upd_4', value=f"SELECT 'Данных для изменения нет'")


def price_on_trans_update_bd(**kwargs):
    def check_price_product(source, station_id, product_id):
        """ Получаем id АЗС и источник цены, возвращаем id продукта без ошибки """
        if df01[(df01['station_id'] == station_id) & (df01['source'] == source) & (df01['product_id'] == product_id)] \
                ['product_id'].values[0] not in check_dic:
            if (35 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                           (df01['product_id'] == product_id)]['price']).values[0] and \
                    (110 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id
        else:
            if (check_dic[product_id]['min'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                     (df01['product_id'] == product_id)]['price']).values[0] \
                    and \
                    (check_dic[product_id]['max'] >=
                     df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                          (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id

    def check_fuel_comparison(source, station_id, product_id):
        """ Получаем информацию по АЗС и проверяем брендовые/типовое топливо на идентичность и равную цену """
        if product_id in dict_fuel_comparison:
            for i in dict_fuel_comparison[product_id]:
                if i in list(df01[(df01['source'] == source) & (df01['date_upd'] == datetime.now().date()) &
                                  (df01['station_id'] == station_id) & (df01['show_in_report'] == 't')]['product_id']):
                    if source in true_source:
                        if df01[(df01['source'] == source) & (df01['date_upd'] == datetime.now().date()) &
                                (df01['product_id'] == i) & (df01['station_id'] == station_id) &
                                (df01['show_in_report'] == 't')]['price'].values[0] == \
                                df01[(df01['source'] == source) & (df01['date_upd'] == datetime.now().date()) &
                                     (df01['product_id'] == product_id) & (df01['station_id'] == station_id)][
                                    'price'].values[0]:
                            return i
                    else:
                        if df01[(df01['source'] == source) & (df01['date_upd'] == datetime.now().date()) &
                                (df01['product_id'] == i) & (df01['station_id'] == station_id) &
                                (df01['show_in_report'] == 't')]['price'].values[0] == \
                                df01[(df01['source'] == source) & (
                                        df01['date_upd'] > datetime.now().date() - timedelta(days=16)) &
                                     (df01['product_id'] == product_id) & (df01['station_id'] == station_id)][
                                    'price'].values[0]:
                            return i

    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data_3')
    df01 = pd.DataFrame(prices)
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']
    # Включение цен, по которым уже есть источник цен, но появились новые виды топлива на текущую дату.
    # Формируются два ДФ и выделяется список АЗС, где некоторые виды топлива не включены.
    df10 = pd.DataFrame(
        {'count_fuel_true': df01[df01['show_in_report'] == 't'].groupby(['station_id', 'source']).size()}).reset_index()
    df99 = df10.groupby(['station_id'])['source'].count().reset_index().rename({'source': 'count_source'}, axis=1)
    df10 = df10.merge(df99, on='station_id', how='left')
    df10 = df10.loc[df10['count_source'] == 1, ['station_id', 'source', 'count_fuel_true', 'count_source']]

    # Формируем ДФ с источниками, которые не идут в отчёт
    df11 = pd.DataFrame({'count_fuel_false': df01[
        (df01['show_in_report'] == 'f') & (df01['price'] > 0) & (df01['date_upd'] == datetime.now().date())]
                        .groupby(['station_id', 'source']).size()}).reset_index()

    # Объединяем датафреймы
    df12 = df10.merge(df11, on=['station_id', 'source'], how='inner')

    # ------- Изменил создание записи запроса для уменьшения обращений к БД --------
    # Формирование списка кода UPDATE для БД
    list_new_product_price = []
    for k, i in enumerate(df12['station_id']):
        if df12.loc[k, ['source']].isin(true_source).values[0]:
            for j in df01[(df01['station_id'] == i) & (df01['date_upd'] == datetime.now().date()) &
                          (df01['show_in_report'] == 'f') & (df01['source'] == df12.loc[k, 'source'])]['product_id']:
                if check_price_product(df12.loc[k, ['source']].values[0], i, j) is not None and \
                        check_fuel_comparison(df12.loc[k, ['source']].values[0], i, j) is None:
                    list_new_product_price.append(
                        f"""(station_id = {str(i)} and source_type = '{df12[df12['station_id'] == i]['source'].values[0]}'\
                         and product_id = {str(check_price_product(df12.loc[k, ['source']].values[0], i, j))} and \
                         DATE(updated_at) = CURRENT_DATE) OR""")
        else:
            for j in df01[(df01['station_id'] == i) & (df01['date_upd'] > datetime.now().date() - timedelta(days=8)) &
                          (df01['show_in_report'] == 'f') & (df01['source'] == df12.loc[k, 'source'])]['product_id']:
                if check_price_product(df12.loc[k, ['source']].values[0], i, j) is not None and \
                        check_fuel_comparison(df12.loc[k, ['source']].values[0], i, j) is None:
                    list_new_product_price.append(
                        f"""(station_id = {str(i)} and source_type = '{df12[df12['station_id'] == i]['source'].values[0]}'\
                         and product_id = {str(check_price_product(df12.loc[k, ['source']].values[0], i, j))} and \
                         DATE(updated_at) BETWEEN CURRENT_DATE - 7 AND CURRENT_DATE) OR""")

    # Дополнительно отключаем старые цены по источникам в df12
    df13 = df12[~df12['source'].isin(not_true_source)]
    list_off_product_price1 = []
    for k, i in enumerate(df13['station_id']):
        if not df01[(df01['station_id'] == i) & (df01['date_upd'] < datetime.now().date()) &
                    (df01['show_in_report'] == 't') &
                    (df01['source'] == df13[df13['station_id'] == i]['source'].values[0])]['product_id'].empty:
            for l in df01[(df01['station_id'] == i) & (df01['date_upd'] < datetime.now().date()) &
                          (df01['show_in_report'] == 't') & (
                                  df01['source'] == df13[df13['station_id'] == i]['source'].values[0])]['product_id']:
                list_off_product_price1.append(
                    f"""(station_id = {str(i)} and source_type = '{df13[df13['station_id'] == i]['source'].values[0]}' \
    and product_id = {str(l)}) OR""")

    conv_string5 = ' '.join(list_new_product_price)[:-3] + ';'
    price_on5 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string5
    # Запись изменений по источникам цен в БД
    if list_new_product_price:
        ti.xcom_push(key='price_on_upd_5', value=price_on5)
    else:
        ti.xcom_push(key='price_on_upd_5', value=f"SELECT 'Данных для изменения нет'")

    conv_string6 = ' '.join(list_off_product_price1)[:-3] + ';'
    price_off6 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string6
    # Запись изменений по источникам цен в БД
    if list_off_product_price1:
        ti.xcom_push(key='price_off_upd_6', value=price_off6)
    else:
        ti.xcom_push(key='price_off_upd_6', value=f"SELECT 'Данных для изменения нет'")


def change_price_source(**kwargs):
    def check_price_product(source, station_id, product_id):
        """ Получаем id АЗС и источник цены, возвращаем id продукта без ошибки """
        if df01[(df01['station_id'] == station_id) & (df01['source'] == source) & (df01['product_id'] == product_id)] \
                ['product_id'].values[0] not in check_dic:
            if (35 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                           (df01['product_id'] == product_id)]['price']).values[0] and \
                    (110 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id
        else:
            if (check_dic[product_id]['min'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                     (df01['product_id'] == product_id)]['price']).values[0] \
                    and \
                    (check_dic[product_id]['max'] >=
                     df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                          (df01['product_id'] == product_id)]['price']).values[0]:
                return product_id

    source_dict = {'api_gpn': 1, 'api_azsopti': 2, 'api_benzuber': 3, 'api_yandex_fuel': 4, 'api_licard': 5,
                   'api_lukoil': 6, 'api_sng':7, 'api_rncard': 10, 'api_fuelup': 11, 'api_ppr': 12, 'manual_manual': 13,
                   'manual_app': 14, 'manual_phone': 15, 'manual_ppr': 16, 'manual_gazprom': 17, 'manual_other': 18,
                   'api_yandex_other': 19, }
    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data_2')
    df01 = pd.DataFrame(prices)
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']

    # Замена недостоверных источников на достоверные.
    # Формируется два ДФ для замены одних источников на другие.
    df14 = pd.DataFrame({'count_fuel_true': df01[(df01['show_in_report'] == 't') &
                                                 (df01['source'].isin(['api_ppr', 'api_rncard', 'api_yandex_other',
                                                                       'api_fuelup']))]
                        .groupby(['station_id', 'source']).size()}).reset_index()
    df15 = df14.groupby(['station_id'])['source'].count().reset_index().rename({'source': 'count_source'}, axis=1)
    df16 = df14.merge(df15, on='station_id', how='left')
    df16 = df16.loc[df16['count_source'] == 1, ['station_id', 'source', 'count_source']]
    df17 = pd.DataFrame({'count_fuel_false': df01[(df01['show_in_report'] == 'f') & (df01['price'] > 0) &
                                                  (df01['date_upd'] == datetime.now().date()) &
                                                  (df01['source'].isin(['api_azsgo', 'api_benzuber', 'api_yandex_fuel',
                                                                        'api_azsopti', 'api_licard', 'api_gpn',
                                                                        'api_lukoil', 'api_sng']))]
                        .groupby(['station_id', 'source']).size()}).reset_index()

    df17['rate'] = df17['source'].replace(source_dict)
    try:
        df18 = df17.groupby(['station_id'], as_index=False).apply(lambda x: x.nsmallest(1, ['rate'])).reset_index(
            level=1,
            drop=True)
    except Exception:
        df18 = df17
    df19 = df16.merge(df18, on=['station_id'], how='inner')
    # Удаляем из списка АЗС с двумя источниками с флагом show_in_rep = True
    df001 = df01[(df01['show_in_report'] == 't')].groupby(['station_id', 'source']).count().reset_index()
    df002 = df001.groupby(['station_id'])['source'].count().reset_index().rename({'source': 'count_source'}, axis=1)
    df002 = df002[df002['count_source'] > 1]
    df19 = df19.set_index('station_id')
    df002 = df002.set_index('station_id')
    df19.update(df002)
    df20 = df19[df19['count_source'] == 1].reset_index()

    # Отключаем цены по недоствоерным источникам и включаем достоверные:
    list_off_source_unreliable = [
        f"""(station_id = {df20.loc[i]['station_id']} and source_type = '{df20.loc[i]['source_x']}') OR"""
        for i in df20.index]
    conv_string7 = ' '.join(list_off_source_unreliable)[:-3] + ';'
    price_off7 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string7

    if list_off_source_unreliable:
        ti.xcom_push(key='price_off_upd_7', value=price_off7)
    else:
        ti.xcom_push(key='price_off_upd_7', value=f"SELECT 'Данных для изменения нет'")

    list_on_source_unreliable = []
    for k, i in enumerate(df20['station_id'].unique()):
        for j in df01[(df01['station_id'] == i) & (df01['date_upd'] == datetime.now().date()) &
                      (df01['show_in_report'] == 'f') & (df01['source'] == df20.loc[k, 'source_y'])]['product_id']:
            if check_price_product(df20.loc[k, ['source_y']].values[0], i, j) is not None:
                list_on_source_unreliable.append(f"""(station_id = {i} and source_type = '{df20.loc[k]['source_y']}' \
                and product_id = {j} and DATE(updated_at) = CURRENT_DATE) OR""")

    conv_string8 = ' '.join(list_on_source_unreliable)[:-3] + ';'
    price_on8 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string8

    if list_on_source_unreliable:
        ti.xcom_push(key='price_on_upd_8', value=price_on8)
    else:
        ti.xcom_push(key='price_on_upd_8', value=f"SELECT 'Данных для изменения нет'")


def change_yf_bz(**kwargs):
    def check_price(source, station_id):
        """ Получаем id АЗС и источник цены, возвращаем проверку или количество ошибок по цене """
        res = 0
        for i in list(df01[(df01['station_id'] == station_id) & (df01['source'] == source)]['product_id']):
            if df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                    (df01['product_id'] == i)]['product_id'].values[0] not in check_dic:
                if (35 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                               (df01['product_id'] == i)]['price']).values[0] or \
                        (110 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                    (df01['product_id'] == i)]['price']).values[0]:
                    res += 1
            else:
                if (check_dic[i]['min'] >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                (df01['product_id'] == i)]['price']).values[0] or \
                        (check_dic[i]['max'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
                                                     (df01['product_id'] == i)]['price']).values[0]:
                    res += 1
        if res == 0:
            return 'OK'
        elif res != 0:
            try:
                if res / df005[(df005['station_id'] == station_id) & (df005['source'] == source)]['count_fuel'] \
                        .values[0] > 0.3:
                    return 'err'
                else:
                    return res
            except Exception:
                print(f'err in table: OK - err - count_err, station_id - {station_id}')

    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data_2')
    df01 = pd.DataFrame(prices)
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']

    # ------ Формируются два ДФ и выделяется список АЗС, по которым не включены цены -------
    df02 = pd.DataFrame({'station_id': df01[df01['show_in_report'] == 't']['station_id'].unique()})
    df03 = pd.DataFrame({'station_id': df01[(df01['show_in_report'] == 'f') & (df01['price'] > 0) & \
                                            (df01['date_upd'] == datetime.now().date())]['station_id'].unique()})
    df04 = df03[~df03['station_id'].isin(df02['station_id'])]
    # -- Формируется датафрейм с полями: общее кол-во видов топлива по АЗС, источник цены и количество источников цен на АЗС
    df05 = pd.DataFrame(
        {'count_fuel': df01[df01['station_id'].isin(df04['station_id'])].groupby(['station_id', 'source', 'date_upd'])
        .size()}).reset_index()
    df005 = df05.groupby(['station_id', 'source'])['count_fuel'].sum().reset_index()

    # Switch off source yandex_fuel and switch on - benzuber if count fuel more then yandex_fuel
    # Формируется 2 ДФ для замены одних источников на другие
    df21 = pd.DataFrame({'count_fuel_true': df01[(df01['show_in_report'] == 't') &
                                                 (df01['source'].isin(['api_yandex_fuel']))].
                        groupby(['station_id', 'source']).size()}).reset_index()
    df22 = pd.DataFrame(
        {'count_fuel_true': df01[(df01['show_in_report'] == 'f') & (df01['source'].isin(['api_benzuber'])) &
                                 (df01['date_upd'] == datetime.now().date())]. \
            groupby(['station_id', 'source']).size()}).reset_index()
    df23 = df22.loc[df22['station_id'].isin(df21['station_id'])]
    df24 = df23.merge(df21, on='station_id', how='left')
    df25 = df24[df24['count_fuel_true_x'] >= df24['count_fuel_true_y']].reset_index().drop(['index'], axis=1)

    # Отключаем цены с yandex_fuel и включаем benzuber:
    list_yaf, list_bz = [], []
    for k, i in enumerate(df25['station_id'].unique()):
        if check_price(df25.loc[k, ['source_x']].values[0], i) == 'OK':
            list_yaf.append(f"""(station_id = {i} and source_type = '{df25.loc[k]['source_y']}') OR""")
            list_bz.append(f"""(station_id = {i} and source_type = '{df25.loc[k]['source_x']}' and \
    DATE(updated_at) = CURRENT_DATE) OR""")

    conv_string9 = ' '.join(list_yaf)[:-3] + ';'
    price_off9 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string9

    if list_yaf:
        ti.xcom_push(key='price_off_upd_9', value=price_off9)
    else:
        ti.xcom_push(key='price_off_upd_9', value=f"SELECT 'Данных для изменения нет'")

    conv_string10 = ' '.join(list_bz)[:-3] + ';'
    price_on10 = f"""UPDATE gs__station_product SET show_in_report = 't' where """ + conv_string10

    if list_bz:
        ti.xcom_push(key='price_on_upd_10', value=price_on10)
    else:
        ti.xcom_push(key='price_on_upd_10', value=f"SELECT 'Данных для изменения нет'")


def correct_price(**kwargs):
    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data_2')
    df01 = pd.DataFrame(prices)
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']

    # Отключение цен по общей границе для всех видов топлива (price <8 & >103)
    df27 = df01[((df01['price'] < 11) | (df01['price'] > 110)) & (df01['show_in_report'] == 't')] \
        [['station_id', 'product_id', 'source']].reset_index().drop(['index'], axis=1)

    list_wrong_price = [f"""(station_id = {df27.loc[i]['station_id']} and source_type = '{df27.loc[i]['source']}' and \
    product_id = {df27.loc[i]['product_id']}) OR""" for i in df27.index]

    conv_string11 = ' '.join(list_wrong_price)[:-3] + ';'
    price_off11 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string11
    if list_wrong_price:
        ti.xcom_push(key='price_off_upd_11', value=price_off11)
    else:
        ti.xcom_push(key='price_off_upd_11', value=f"SELECT 'Данных для изменения нет'")


def correct_diesel(**kwargs):
    ti = kwargs['ti']
    prices = ti.xcom_pull(task_ids='sql_get_data_4')
    df01 = pd.DataFrame(prices)
    df01.columns = ['station_id', 'product_id', 'price', 'source', 'show_in_report', 'date_upd']

    # После перевода дизеля на сезонность появилась необходимость оперативно отключать из отчёта старые данные по дизелю
    # В данном коде фильтруем все АСЗ у которых более 2 ДТ, которые идут в отчёт. Далее формируем список на отключение
    # где одинаковые цены на ДТ

    df_01 = pd.DataFrame({'count_diezel': df01[(df01['show_in_report'] == 't') &
                                               (df01['product_id'].isin([176, 181, 182, 183, 184]))]
                         .groupby(['station_id']).size()}).reset_index()
    df_01 = df_01[df_01['count_diezel'] > 1]
    df_02 = df01[(df01['show_in_report'] == 't') & (df01['product_id'].isin([176, 181, 182, 183, 184]))][
        ['station_id', 'product_id', 'price', 'date_upd']]
    df_03 = df_01.merge(df_02, on='station_id', how='left')
    df_04 = df_03.sort_values(['station_id', 'date_upd'], ascending=[True, True]).drop_duplicates(
        ['station_id', 'price'])
    df_05 = df_04.groupby(['station_id'])['product_id'].count().reset_index()
    df_05 = df_05[df_05['product_id'] == 1]
    df_06 = df_04[df_04['station_id'].isin(df_05['station_id'])]
    if not df_06.empty:
        conv_string12 = df_06.apply(lambda
                                        row: f"(station_id = {row['station_id']} and product_id = {row['product_id']} and show_in_report = 't') OR",
                                    axis=1).str.cat(sep=' ')
        conv_string12 = conv_string12[:-3] + ';'
        price_diesel_off12 = f"""UPDATE gs__station_product SET show_in_report = 'f' where """ + conv_string12
    else:
        conv_string12 = ''
    if conv_string12:
        ti.xcom_push(key='price_off_upd_12', value=price_diesel_off12)
    else:
        ti.xcom_push(key='price_off_upd_12', value=f"SELECT 'Данных для изменения нет'")


with DAG('work_exa_2',
         default_args=default_args,
         schedule_interval='0 1,8,11,15,21 * * *',
         catchup=False) as dag:
    # Операторы выключения и обновления бд старых цен
    off_price_6days = PythonOperator(
        task_id='off_price_6days',
        python_callable=process_data_price_off_1,
    )
    off_price_17days = PythonOperator(
        task_id='off_price_17days',
        python_callable=process_data_price_off_2,
    )
    check_6days_data = BranchPythonOperator(
        task_id='check_6days_data',
        python_callable=check_6days_data,
        provide_context=True,
    )
    skip_step1 = DummyOperator(task_id='skip_6days_data')
    join_step1 = DummyOperator(task_id='join_step_6days_data', trigger_rule='none_failed')
    update_off_price_6days = SQLExecuteQueryOperator(
        task_id='update_off_price_6days',
        sql="{{ti.xcom_pull(task_ids='off_price_6days', key='price_off_6_day')}}",
        conn_id='datomt',
    )
    check_17days_data = BranchPythonOperator(
        task_id='check_17days_data',
        python_callable=check_17days_data,
        provide_context=True,
    )
    skip_step2 = DummyOperator(task_id='skip_17days_data')
    join_step2 = DummyOperator(task_id='join_step_17days_data', trigger_rule='none_failed')
    update_off_price_17days = SQLExecuteQueryOperator(
        task_id='update_off_price_17days',
        sql="{{ti.xcom_pull(task_ids='off_price_17days', key='price_off_17_day')}}",
        conn_id='datomt',
    )
    # Операторы включения и обновления новых цен, АЗС которых нет в отчётах
    price_new_on = PythonOperator(
        task_id='price_new_1',
        python_callable=transform_data_price_on,
    )
    price_new_on_upd_1 = PythonOperator(
        task_id='price_new_on_upd_1',
        python_callable=price_on_trans_update_1,
    )
    price_new_on_upd_2 = PythonOperator(
        task_id='price_new_on_upd_2',
        python_callable=price_on_trans_update_2,
    )
    price_new_on_upd_3 = PythonOperator(
        task_id='price_new_on_upd_3',
        python_callable=price_on_trans_update_3,
    )
    update_on_new_price_1 = SQLExecuteQueryOperator(
        task_id='update_on_new_price_1',
        sql=["{{ti.xcom_pull(task_ids='price_new_on_upd_1', key='price_on_upd_1')}}",
             "{{ti.xcom_pull(task_ids='price_new_on_upd_1', key='price_on_upd_2')}}"],
        conn_id='datomt'
    )
    # update_on_new_price_2 = PostgresOperator(
    #     task_id='update_on_new_price_2',
    #     sql="{{ ti.xcom_pull(task_ids='price_new_on_upd_1', key='price_on_upd_2')}}",
    #     postgres_conn_id='datomt'
    # )
    update_on_new_price_3 = SQLExecuteQueryOperator(
        task_id='update_on_new_price_3',
        sql="{{ti.xcom_pull(task_ids='price_new_on_upd_2', key='price_on_upd_3')}}",
        conn_id='datomt'
    )
    update_on_new_price_4 = SQLExecuteQueryOperator(
        task_id='update_on_new_price_4',
        sql="{{ti.xcom_pull(task_ids='price_new_on_upd_3', key='price_on_upd_4')}}",
        conn_id='datomt'
    )
    # Операторы включения и обновления новых цен по тем АЗС, которые уже идут в отчёт
    price_new_on_in_report = PythonOperator(
        task_id='price_new_on_in_report',
        python_callable=price_on_trans_update_bd,
    )
    update_new_price_in_report = SQLExecuteQueryOperator(
        task_id='update_new_price_in_report',
        sql=["{{ti.xcom_pull(task_ids='price_new_on_in_report', key='price_on_upd_5')}}",
             "{{ti.xcom_pull(task_ids='price_new_on_in_report', key='price_off_upd_6')}}"],
        conn_id='datomt'
    )
    # Операторы включения и обновления достоверных источников данных
    change_source = PythonOperator(
        task_id='change_source',
        python_callable=change_price_source,
    )
    update_change_source = SQLExecuteQueryOperator(
        task_id='update_change_source',
        sql=["{{ti.xcom_pull(task_ids='change_source', key='price_off_upd_7')}}",
             "{{ti.xcom_pull(task_ids='change_source', key='price_on_upd_8')}}"],
        conn_id='datomt'
    )
    # Операторы включения и обновления источника цен яндекс заправки на бензубер
    change_yf_on_bz = PythonOperator(
        task_id='change_yf_on_bz',
        python_callable=change_yf_bz,
    )
    update_change_yf_on_bz = SQLExecuteQueryOperator(
        task_id='update_change_yf_on_bz',
        sql=["{{ti.xcom_pull(task_ids='change_yf_on_bz', key='price_off_upd_9')}}",
             "{{ti.xcom_pull(task_ids='change_yf_on_bz', key='price_on_upd_10')}}"],
        conn_id='datomt'
    )
    # Оператор отключения источника с некорректной ценой
    change_wrong_price = PythonOperator(
        task_id='change_wrong_price',
        python_callable=correct_price,
    )
    update_change_wrong_price = SQLExecuteQueryOperator(
        task_id='update_change_wrong_price',
        sql="{{ti.xcom_pull(task_ids='change_wrong_price', key='price_off_upd_11')}}",
        conn_id='datomt'
    )
    # Операторы отключения старых цен дизелю при смене сезона
    change_old_diesel = PythonOperator(
        task_id='change_old_diesel',
        python_callable=correct_diesel,
    )
    update_old_price_diesel = SQLExecuteQueryOperator(
        task_id='update_old_price_diesel',
        sql="{{ti.xcom_pull(task_ids='change_old_diesel', key='price_off_upd_12')}}",
        conn_id='datomt'
    )
    # Операторы получения данных из БД - датомт
    sql_get_data = SQLExecuteQueryOperator(
        task_id='sql_get_data',
        sql=generate_station_query(),
        conn_id='datomt'
    )
    sql_get_data_2 = SQLExecuteQueryOperator(
        task_id='sql_get_data_2',
        sql=generate_station_query(),
        conn_id='datomt',
    )
    sql_get_data_3 = SQLExecuteQueryOperator(
        task_id='sql_get_data_3',
        sql=generate_station_query(),
        conn_id='datomt'
    )
    sql_get_data_4 = SQLExecuteQueryOperator(
        task_id='sql_get_data_4',
        sql=generate_station_query(),
        conn_id='datomt'
    )

    sql_get_data >> [off_price_6days, off_price_17days]
    off_price_6days >> check_6days_data >> [skip_step1, update_off_price_6days] >> join_step1 >> sql_get_data_2
    off_price_17days >> check_17days_data >> [skip_step2, update_off_price_17days] >> join_step2 >> sql_get_data_2
    sql_get_data_2 >> price_new_on >> [price_new_on_upd_1, price_new_on_upd_2, price_new_on_upd_3,
                                       change_source, change_yf_on_bz, change_wrong_price]

    [price_new_on_upd_1 >> update_on_new_price_1, price_new_on_upd_2 >> update_on_new_price_3,
     price_new_on_upd_3 >> update_on_new_price_4, change_yf_on_bz >> update_change_yf_on_bz,
     change_source >> update_change_source,
     change_wrong_price >> update_change_wrong_price] >> sql_get_data_3 >> price_new_on_in_report
    price_new_on_in_report >> update_new_price_in_report
    update_new_price_in_report >> sql_get_data_4 >> change_old_diesel >> update_old_price_diesel
