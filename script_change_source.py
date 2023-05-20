import datetime
import pandas as pd
from psycopg2 import OperationalError
from connection_db import connection1


def update_query(connection, query):
	""" Получаем запрос и вносим изменения в БД """
	try:
		with connection.cursor() as curs:
			curs.execute(query)
			connection.commit()
			return 'Done'
	except OperationalError as e:
		print(f"The error '{e}' occurred")


def execute_read_query1(connection, query):
	""" Получаем запрос и превращаем в датафрейм pandas """
	result = None
	try:
		with connection.cursor() as curs:
			curs.execute(query)
			result = curs.fetchall()
			return result
	except OperationalError as e:
		print(f"The error '{e}' occurred")


def main():
	def check_price(source, station_id):
		""" Получаем id АЗС и источник цены, возвращаем проверку или количество ошибок по цене """
		res = 0
		for i in list(df01[(df01['station_id'] == station_id) & (df01['source'] == source)]['product_id']):
			if df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
					(df01['product_id'] == i)]['product_id'].values[0] not in check_dic:
				if (35 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
							   (df01['product_id'] == i)]['price']).values[0] or \
					(87 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
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

	def check_price_product(source, station_id, product_id):
		""" Получаем id АЗС и источник цены, возвращаем id продукта без ошибки """
		if df01[(df01['station_id'] == station_id) & (df01['source'] == source) & (df01['product_id'] == product_id)] \
			['product_id'].values[0] not in check_dic:
			if (35 <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
						   (df01['product_id'] == product_id)]['price']).values[0] and \
				(87 >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
							(df01['product_id'] == product_id)]['price']).values[0]:
				return product_id
		else:
			if (check_dic[product_id]['min'] <= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
													 (df01['product_id'] == product_id)]['price']).values[0] \
				and \
				(check_dic[product_id]['max'] >= df01[(df01['station_id'] == station_id) & (df01['source'] == source) &
													  (df01['product_id'] == product_id)]['price']).values[0]:
				return product_id

	def check_fuel_comparison(source, station_id, product_id):
		""" Получаем информацию по АЗС и проверяем брендовые/типовое топливо на идентичность и равную цену """
		if product_id in dict_fuel_comparison:
			for i in dict_fuel_comparison[product_id]:
				if i in list(df01[(df01['source'] == source) & (df01['date_upd'] == datetime.datetime.now().date()) &
								  (df01['station_id'] == station_id) & (df01['show_in_report'] == 't')]['product_id']):

					if df01[(df01['source'] == source) & (df01['date_upd'] == datetime.datetime.now().date()) &
							(df01['product_id'] == i) & (df01['station_id'] == station_id) &
							(df01['show_in_report'] == 't')]['price'].values[0] == \
						df01[(df01['source'] == source) & (df01['date_upd'] == datetime.datetime.now().date()) &
							 (df01['product_id'] == product_id) & (df01['station_id'] == station_id)]['price'].values[
							0]:
						return i

	check_dic = {2: {'max': 85, 'min': 35}, 3: {'max': 85, 'min': 35}, 4: {'max': 85, 'min': 44},
				 41: {'max': 85, 'min': 45}, 42: {'max': 65, 'min': 33}, 173: {'max': 85, 'min': 45},
				 174: {'max': 85, 'min': 38}, 175: {'max': 85, 'min': 38}, 176: {'max': 90, 'min': 38},
				 139: {'max': 85, 'min': 40}, 177: {'max': 90, 'min': 46}, 184: {'max': 92, 'min': 45},
				 178: {'max': 40, 'min': 10}, 179: {'max': 45, 'min': 11}, 181: {'max': 87, 'min': 40},
				 182: {'max': 87, 'min': 40}, 183: {'max': 92, 'min': 45}, 189: {'max': 60, 'min': 40},
				 204: {'max': 60, 'min': 40}, 133: {'max': 60, 'min': 40}, 156: {'max': 60, 'min': 30},
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

	query_datomt = """
	WITH client_azs AS (
		SELECT CCS.station_id AS cl_id, CCS.id
		FROM customer__customer_station AS CCS
		LEFT JOIN customer__customer AS CC ON CC.id = CCS.customer_id
		WHERE cc.status IN('client')),
	
		competitors_azs AS (
		SELECT DISTINCT CCSC.station_id AS comp_id
		FROM client_azs AS CA
		LEFT JOIN customer__customer_station_competitor AS CCSC ON CCSC.customer_station_id = CA.id
		where CCSC.station_id is not null
		union
		SELECT DISTINCT cl_id FROM client_azs)
	
	SELECT GSP.station_id, GSP.product_id, DP.name AS product_name, DP.name AS продукт_общ, GSP.price, GSP.source_type, 
		CASE WHEN GSP.show_in_report = 't' 
			THEN 't' 
				ELSE 'f' END AS show_in_report, 
	DATE(GSP.updated_at) as date_upd
	FROM gs__station_product AS GSP
	LEFT JOIN directory__product AS DP ON DP.id = GSP.product_id
	WHERE GSP.station_id NOT IN(
		SELECT comp_id FROM competitors_azs)
	AND ((GSP.show_in_report = 't' AND DATE(GSP.updated_at) > CURRENT_DATE-60) OR (GSP.show_in_report = 'f' AND DATE(GSP.updated_at) > CURRENT_DATE-7))
	
	ORDER BY 1, 5, 7, 2
	"""
	prices = execute_read_query1(connection1(), query_datomt)
	df01 = pd.DataFrame(prices)
	df01.rename(columns={0: 'station_id', 1: 'product_id', 2: 'product', 3: 'prod_other', 4: 'price', 5: 'source',
						 6: 'show_in_report', 7: 'date_upd'}, inplace=True)

	# Отключение источников цен, по которым даты превысили срок действия по отношению к текущей дате
	# Формирование ДФ с датами обновления цены более 7 дней назад
	df_1 = df01.loc[
		(df01['source'].isin(
			['api_azsgo', 'api_azsopti', 'api_yandex_fuel', 'api_benzuber', 'api_yandex_other', 'manual_app',
			 'manual_manual', 'manual_phone', 'manual_ppr', 'manual_gazprom', 'manual_other', 'api_licard']))
		& (df01['show_in_report'] == 't') & (
			df01['date_upd'] < datetime.datetime.now().date() - datetime.timedelta(days=6))]
	list_off_source = []
	for i in df_1['station_id'].unique():
		for j in df_1[df_1['station_id'] == i]['product_id']:
			list_off_source.append(
				('update gs__station_product set show_in_report = \'f\' where station_id = ' + str(i) +
				 ' and source_type = \'' + df_1[df_1['station_id'] == i]['source'].unique() +
				 '\' and product_id = ' + str(j) + ';')[0])
	# Запись изменений - удаление старых цен в БД
	for i in range(len(list_off_source)):
		for j in list_off_source[i: i + 1]:
			update_query(connection1(), j)

	# Формирование ДФ с датами обновления цены более 18 дней назад
	df_2 = df01.loc[(df01['source'].isin(['api_ppr', 'api_rncard'])) & (df01['show_in_report'] == 't') &
					(df01['date_upd'] <= datetime.datetime.now().date() - datetime.timedelta(days=17))]
	list_off_source1 = []
	for i in df_2['station_id'].unique():
		for j in df_2[df_2['station_id'] == i]['product_id']:
			list_off_source1.append(
				('update gs__station_product set show_in_report = \'f\' where station_id = ' + str(i) +
				 ' and source_type = \'' + df_2[df_2['station_id'] == i]['source'].unique() +
				 '\' and product_id = ' + str(j) + ';')[0])
	# Запись изменений - удаление старых цен в БД
	for i in range(len(list_off_source1)):
		for j in list_off_source1[i: i + 1]:
			update_query(connection1(), j)

	# Отправляем новый запрос, обновляя df01 после отключения источников
	prices = execute_read_query1(connection1(), query_datomt)
	df01 = pd.DataFrame(prices)
	df01.rename(columns={0: 'station_id', 1: 'product_id', 2: 'product', 3: 'prod_other', 4: 'price', 5: 'source',
						 6: 'show_in_report',
						 7: 'date_upd'}, inplace=True)
	# Включение источников с ценами на текущий день, которые не идут в отчёт.
	# Формируются два ДФ и выделяется список АЗС, по которым не включены цены
	df02 = pd.DataFrame({'station_id': df01[df01['show_in_report'] == 't']['station_id'].unique()})
	df03 = pd.DataFrame({'station_id': df01[(df01['show_in_report'] == 'f') & (df01['price'] > 0) & \
											(df01['date_upd'] == datetime.datetime.now().date())][
		'station_id'].unique()})
	df04 = df03[~df03['station_id'].isin(df02['station_id'])]
	# Формируется датафрейм с полями: общее кол-во видов топлива по АЗС, источник цены и количество источников цен на АЗС
	df05 = pd.DataFrame(
		{'count_fuel': df01[df01['station_id'].isin(df04['station_id'])].groupby(['station_id', 'source', 'date_upd'])
		.size()}).reset_index()
	df005 = df05.groupby(['station_id', 'source'])['count_fuel'].sum().reset_index()
	df06 = df05[df05['date_upd'] == datetime.datetime.now().date()].reset_index()
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

	source_dict = {'api_azsopti': 1, 'api_benzuber': 2, 'api_yandex_fuel': 3, 'api_licard': 4, 'api_rncard': 5,
				   'api_ppr': 6, 'manual_manual': 7, 'manual_app': 8, 'manual_phone': 9, 'manual_ppr': 10,
				   'manual_gazprom': 11, 'manual_other': 12, 'api_yandex_other': 13}
	# Формируется ДФ с единственным источником цены для внесения изменений в БД
	df08['rate'] = df08['source'].replace(source_dict)
	try:
		df09 = df08.groupby(['station_id'], as_index=False).apply(lambda x: x.nsmallest(1, ['rate'])).reset_index(
			level=1, drop=True)
	except Exception:
		df09 = df08

	# Формируем список UPDATE для записи в БД
	list_price_not_source = []
	for k, i in enumerate(df09['station_id']):
		if ''.join(df09[df09['station_id'] == i]['source']) != 'api_ppr':
			if (df09[df09['station_id'] == i]['check'] == 'OK')[k] and \
				(df09[df09['station_id'] == i]['count_err'] == '')[k]:
				list_price_not_source.append(
					('update gs__station_product set show_in_report = \'t\' where station_id = ' +
					 str(i) + ' and source_type = \'' + df09[df09['station_id'] == i]['source'] +
					 '\' and DATE(updated_at) = CURRENT_DATE;')[k])
			elif (df09[df09['station_id'] == i]['count_err'] != '')[k]:
				for j in df01[(df01['station_id'] == i) & (df01['source'] == df09.loc[k, ['source']].values[0])][
					'product_id']:
					if check_price_product(df09.loc[k, ['source']].values[0], i, j) is not None:
						list_price_not_source.append(
							('update gs__station_product set show_in_report = \'t\' where station_id = '
							 + str(i) + ' and source_type = \'' + df09[df09['station_id'] == i]['source'] +
							 '\' and product_id = ' + str(check_price_product(df09.loc[k, ['source']].values[0], i, j))
							 + ' and DATE(updated_at) = CURRENT_DATE;')[k])
		else:
			if (df09[df09['station_id'] == i]['check'] == 'OK')[k] and \
				(df09[df09['station_id'] == i]['count_err'] == '')[
					k]:
				list_price_not_source.append(
					('update gs__station_product set show_in_report = \'t\' where station_id = '
					 + str(i) + ' and source_type = \'' + df09[df09['station_id'] == i]['source']
					 + '\' and DATE(updated_at) BETWEEN CURRENT_DATE-14 AND CURRENT_DATE;')[k])
			elif (df09[df09['station_id'] == i]['count_err'] != '')[k]:
				for j in df01[(df01['station_id'] == i) & (df01['source'] == df09.loc[k, ['source']].values[0])][
					'product_id']:
					if check_price_product(df09.loc[k, ['source']].values[0], i, j) is not None:
						list_price_not_source.append(('update gs__station_product set show_in_report = \'t\' where '
													  'station_id = ' + str(i) + ' and source_type = \'' +
													  df09[df09['station_id'] == i]['source'] + '\' and product_id = ' +
													  str(check_price_product(df09.loc[k, ['source']].values[0], i,
																			  j)) +
													  ' and DATE(updated_at) BETWEEN CURRENT_DATE-14 AND CURRENT_DATE;')[
														 k])

	# Запись изменений по источникам цен в БД
	for i in range(len(list_price_not_source)):
		for j in list_price_not_source[i: i + 1]:
			update_query(connection1(), j)

	# Включение цен, по которым уже есть источник цен, но появились новые виды топлива на текущую дату.
	# Формируются два ДФ и выделяется список АЗС, где некоторые виды топлива не включены.
	df10 = pd.DataFrame(
		{'count_fuel_true': df01[df01['show_in_report'] == 't'].groupby(['station_id', 'source']).size()}).reset_index()
	df99 = df10.groupby(['station_id'])['source'].count().reset_index().rename({'source': 'count_source'}, axis=1)
	df10 = df10.merge(df99, on='station_id', how='left')
	df10 = df10.loc[df10['count_source'] == 1, ['station_id', 'source', 'count_fuel_true', 'count_source']]

	# Формируем ДФ с источниками, которые не идут в отчёт
	df11 = pd.DataFrame({'count_fuel_false': df01[
		(df01['show_in_report'] == 'f') & (df01['price'] > 0) & (df01['date_upd'] == datetime.datetime.now().date())]
						.groupby(['station_id', 'source']).size()}).reset_index()

	# Объединяем датафреймы
	df12 = df10.merge(df11, on=['station_id', 'source'], how='inner')
	# Формирование списка кода UPDATE для БД
	list_new_product_price = []
	for k, i in enumerate(df12['station_id']):
		for j in df01[(df01['station_id'] == i) & (df01['date_upd'] == datetime.datetime.now().date()) &
					  (df01['show_in_report'] == 'f') & (df01['source'] == df12.loc[k, 'source'])]['product_id']:
			if check_price_product(df12.loc[k, ['source']].values[0], i, j) is not None and \
				check_fuel_comparison(df12.loc[k, ['source']].values[0], i, j) is None:
				list_new_product_price.append(
					('update gs__station_product set show_in_report = \'t\' where station_id = '
					 + str(i) + ' and source_type = \'' + df12[df12['station_id'] == i][
						 'source'] + '\' and product_id = ' + str(
							check_price_product(df12.loc[k, ['source']].values[0], i, j)) +
					 ' and DATE(updated_at) = CURRENT_DATE;')[k])

	# Дополнительно отключаем старые цены по источникам в df12
	df13 = df12[~df12['source'].isin(['api_ppr', 'api_rncard'])]
	list_off_product_price1 = []
	for k, i in enumerate(df13['station_id']):
		if not df01[(df01['station_id'] == i) & (df01['date_upd'] < datetime.datetime.now().date()) & (
			df01['show_in_report'] == 't') & (df01['source'] == df13[df13['station_id'] == i]['source'].values[0])][
			'product_id'].empty:
			for j in df01[(df01['station_id'] == i) & (df01['date_upd'] < datetime.datetime.now().date()) &
						  (df01['show_in_report'] == 't') & (
							  df01['source'] == df13[df13['station_id'] == i]['source'].values[0])]['product_id']:
				list_off_product_price1.append(
					('update gs__station_product set show_in_report = \'f\' where station_id = '
					 + str(i) + ' and source_type = \'' + df13[df13['station_id'] == i][
						 'source'] + '\' and product_id = ' + str(j) + ';').values[0])

	# Объединяем списки
	list_new = [*list_new_product_price, *list_off_product_price1]
	# Запись изменений по источникам цен в БД
	for i in range(len(list_new)):
		for j in list_new[i: i + 1]:
			update_query(connection1(), j)

	# Замена недостоверных источников на достоверные.
	# Формируется два ДФ для замены одних источников на другие.

	df14 = pd.DataFrame({'count_fuel_true': df01[(df01['show_in_report'] == 't') &
												 (df01['source'].isin(['api_ppr', 'api_rncard', 'api_yandex_other']))]
						.groupby(['station_id', 'source']).size()}).reset_index()
	df15 = df14.groupby(['station_id'])['source'].count().reset_index().rename({'source': 'count_source'}, axis=1)
	df16 = df14.merge(df15, on='station_id', how='left')
	df16 = df16.loc[df16['count_source'] == 1, ['station_id', 'source', 'count_source']]
	df17 = pd.DataFrame({'count_fuel_false': df01[(df01['show_in_report'] == 'f') & (df01['price'] > 0) &
												  (df01['date_upd'] == datetime.datetime.now().date()) &
												  (df01['source'].isin(['api_azsgo', 'api_benzuber', 'api_yandex_fuel',
																		'api_azsopti', 'api_licard']))]
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
	list_off_source_unreliable = []
	for k, i in enumerate(df20['station_id'].unique()):
		list_off_source_unreliable.append(
			('update gs__station_product set show_in_report = \'f\' where station_id = ' + str(i) +
			 ' and source_type = \'' + df20[df20['station_id'] == i]['source_x'].unique() + '\';')[0])
		for j in df01[(df01['station_id'] == i) & (df01['date_upd'] == datetime.datetime.now().date()) &
					  (df01['show_in_report'] == 'f') & (df01['source'] == df20.loc[k, 'source_y'])]['product_id']:
			if check_price_product(df20.loc[k, ['source_y']].values[0], i, j) is not None:
				list_off_source_unreliable.append(
					('update gs__station_product set show_in_report = \'t\' where station_id = '
					 + str(i) + ' and source_type = \'' + df20[df20['station_id'] == i]['source_y'] +
					 '\' and product_id = ' + str(j) + ' and DATE(updated_at) = CURRENT_DATE;')[k])
	# Запись изменений по источникам цен в БД
	for i in range(len(list_off_source_unreliable)):
		for j in list_off_source_unreliable[i: i + 1]:
			update_query(connection1(), j)

	# Switch off source yandex_fuel and switch on - benzuber if count fuel more then yandex_fuel
	# Формируется 2 ДФ для замены одних источников на другие
	df21 = pd.DataFrame({'count_fuel_true': df01[(df01['show_in_report'] == 't') &
												 (df01['source'].isin(['api_yandex_fuel']))].
						groupby(['station_id', 'source']).size()}).reset_index()
	df22 = pd.DataFrame(
		{'count_fuel_true': df01[(df01['show_in_report'] == 'f') & (df01['source'].isin(['api_benzuber'])) &
								 (df01['date_upd'] == datetime.datetime.now().date())]. \
		groupby(['station_id', 'source']).size()}).reset_index()
	df23 = df22.loc[df22['station_id'].isin(df21['station_id'])]
	df24 = df23.merge(df21, on='station_id', how='left')
	df25 = df24[df24['count_fuel_true_x'] >= df24['count_fuel_true_y']].reset_index().drop(['index'], axis=1)

	# Отключаем цены с yandex_fuel и включаем benzuber:
	list_yaf_benz = []
	for k, i in enumerate(df25['station_id'].unique()):
		if check_price(df25.loc[k, ['source_x']].values[0], i) == 'OK':
			list_yaf_benz.append(('update gs__station_product set show_in_report = \'t\' where station_id = ' + str(i) +
								  ' and source_type = \'' + df25[df25['station_id'] == i]['source_x'] +
								  '\' and DATE(updated_at) = CURRENT_DATE;')[k])
			list_yaf_benz.append(('update gs__station_product set show_in_report = \'f\' where station_id = ' + str(i) +
								  ' and source_type = \'' + df25[df25['station_id'] == i]['source_y'].unique() + '\';')[
									 0])
	# Запись изменений по источникам цен в БД
	for i in range(len(list_yaf_benz)):
		for j in list_yaf_benz[i: i + 1]:
			update_query(connection1(), j)

	# Отключение цен по общей границе для всех видов топлива (price <5 & >100)
	df27 = df01[((df01['price'] < 5) | (df01['price'] > 100)) & (df01['show_in_report'] == 't')] \
		[['station_id', 'product_id', 'source']].reset_index().drop(['index'], axis=1)
	list_priceoff = []
	for k, i in enumerate(df27['station_id']):
		list_priceoff.append('update gs__station_product set show_in_report = \'f\' where station_id = ' + str(i) +
							 ' and source_type = \'' + df27.loc[k, 'source'] + '\' and product_id = ' +
							 str(df27.loc[k, 'product_id']) + ' ;')

	# Запись изменений по источникам цен в БД
	for i in range(len(list_priceoff)):
		for j in list_priceoff[i: i + 1]:
			update_query(connection1(), j)


if __name__ == "__main__":
	main()