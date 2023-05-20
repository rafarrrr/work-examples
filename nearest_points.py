import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
import mysql.connector
from mysql.connector import Error
from psycopg2 import OperationalError
import psycopg2
from connection_db import connection
from connection_db import connection1
import os
import openpyxl


# def create_connection(host_name, user_name, user_password, db_name):  # Создание подключения к БД Паркон
# 	connection = None
# 	try:
# 		connection = mysql.connector.connect(
# 			host=host_name,
# 			user=user_name,
# 			passwd=user_password,
# 			database=db_name
# 		)
# 		print("Connection to MySQL DB successful")
# 	except Error as e:
# 		print(f"The error '{e}' occurred")
# 	return connection


def execute_read_query(connection, query):  # Создание запроса
	""" Получаем запрос и возвращаем результат запроса из Паркона """
	cursor = connection.cursor()
	result = None
	try:
		cursor.execute(query)
		result = cursor.fetchall()
		return result
	except Error as e:
		print(f"The error '{e}' occurred")


# def create_connection1(db_name, db_user, db_password, db_host, db_port): # Создание подключения к Datomt
# 	connection = None
# 	try:
# 		connection = psycopg2.connect(
#             database=db_name,
#             user=db_user,
#             password=db_password,
#             host=db_host,
#             port=db_port,
#         )
# 		print("Connection to PostgreSQL DB successful")
# 	except OperationalError as e:
# 		print(f"The error '{e}' occurred")
# 	return connection


def execute_read_query1(connection, query): # Запрос к datomt
	""" Получаем запрос и возвращаем результат запроса из ДатОМТ """
	cursor = connection.cursor()
	result = None
	try:
		cursor.execute(query)
		result = cursor.fetchall()
		return result
	except OperationalError as e:
		print(f"The error '{e}' occurred")


earth_radius = 6371

# Формирование списка точек с координатами для поиска расстояний между ними. Информация в колонках
# - id, широта, долгота.
query_point = """
	SELECT gas_station.gs_id, TRIM(SUBSTRING_INDEX(address.gps_var, ",", 1)) as lat, 
	TRIM(SUBSTRING_INDEX(address.gps_var, ",", -1)) as lon
FROM gas_station
LEFT JOIN address ON gas_station.gs_id = address.gs_id
WHERE gas_station.status_gs IN('Действующая', 'Реконструкция', 'Ведомственная')
AND TRIM(SUBSTRING_INDEX(address.gps_var, ",", 1)) IS NOT NULL 
AND TRIM(SUBSTRING_INDEX(address.gps_var, ",", -1)) != ""
"""
points = execute_read_query(connection(), query_point)

# Старый блок - Загружаем файл с координатами, информация в колонках - id, широта, долгота.
# with open('C:\\!Равшан_работа\\OMT Consult\\Концепт\\coordinates.csv', 'r', encoding='utf-8-sig') as w:
# 	a = []
# 	for line in w:
# 		a.append(line.strip().split(';'))

# Список конвертируем в массив Numpy
g = np.array(points)
b = g[:, 0]
# преобразуем данные массива в вещественный тип
g = np.array(g[:, [1, 2]], dtype='<f8')
# Координаты находятся в 1 и 2 колонке и далее переводим значения в радианы
array_coordinates = g[::, [0, 1]]
array_coordinates_radian = np.radians(array_coordinates)
# В отдельный список сохраняем id АЗС
list_id_azs = b.tolist()
# Блок определения ближайших точек, параметр "n_neighbors=" определяет количество соседей, включая себя.
# Или ввод через консоль количества ближайших АЗС
n_azs = int(input('Введите количество АЗС для поиска: '))
nbrs = NearestNeighbors(n_neighbors=n_azs, algorithm='ball_tree', metric='haversine').fit(array_coordinates_radian)
distances, indices = nbrs.kneighbors(array_coordinates_radian)
distances = np.array(distances) * earth_radius

# Блок формирования файла с расстоянием между точками
df = pd.DataFrame(indices)
df = df.iloc[:, 0:df.shape[1]]  # Чтобы искомая точка не была в списке конкурентов нужно использовать диапазон df.iloc[:, 1:df.shape[1]]
df = df.stack().reset_index()
df = df.iloc[:, 0:3:2]
df.rename(columns={df.columns[0]: "point1", df.columns[1]: "point2"}, inplace=True)
df1 = pd.DataFrame(distances)
df1 = df1.iloc[:, 0:df1.shape[1]]  # Чтобы искомая точка не была в списке конкурентов нужно использовать диапазон df1.iloc[:, 1:df1.shape[1]]
df1 = df1.stack().reset_index()
df1 = df1.iloc[:, 0:3:2]
df1.rename(columns={df1.columns[0]: "point1", df1.columns[1]: "dist"}, inplace=True)
df['dist'] = df1.iloc[:, 1]
df5 = pd.DataFrame(list_id_azs).reset_index()
df5.rename(columns={df5.columns[0]: 'col1'}, inplace=True)
df6 = df.merge(df5, left_on='point1', right_on='col1', how='left')
df6 = df6.merge(df5, left_on='point2', right_on='col1', how='left')
df9 = df6.iloc[:, [4, 6, 2]]
df9 = df9.rename(columns={df9.columns[0]: "point1", df9.columns[1]: "point2", df9.columns[2]: "dist, km"})
df9['point1'] = pd.to_numeric(df9['point1'])
df9['point2'] = pd.to_numeric(df9['point2'])

# Запрос к Паркону для формирования справочника АЗС
query = """
SELECT t1.gs_id as 'id_Parcon', t1.owner_type as Принадлежность, t1.type as Тип, CASE WHEN t1.type IN('АЗС', 'АЗК') 
THEN 1 WHEN t1.type IN('АГЗС', 'АГНКС', 'МАГЗС', 'КриоАЗС') THEN 2 WHEN t1.type IN('МАЗК', 'МАЗС') THEN 3 ELSE 0 END AS type_1, 
t1.brand AS Бренд, t1.affilation AS Аффилированность, t1.owner_number as 'Идентификатор АЗС', t1.status_gs as 'Статус 
АЗС', t1.region_name as 'Субъект РФ', CASE WHEN RIGHT(t1.address,1) = ',' THEN LEFT(t1.address, 
CHAR_LENGTH(t1.address)-1) ELSE t1.address END AS address, t1.gps_var AS Координаты 
FROM (
	SELECT
	gas_station.gs_id,
	gas_station.owner_type,
	gas_station.type,
	br.name as brand,
	br2.name as affilation,
	gas_station.owner_number,
	gas_station.status_gs, 
	region.region_name, 
	TRIM(CONCAT(CASE 
		WHEN address.road_number IS NULL OR address.road_number='' THEN '' ELSE CONCAT(address.road_number, ", ") END,
	CASE 
		WHEN address.road_km IS NULL THEN '' ELSE CONCAT(ROUND(address.road_km, 0), " км, ") END,
	CASE 
		WHEN address.side IS NULL OR address.side='Нет_данных' OR address.side='' THEN '' ELSE CONCAT(address.side, ", ") END,
	CASE 
		WHEN address.city IS NULL OR address.city='' THEN '' ELSE CONCAT(address.city, ", ") END,
	CASE 
		WHEN address.locality IS NULL OR address.locality='' THEN '' ELSE CONCAT(address.locality, ", ") END,
	CASE 
		WHEN address.street IS NULL OR address.street='' THEN '' ELSE CONCAT(address.street, ", ") END,
	CASE 
		WHEN address.building IS NULL OR address.building='' THEN '' ELSE address.building END
	)) AS address,
	address.gps_var
	FROM gas_station
	LEFT JOIN brand br ON gas_station.brand_id = br.id
	LEFT JOIN brand br2 ON gas_station.affiliation_id = br2.id
	LEFT JOIN region ON gas_station.region_id = region.id
	LEFT JOIN address ON gas_station.gs_id = address.gs_id
	WHERE gas_station.status_gs IN('Действующая', 'Реконструкция', 'Ведомственная')) AS t1
"""
res = execute_read_query(connection(), query)
df002 = pd.DataFrame(res)
df002 = df002.rename(columns={df002.columns[0]: "id_parcon", df002.columns[1]: "type_1", df002.columns[2]: "type_azs",
							  df002.columns[3]: "check", df002.columns[4]: "brand", df002.columns[5]: "affilation",
							  df002.columns[6]: "number", df002.columns[7]: "status", df002.columns[8]: "region",
							  df002.columns[9]: "address", df002.columns[10]: "gps"})
df009 = df9.merge(df002, left_on='point2', right_on='id_parcon', how='left')
df008 = df009.merge(df002, left_on='point1', right_on='id_parcon', how='left')
df008 = df008.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 17, 18, 22]]
df008 = df008[['point1', 'brand_y', 'region_y', 'type_azs_y', 'check_y', 'point2', 'dist, km', 'id_parcon_x', 'type_1_x',
			   'type_azs_x','check_x', 'brand_x', 'affilation_x', 'number_x', 'status_x', 'region_x', 'address_x',
			   'gps_x']]
# В данном блоке происходит разгруппировка по типу АЗС, чтобы АГЗС не отображались в конкурентах у АЗС
df007 = df008[
	(((df008['check_y'] == 1) & (df008['check_x'] == 1)) | ((df008['check_y'] == 1) & (df008['check_x'] == 3))) |
	(((df008['check_y'] == 2) & (df008['check_x'] == 2)) | ((df008['check_y'] == 2) & (df008['check_x'] == 3))) |
	(((df008['check_y'] == 3) & (df008['check_x'] == 3)) | ((df008['check_y'] == 3) & (df008['check_x'] == 1)) |
	 ((df008['check_y'] == 3) & (df008['check_x'] == 2)))]
df007['sequence'] = df007.groupby('point1').cumcount()
df007 = df007[df007['sequence'] < n_azs]
columns = df007.columns.to_list()
columns1 = [_.replace('_x', '') for _ in columns]
df007.columns = columns1

# Запрос к datomt для формирования актуальных цен по АЗС
query_datomt = """
SELECT GS.parcon_id, GSP.source_type, case WHEN GSP.source_type IN('api_yandex_fuel', 'api_azsgo', 'api_benzuber', 
'api_azsopti', 'api_licard') THEN 1 WHEN GSP.source_type IN('api_yandex_other', 'api_rncard', 'api_ppr') THEN 2 ELSE 0 
END AS Тип_ист
FROM gs__station AS GS
LEFT JOIN gs__station_product AS GSP ON GSP.station_id = GS.id
LEFT JOIN directory__region AS DR ON GS.region_id = DR.id
WHERE GSP.source_type IN('api_yandex_fuel', 'api_azsgo', 'api_benzuber', 'api_azsopti', 'api_licard', 'api_yandex_other', 
'api_rncard', 'manual_app', 'manual_gazprom', 'manual_gpn', 'manual_manual', 'manual_other', 'manual_phone', 
'manual_ppr', 'api_ppr')
AND DATE(GSP.updated_at) = CURRENT_DATE
GROUP BY GS.parcon_id, GSP.source_type
"""
prices = execute_read_query1(connection1(), query_datomt)

df01 = pd.DataFrame(prices)
df01.rename(columns={0: 'parcon_id', 1: 'source', 2: 'type_source'}, inplace=True)
df02 = df01.groupby(['parcon_id', 'type_source']).count().reset_index().iloc[:, 0:3].\
	sort_values(by=['parcon_id', 'type_source'])
df02['sequence'] = df02.groupby('parcon_id').cumcount()
# Тип источника: 0 - ручной мониторинг; 1 - автоматические, 2 - прочие

df03 = df02[df02['sequence'] == 0]
df007 = df007.merge(df03, left_on='id_parcon', right_on='parcon_id', how='left')
df007.drop(df007.columns[[3, 4, 5, 18, 19, 21, 22]], axis=1, inplace=True)
df007.rename(columns={'brand_y': 'brand_client', 'region_y': 'region_client'}, inplace=True)

df007.to_excel(os.path.join('..', 'data_file', 'nearest_competitors.xlsx'), index=False, encoding='utf-8')