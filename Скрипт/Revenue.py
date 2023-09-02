#!/usr/bin/env python
# coding: utf-8

# In[123]:


# Подгружаем либы

import pandas as pd
import numpy as np
import csv
import sys
from datetime import datetime
import locale


# In[124]:


# Настраиваем подключения к БД

import psycopg2 as pc2
from psycopg2 import Error
from psycopg2 import sql

#Личные
from db_connect import db_connect
from db_connect import query
from db_connect import single_insert


# In[125]:


file_path = 'C:\\Для BI отчетности\\Reports_bi\\Файлы_xlsx\\Оборотно-сальдовая ведомость по счету 90.01.1 за Март 2023 г. ООО  ПРОЖБИ ИНЖИНИРИНГ.xlsx'


# In[126]:


# Читаем файл excel с выручкой из 1С

df = pd.read_excel(file_path)


# In[127]:


# Вытаскиваем из названия файла отчетную дату

locale.setlocale(locale.LC_ALL, '')

current_date = datetime.now()
current_year = current_date.year

report_month = df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0][df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                 .find('90.01.1 за')+11:df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                 .find(str(current_year))-1]

date_str = f'01 {report_month} {current_year}'
format = '%d %B %Y'
rep_date = datetime.strptime(date_str, format).date()


# In[128]:


# Выбираем нужные данные из файла, столбцы и строки

df.columns = [df.iloc[4].values]
df = df.iloc[8:10,[0,4]]
df.columns = ['Счет', 'Выручка']


# In[129]:


# Актуализируем информацию из БД

sql_query="""select * from revenue"""

db = query(sql_query)


# In[131]:


# Формируем данные для загрузки в БД

start_index = max(db[0])+1
rev_opt = df['Выручка'][8]
rev_transport = df['Выручка'][9]
values_insert = (start_index, rep_date, rev_opt, rev_transport)
values_update = (rev_opt, rev_transport, rep_date)


# In[132]:


# Если дата новая, загружаем данные в БД, если старая - обновляем значения в БД

sql_insert="""INSERT INTO revenue (id,"Date", rev_opt, rev_transport) VALUES (%s, %s, %s, %s)"""
sql_update="""UPDATE revenue SET rev_opt = %s, rev_transport = %s WHERE "Date" = %s"""

if db[1][start_index-1] != rep_date:
    single_insert(sql_insert, values_insert)
else:
    single_insert(sql_update, values_update)

