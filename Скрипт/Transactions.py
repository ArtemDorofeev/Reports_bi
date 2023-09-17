#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Подгружаем либы

import pandas as pd
import numpy as np
import csv
import sys
from datetime import datetime
import locale


# In[2]:


#Связи

from db_connect import db_connect
from db_connect import query
from db_connect import single_insert
from db_connect import db_engine


# In[3]:


def transaction_10(path):
    # Читаем файл excel с выручкой из 1С
    df = pd.read_excel(path)
    
    # Актуализируем информацию из БД по транзакциям
    sql_query_tr="""select * from transactions"""
    db_tr = query(sql_query_tr)
    start_index = max(db_tr[0])+1
    
    # Актуализируем информацию из БД по категориям
    sql_query_cat="""select * from categories"""
    db_cat = query(sql_query_cat)

    # Преобразуем категории в словарь {категория: позиция}
    category_report = dict(zip(db_cat[1], db_cat[0]))
    
    # Вытаскиваем из названия файла отчетную дату

    locale.setlocale(locale.LC_ALL, '')
    current_date = datetime.now()
    current_year = current_date.year

    if 'ПРОЖБИ ИНЖИНИРИНГ' in path:
        report_month = df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0][df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                     .find('счету 10 за')+12:df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                     .find(str(current_year))-1]
    elif 'СТРОЙЛОГИСТИКА' in path:
        report_month = df['ООО "СТРОЙЛОГИСТИКА"'][0][df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                     .find('счету 10 за')+12:df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                     .find(str(current_year))-1]
    
    date_str = f'01 {report_month} {current_year}'
    forma = '%d %B %Y'
    rep_date = datetime.strptime(date_str, forma).date()
    
    # Выбираем целевые значения таблицы
    df.columns = list(df.iloc[2].values)
    df = df.iloc[6:len(df)-2,[0,4,5]]
    df = df[df['Счет'].notna()][~df[df['Счет'].notna()]['Счет'].str.contains('10.0|10.1')]
    df = df.set_index(pd.Index(range(start_index, start_index+len(df)))).reset_index()
    
    # Формируем таблицу под структуру БД
    df['Дата'] = rep_date
    df['id_account'] = 1
    df['id_category'] = category_report['Затраты на офис']
    df = df[['index', 'Дата', 'Счет', 'id_account', 'Обороты за период', 'id_category']]
    df.columns = ['id', "Date", 'category', 'id_account', 'amount', 'id_category']
    
    return df


# In[ ]:


def transaction_41(path):
    # Читаем файл excel с выручкой из 1С
    df = pd.read_excel(path)
    
    # Актуализируем информацию из БД по транзакциям
    sql_query_tr="""select * from transactions"""
    db_tr = query(sql_query_tr)
    start_index = max(db_tr[0])+1
    
    # Актуализируем информацию из БД по категориям
    sql_query_cat="""select * from categories"""
    db_cat = query(sql_query_cat)

    # Преобразуем категории в словарь {категория: позиция}
    category_report = dict(zip(db_cat[1], db_cat[0]))
    
    # Вытаскиваем из названия файла отчетную дату

    locale.setlocale(locale.LC_ALL, '')
    current_date = datetime.now()
    current_year = current_date.year

    if 'ПРОЖБИ ИНЖИНИРИНГ' in path:
        report_month = df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0][df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                         .find('41.01 за')+9:df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                         .find(str(current_year))-1]
    elif 'СТРОЙЛОГИСТИКА' in path:    
        report_month = df['ООО "СТРОЙЛОГИСТИКА"'][0][df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                         .find('41.01 за')+9:df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                         .find(str(current_year))-1]
        
    date_str = f'01 {report_month} {current_year}'
    forma = '%d %B %Y'
    rep_date = datetime.strptime(date_str, forma).date()
    
    # Выбираем целевые значения таблицы
    df.columns = list(df.iloc[2].values)
    df = df.iloc[5:6,[0,4,5]]
    df = df.set_index(pd.Index(range(start_index, start_index+len(df)))).reset_index()
    
    # Формируем таблицу под структуру БД

    df['Дата'] = rep_date
    df['id_account'] = 2
    df['id_category'] = category_report['Прямые расходы (себестоимость)']
    df['category'] = 'Товары'
    df = df[['index', 'Дата', 'category', 'id_account', 'Обороты за период', 'id_category']]
    df.columns = ['id', "Date", 'category', 'id_account', 'amount', 'id_category']
    
    return df


# In[ ]:


def transaction_44(path):
    # Читаем файл excel с выручкой из 1С
    df = pd.read_excel(path)
    
    # Актуализируем информацию из БД по транзакциям
    sql_query_tr="""select * from transactions"""
    db_tr = query(sql_query_tr)
    start_index = max(db_tr[0])+1
    
    # Актуализируем информацию из БД по категориям
    sql_query_cat="""select * from categories"""
    db_cat = query(sql_query_cat)

    # Преобразуем категории 1c в словарь {категория: позиция}
    category_1c = dict(zip(db_cat[1], db_cat[0]))
    
    # Преобразуем категории БД в словарь {категория: позиция}
    category_report = dict(zip(db_tr[2], db_tr[5]))
    
    # Формируем шапку таблицы, убираем строки в голове
    df.columns = list(df.iloc[4].values)
    df = df.iloc[7:len(df)-2,[0,2,3,5]]
    df = df.set_index(pd.Index(range(start_index, start_index+len(df)))).reset_index()
    
    def category(row):
        part = row[3].split('\n')[0]   
        for i in category_report:
            if part in i:
                x = category_report[i]
                break               
            else:
                try:
                    x = category_1c[row[2].strip()]
                except:
                    x = category_1c['Другие расходы']
        return x
    
    # Формируем таблицу под структуру БД

    df['Дата'] = df['Период'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
    df['id_account'] = 4    
    df['id_category'] = df.apply(category, axis=1)
    df.columns = ['id', "Date", 'category_group', 'category', 'amount', 'Дата', 'id_account', 'id_category']
    df = df[['id', 'Date', 'category', 'id_account', 'amount', 'id_category']]  
    
    return df


# In[ ]:


def transaction_90(path):
        
    # Читаем файл excel с выручкой из 1С
    df = pd.read_excel(path)
    
    # Вытаскиваем из названия файла отчетную дату

    locale.setlocale(locale.LC_ALL, '')
    current_date = datetime.now()
    current_year = current_date.year
    
    if 'ПРОЖБИ ИНЖИНИРИНГ' in path:        
        report_month = df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0][df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                         .find('90.01.1 за')+11:df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                         .find(str(current_year))-1]
    elif 'СТРОЙЛОГИСТИКА' in path:
        report_month = df['ООО "СТРОЙЛОГИСТИКА"'][0][df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                         .find('90.01.1 за')+11:df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                         .find(str(current_year))-1]
    
    date_str = f'01 {report_month} {current_year}'
    forma = '%d %B %Y'
    rep_date = datetime.strptime(date_str, forma).date()
    
    # Выбираем нужные данные из файла, столбцы и строки

    df.columns = [df.iloc[4].values]
    df = df[df.iloc[:,0] == '90.01.1'].iloc[:,[0,4]]
    df.columns = ['Счет', 'Выручка']
    
    # Актуализируем информацию из БД

    sql_query="""select * from revenue"""
    db = query(sql_query)
    
    # Формируем данные для загрузки в БД

    start_index = max(db[0])+1
    revenue = df['Выручка'].values[0]
    rev_transport = 0
    values_insert = (start_index, rep_date, revenue, rev_transport)
    values_update = (revenue, rev_transport, rep_date)
    
    return values_insert


# In[ ]:


def transaction_91(path):
    # Читаем файл excel с выручкой из 1С
    df = pd.read_excel(path)
    
    # Актуализируем информацию из БД по транзакциям
    sql_query_tr="""select * from transactions"""
    db_tr = query(sql_query_tr)
    start_index = max(db_tr[0])+1
    
    # Актуализируем информацию из БД по категориям
    sql_query_cat="""select * from categories"""
    db_cat = query(sql_query_cat)

    # Преобразуем категории в словарь {категория: позиция}
    category_1с = dict(zip(db_cat[1], db_cat[0]))
    
    # Преобразуем категории БД в словарь {категория: позиция}
    category_report = dict(zip(db_tr[2], db_tr[5]))
    
    # Вытаскиваем из названия файла отчетную дату

    locale.setlocale(locale.LC_ALL, '')
    current_date = datetime.now()
    current_year = current_date.year

    if 'ПРОЖБИ ИНЖИНИРИНГ' in path:
        report_month = df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0][df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                     .find('91.02 за')+9:df['ООО "ПРОЖБИ ИНЖИНИРИНГ"'][0]\
                                     .find(str(current_year))-1]
    elif 'СТРОЙЛОГИСТИКА' in path:
        report_month = df['ООО "СТРОЙЛОГИСТИКА"'][0][df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                     .find('91.02 за')+9:df['ООО "СТРОЙЛОГИСТИКА"'][0]\
                                     .find(str(current_year))-1]
        
    date_str = f'01 {report_month} {current_year}'
    forma = '%d %B %Y'
    rep_date = datetime.strptime(date_str, forma).date()
    
    # Выбираем целевые значения таблицы
    df.columns = list(df.iloc[2].values)
    df = df.iloc[7:len(df)-2,[0,4,5]]
    df = df[df['Счет'].notna()][~df[df['Счет'].notna()]['Счет'].str.contains('Прочие|Расходы на услуги')]
    df = df.set_index(pd.Index(range(start_index, start_index+len(df)))).reset_index()
    
    def category(row):          
        for i in category_report:
            if row[1] in i:
                x = category_report[i]
                break
            else:
                x = category_1с['Другие расходы']
        return x
    
    # Формируем таблицу под структуру БД
    df['Дата'] = rep_date
    df['id_account'] = 3
    df['id_category'] = df.apply(category, axis=1)
    df = df[['index', 'Дата', 'Счет', 'id_account', 'Обороты за период', 'id_category']]
    df.columns = ['id', "Date", 'category', 'id_account', 'amount', 'id_category']
    
    return df


# In[ ]:


def transaction(path):
    if '10' in path:
        df = transaction_10(path)
    elif '41.01' in path:
        df =transaction_41(path)
    elif '44.01' in path:
        df = transaction_44(path)
    elif '90.01' in path:
        df = transaction_90(path)
    elif '91.02' in path:
        df = transaction_91(path)
    else:
        df = pd.DataFrame()
    return df


# In[ ]:





# In[ ]:




