#!/usr/bin/env python
# coding: utf-8

# In[1]:

# Подгружаем либы

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import locale


# In[2]:

# Настраиваем подключения к БД

import psycopg2 as pc2
from psycopg2 import Error
from psycopg2 import sql

#Личные
from db_connect import db_connect
from db_connect import query
from db_connect import single_insert
from db_connect import db_engine

from Catalog import create_path
from Transactions import transaction


# Получаем пути к загруженным файлам

# In[3]:


global_path = 'C:\\Для BI отчетности\\Reports_bi\\Файлы_xlsx\\'
dp = create_path()


# In[ ]:


def write_log(path):
    lst = list()
    lst.append(path)
    d = pd.DataFrame(lst)
    d.to_csv(global_path + 'file_logs.csv',  mode='a', header=False, index=False)   


# In[ ]:


if dp:
    # Генерируем датасет для загрузки в базу данных
    df_90 = transaction(global_path + dp['path_90'])

    # Загружаем датасет в базу данных
    if len(df_90) > 0:
        sql_insert="""INSERT INTO revenue (id,"Date", rev_opt, rev_transport) VALUES (%s, %s, %s, %s)"""
        single_insert(sql_insert, df_90)
        write_log(dp['path_90'])
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    df_44 = transaction(global_path + dp['path_44'])

    # Загружаем датасет в базу данных
    if len(df_44) > 0:
        conn_engine = db_engine()
        df_44.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
        write_log(dp['path_44'])
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    df_10 = transaction(global_path + dp['path_10'])

    # Загружаем датасет в базу данных
    if len(df_10) > 0:
        conn_engine = db_engine()
        df_10.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
        write_log(dp['path_10'])
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    df_41 = transaction(global_path + dp['path_41'])

    # Загружаем датасет в базу данных
    if len(df_41) > 0:
        conn_engine = db_engine()
        df_41.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
        write_log(dp['path_41'])
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    df_91 = transaction(global_path + dp['path_91'])

    # Загружаем датасет в базу данных
    if len(df_91) > 0:
        conn_engine = db_engine()
        df_91.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
        write_log(dp['path_91'])
    else:
        pass


else:
    pass
