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

from Catalog import check_files
from Transactions import transaction


# Получаем пути к загруженным файлам

# In[3]:


global_path = 'C:\\Для BI отчетности\\Reports_bi\\Файлы_xlsx\\'
dp = check_files()


# In[ ]:


def write_log(path):
    lst = list()
    lst.append(path)
    d = pd.DataFrame(lst)
    d.to_csv(global_path + 'file_logs.csv',  mode='a', header=False, index=False)   


# In[ ]:


if dp:

    # Генерируем датасет для загрузки в базу данных
    if dp:
        for i in dp: 
            if '62' in i:
                df_62 = transaction(global_path + i)

                # Загружаем датасет в базу данных
                if len(df_62) > 0:
                    sql_insert="""INSERT INTO revenue (id,"Date", rev_opt, rev_transport) VALUES (%s, %s, %s, %s)"""
                    single_insert(sql_insert, df_62)
                    write_log(i)
                else:
                    pass
            else:
                continue
    else:
        pass
        
    # Генерируем датасет для загрузки в базу данных
    if dp:
        for i in dp: 
            if '90.01' in i:
                df_90 = transaction(global_path + i)

                # Загружаем датасет в базу данных
                if len(df_90) > 0:
                    sql_insert="""INSERT INTO revenue (id,"Date", rev_opt, rev_transport) VALUES (%s, %s, %s, %s)"""
                    single_insert(sql_insert, df_90)
                    write_log(i)
                else:
                    pass
            else:
                continue
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    if dp:
        for i in dp: 
            if '44.01' in i:
                df_44 = transaction(global_path + i)

                # Загружаем датасет в базу данных
                if len(df_44) > 0:
                    conn_engine = db_engine()
                    df_44.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
                    write_log(i)
                else:
                    pass
            else:
                continue
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    if dp:
        for i in dp: 
            if '10' in i:
                df_10 = transaction(global_path + i)

                # Загружаем датасет в базу данных
                if len(df_10) > 0:
                    conn_engine = db_engine()
                    df_10.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
                    write_log(i)
                else:
                    pass
            else:
                continue
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    if dp:
        for i in dp: 
            if '41.01' in i:
                df_41 = transaction(global_path + i)

                # Загружаем датасет в базу данных
                if len(df_41) > 0:
                    conn_engine = db_engine()
                    df_41.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
                    write_log(i)
                else:
                    pass
            else:
                continue
    else:
        pass

    # Генерируем датасет для загрузки в базу данных
    if dp:
        for i in dp: 
            if '91.02' in i:
                df_91 = transaction(global_path + i)

                # Загружаем датасет в базу данных
                if len(df_91) > 0:
                    conn_engine = db_engine()
                    df_91.to_sql('transactions', conn_engine, if_exists="append", chunksize=100, index=False)
                    write_log(i)
                else:
                    pass
            else:
                continue
    else:
        pass
