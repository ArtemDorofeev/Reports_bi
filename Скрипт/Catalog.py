#!/usr/bin/env python
# coding: utf-8

# In[68]:


import os
import sys
import csv
import pandas as pd


# In[69]:


# Выносим списки файлов в датасеты

def check_files():
    # Список файлов предыдущей загрузки
    logs = pd.read_csv('C:\\Для BI отчетности\\Файлы_xlsx\\file_logs.csv')
    
    # Список файлов корневой папки в текущий момент
    content = os.listdir('C:\\Для BI отчетности\\Файлы_xlsx\\')
    files = [i for i in content if i.endswith('.xlsx')]
    df = pd.DataFrame(files)
    
    # Сравниваем списки файлов на наличие обновления
    files_list = list(set(df[0]).difference(set(logs['0'])))
           
    return files_list


# In[70]:


# Генерируем пути для добавленных файлов

def create_path():
    list_files = check_files()
    dic = {}
    if list_files:
        accounts = ['10', '41', '44', '90', '91']        
        for i in accounts:
            for j in list_files:
                if i in j:
                    dic[f'path_{i}'] = j
                    break
                else:
                    dic[f'path_{i}'] = ''
    else:
        dic = {}    
    return dic
        


# In[75]:


# Обновляем список загруженных файлов
def write_file_list():
    content = os.listdir('C:\\Для BI отчетности\\Файлы_xlsx\\')
    files = [i for i in content if i.endswith('.xlsx')]
    df = pd.DataFrame(files)
    df.to_csv('C:\\Для BI отчетности\\Файлы_xlsx\\file_logs.csv', index=False)

