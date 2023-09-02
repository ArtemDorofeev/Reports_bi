#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Импортируем библы
import pandas as pd
#База данных
import psycopg2 as pc2
from psycopg2 import Error
from psycopg2 import sql
import sqlalchemy
from sqlalchemy import create_engine


# In[11]:


def db_connect():
    """ Connect to the PostgreSQL database server """
    param_connect = {
        "host"      : "localhost",
        "database"  : "postgres",
        "user"      : "postgres",
        "password"  : "F6XK24twa6"
    }
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = pc2.connect(**param_connect)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    return conn


# In[30]:


def query(sql_query, values=None, conn=None):
    """ Execute a QUERY request """
    try:
        result=None
        conn=db_connect()
        cursor=conn.cursor()    
        cursor.execute(sql_query,values)        
        result=pd.DataFrame(cursor)
        conn.commit()
        return result
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        cursor.close()
        conn.close()
        print("Соединение с PostgreSQL закрыто")
        
        
def single_insert(sql_query, values=None, conn=None):
    """ Execute a QUERY request """
    try:        
        conn=db_connect()
        cursor=conn.cursor()    
        cursor.execute(sql_query,values)        
        conn.commit()        
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        cursor.close()
        conn.close()
        print("Соединение с PostgreSQL закрыто")

def db_engine():
    """ Connect to the PostgreSQL database server """
    conn_engine = None
    try:
        # connect to the PostgreSQL server
        conn_engine = create_engine('postgresql://postgres:F6XK24twa6@localhost:5432/postgres')
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    return conn_engine

