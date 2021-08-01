import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def tinkoff_file_parse(path, db_engine, user_id):
    df = pd.read_csv(path, sep=';', parse_dates=[0,1], dayfirst=True, decimal=",", encoding='cp1251')
    df = df[df['Статус']=='OK'][[
        'Дата операции',
        'Сумма платежа',
        'Категория',
        'Описание'
    ]]

    df_for_sql = df.reindex(index=df.index[::-1]).reset_index(drop=True)
    df_for_sql.columns = ['date', 'amount', 'category', 'description']#, 'balance']
    

    for k,v in db_engine.download_c_rules(user_id=user_id):
        df_for_sql.loc[df_for_sql['description'] == k,'category']=v
        
    return df_for_sql

class DB_Engine:
    def __init__(self, ip, port, user, password, db_name, schema):
        self.connector = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{db_name}')
        self.schema = schema

    def download_c_rules(self, user_id, table='dictionary_categories'):
        return pd.read_sql(f'SELECT key, value FROM {self.schema}.{table} WHERE user_id = {user_id}', self.connector).values.tolist()

    def download_regular(self, user_id, table='regular'):
        return pd.read_sql(f'SELECT description, search_f, arg_sf, amount, start_date, end_date, d_years, d_months, d_days, adjust_price, adjust_date FROM {self.schema}.{table} WHERE user_id = {user_id}', self.connector)

    def download_costs(self, user_id, table='costs'):
        return pd.read_sql(f'SELECT date, amount, category, description, balance FROM {self.schema}.{table} WHERE user_id = {user_id} ORDER BY date', self.connector)
    
    def add_costs(self, data, user_id, table='costs'):
        data = data[['date', 'amount', 'category', 'description', 'balance']].copy().sort_values('date')
        data['user_id'] = [user_id]*len(data)
        data.to_sql(table, self.connector, schema='icyb', if_exists='append', index=False)
    
