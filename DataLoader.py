import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pickle


def tinkoff_file_parse(path, db_engine, user_id):
    df = pd.read_csv(path, sep=';', parse_dates=[
                     0, 1], dayfirst=True, decimal=",", encoding='cp1251')
    df = df[df['Статус'] == 'OK'][[
        'Дата операции',
        'Сумма платежа',
        'Категория',
        'Описание'
    ]]

    df_for_sql = df.reindex(index=df.index[::-1]).reset_index(drop=True)
    df_for_sql.columns = ['date', 'amount',
                          'category', 'description']  # , 'balance']

    for k, v in db_engine.download_c_rules(user_id=user_id):
        df_for_sql.loc[df_for_sql['description'] == k, 'category'] = v

    return df_for_sql


class DB_Engine:
    def __init__(self, ip, port, user, password, db_name, schema):
        self.connector = create_engine(
            f'postgresql://{user}:{password}@{ip}:{port}/{db_name}')
        self.schema = schema

    def download_c_rules(self, user_id, table='dictionary_categories'):
        return pd.read_sql(f'SELECT key, value FROM {self.schema}.{table} WHERE user_id = {user_id}', self.connector).values.tolist()

    def download_regular(self, user_id, table='regular'):
        return pd.read_sql(f'SELECT description, search_f, arg_sf, amount, start_date, end_date, d_years, d_months, d_days, adjust_price, adjust_date, follow_overdue FROM {self.schema}.{table} WHERE user_id = {user_id}', self.connector)

    def download_onetime(self, user_id, table='onetime'):
        data = pd.read_sql(
            f'SELECT date, description, amount FROM {self.schema}.{table} WHERE user_id = {user_id}', self.connector)
        if data.empty:
            data = pd.DataFrame([], columns=['date', 'description', 'amount'])
        else:
            data['date'] = pd.to_datetime(data['date'])
        data = data.set_index('date')
        return data

    def download_transactions(self, user_id, table='transactions'):
        return pd.read_sql(f'SELECT date, amount, category, description, balance FROM {self.schema}.{table} WHERE user_id = {user_id} AND is_del = False ORDER BY date', self.connector)

    def add_transactions(self, data, user_id, table='transactions'):
        data = data[['date', 'amount', 'category',
                     'description', 'balance']].copy().sort_values('date')
        data['user_id'] = user_id
        data['is_del'] = False

        data.to_sql(table, self.connector, schema=self.schema,
                    if_exists='append', index=False)

    def download_last_model(self, user_id, table='sbs_models'):
        df = pd.read_sql(
            f'SELECT dump FROM {self.schema}.{table} WHERE user_id = {user_id} ORDER BY id DESC LIMIT 1', self.connector)
        if df.empty:
            return None

        return pickle.loads(
            df.loc[0, 'dump']
        )

    def upload_model(self, user_id, model, table='sbs_models'):
        pd.DataFrame([[
            user_id,
            pickle.dumps(model)
        ]], columns=[
            'user_id',
            'dump'
        ]).to_sql(table, self.connector, schema=self.schema, if_exists='append', index=False)
