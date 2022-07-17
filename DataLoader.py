import numpy as np
import pandas as pd
from sqlalchemy import create_engine, sql
import pickle
import re
from datetime import date, datetime


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
    df_for_sql.columns = ['date', 'amount', 'category', 'description']

    for k, v in db_engine.download_c_rules(user_id=user_id):
        df_for_sql.loc[df_for_sql['description'] == k, 'category'] = v

    return df_for_sql


def amount_parser(string):
    sep = (" ", "`", "'")
    result = re.search("-?(\d{1,3}[ `'])*\d+([\.\,]\d+)?", string).group(0)
    result = result.replace(',', '.')
    for s in sep:
        result = result.replace(s, '')

    return float(result)


def ru_datetime_parser(string):
    l = len(string)
    string = string.replace(',', '.')
    dotC = string.count('.')
    сolonC = string.count(':')

    if 4 <= l <= 5 and dotC == 1:
        result = datetime.strptime(string, '%d.%m').replace(
            year=datetime.today().year)
    elif 6 <= l <= 8 and dotC == 2:
        result = datetime.strptime(string, '%d.%m.%y')
    elif 8 <= l <= 10 and dotC == 2:
        result = datetime.strptime(string, '%d.%m.%Y')

    elif 7 <= l <= 11 and dotC == 1 and сolonC == 1:
        result = datetime.strptime(string, '%d.%m %H:%M').replace(
            year=datetime.today().year)
    elif 10 <= l <= 14 and dotC == 2 and сolonC == 1:
        result = datetime.strptime(string, '%d.%m.%y %H:%M')
    elif l == 16 and dotC == 2 and сolonC == 1:
        result = datetime.strptime(string, '%d.%m.%Y %H:%M')

    else:
        result = 'NaT'

    return result


class DB_Engine:
    def __init__(self, ip, port, user, password, db_name, schema):
        self.connector = create_engine(
            f'postgresql://{user}:{password}@{ip}:{port}/{db_name}')
        self.schema = schema

        self.sql_queries = {
            'add_regular': sql.text(
                f"INSERT INTO {schema}.regular (user_id, description, search_f, arg_sf, amount, start_date, end_date, d_years, d_months, d_days, adjust_price, adjust_date, follow_overdue) " +
                "VALUES (:user_id, :description, :search_f, :arg_sf, :amount, :start_date, :end_date, :d_years, :d_months, :d_days, :adjust_price, :adjust_date, :follow_overdue) RETURNING id"),
            'add_onetime': sql.text(f"INSERT INTO {schema}.onetime (user_id, date, description, amount) VALUES (:user_id, :date, :description, :amount) RETURNING id"),
            'delete_regular': sql.text(f"UPDATE {self.schema}.regular SET is_del = true WHERE id in :id"),
            'delete_onetime': sql.text(f"UPDATE {self.schema}.onetime SET is_del = true WHERE id in :id"),
        }

    def replace_index(self, data):
        return data.reset_index().rename(columns={'id': 'db_id'})

    def download_c_rules(self, user_id, table='dictionary_categories'):
        return pd.read_sql(f'SELECT key, value FROM {self.schema}.{table} WHERE user_id = {user_id}', self.connector).values.tolist()

    def download_regular(self, user_id, table='regular'):
        return self.replace_index(pd.read_sql(
            f'SELECT id, description, search_f, arg_sf, amount, start_date, end_date, d_years, d_months, d_days, adjust_price, adjust_date, follow_overdue FROM {self.schema}.{table} WHERE user_id = {user_id} AND is_del = False',
            self.connector))

    def download_onetime(self, user_id, table='onetime'):
        data = pd.read_sql(
            f'SELECT id, date, description, amount FROM {self.schema}.{table} WHERE user_id = {user_id} AND is_del = False', self.connector)
        if data.empty:
            data = pd.DataFrame([], columns=['date', 'description', 'amount'])
        else:
            data['date'] = pd.to_datetime(data['date'])

        return self.replace_index(data)

    def download_transactions(self, user_id, table='transactions'):
        return self.replace_index(pd.read_sql(
            f'SELECT id, date, amount, category, description, balance FROM {self.schema}.{table} WHERE user_id = {user_id} AND is_del = False ORDER BY date',
            self.connector))

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

    def delete_transactions(self, user_id, start_date, end_date='end', table='transactions'):
        if end_date == 'end':
            time = f"date > '{start_date}'"
        else:
            time = f"date BETWEEN '{start_date}' AND '{end_date}'"

        sql = f"UPDATE {self.schema}.{table} SET is_del = true WHERE user_id = {user_id} AND {time}"
        result = self.connector.engine.execute(sql)
        print(result)

    def add_regular(self, data):
        result = self.connector.execute(self.sql_queries['add_regular'], data)
        return result.first()[0]

    def add_onetime(self, data):
        result = self.connector.execute(self.sql_queries['add_onetime'], data)
        return result.first()[0]

    def delete_regular(self, db_id):
        self.connector.execute(
            self.sql_queries['delete_regular'], {'id': db_id})

    def delete_onetime(self, db_id):
        self.connector.execute(
            self.sql_queries['delete_onetime'], {'id': db_id})
