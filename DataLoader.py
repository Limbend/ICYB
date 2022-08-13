import numpy as np
import pandas as pd
import psycopg2
from psycopg2.sql import SQL, Identifier
from psycopg2.extensions import AsIs
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
    def __init__(self, host, port, user, password, dbname, schema):
        self.connector = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname)
        self.cursor = self.connector.cursor()
        self.schema = schema

        self.sql_queries = {
            'get_c_rules': SQL("SELECT key, value FROM {schema}.dictionary_categories WHERE user_id = %(user_id)s"),
            'get_regular': SQL("SELECT id, description, search_f, arg_sf, amount, start_date, end_date, d_years, d_months, d_days, adjust_price, adjust_date, follow_overdue FROM {schema}.regular WHERE user_id = %(user_id)s AND is_del = False ORDER BY start_date"),
            'get_onetime': SQL("SELECT id, date, description, amount FROM {schema}.onetime WHERE user_id = %(user_id)s AND is_del = False ORDER BY date"),
            'get_transactions': SQL("SELECT id, date, amount, category, description, balance FROM {schema}.transactions WHERE user_id = %(user_id)s AND is_del = False ORDER BY date"),
            'get_last_model': SQL("SELECT dump FROM {schema}.sbs_models WHERE user_id = %(user_id)s ORDER BY id DESC LIMIT 1"),

            'add_regular': SQL(
                "INSERT INTO {schema}.regular (user_id, description, search_f, arg_sf, amount, start_date, end_date, d_years, d_months, d_days, adjust_price, adjust_date, follow_overdue) " +
                "VALUES (%(user_id)s, %(description)s, %(search_f)s, %(arg_sf)s, %(amount)s, %(start_date)s, %(end_date)s, %(d_years)s, %(d_months)s, %(d_days)s, %(adjust_price)s, %(adjust_date)s, %(follow_overdue)s) RETURNING id"),
            'add_onetime': SQL("INSERT INTO {schema}.onetime (user_id, date, description, amount) VALUES (%(user_id)s, %(date)s, %(description)s, %(amount)s) RETURNING id"),

            'delete_transactions': SQL("UPDATE {schema}.transactions SET is_del = true WHERE user_id = %(user_id)s AND %(time)s"),
            'delete_regular': SQL("UPDATE {schema}.regular SET is_del = true WHERE id = %(id)s"),
            'delete_onetime': SQL("UPDATE {schema}.onetime SET is_del = true WHERE id = %(id)s"),

            'update_regular': SQL("UPDATE {schema}.regular SET %(column)s = %(value)s WHERE id = %(id)s"),
            'update_onetime': SQL("UPDATE {schema}.onetime SET %(column)s = %(value)s WHERE id = %(id)s"),
        }

        self.sql_queries = {key: self.sql_queries[key].format(
            schema=Identifier(self.schema),
        ) for key in self.sql_queries.keys()}

    def download_c_rules(self, user_id):
        return self.__read_sql('get_c_rules', {'user_id': user_id})[['key', 'value']].values.tolist()

    def download_regular(self, user_id):
        return self.__read_sql('get_regular', {'user_id': user_id})

    def download_onetime(self, user_id):
        data = self.__read_sql('get_onetime', {'user_id': user_id})
        # if data.empty:
        #     data = pd.DataFrame([], columns=['date', 'description', 'amount'])

        data['date'] = pd.to_datetime(data['date'])
        return data

    def download_transactions(self, user_id):
        return self.__read_sql('get_transactions', {'user_id': user_id})

    def add_transactions(self, data, user_id, table='transactions'):
        data = data[['date', 'amount', 'category',
                     'description', 'balance']].copy().sort_values('date')
        data['user_id'] = user_id
        data['is_del'] = False

        data.to_sql(table, self.connector, schema=self.schema,
                    if_exists='append', index=False)

    def download_last_model(self, user_id):
        df = self.__read_sql('get_last_model', {'user_id': user_id})
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

    def delete_transactions(self, user_id, start_date, end_date='end'):
        if end_date == 'end':
            time = f"date > '{start_date}'"
        else:
            time = f"date BETWEEN '{start_date}' AND '{end_date}'"

        self.cursor.execute(self.sql_queries['delete_transactions'], {
                            'user_id': user_id, 'time': time})
        self.connector.commit()

    def add_event(self, table: str, data: dict):
        self.cursor.execute(self.sql_queries['add_'+table], data)
        self.connector.commit()
        return self.cursor.fetchone()[0]

    def delete_event(self, table, db_id):
        self.cursor.execute(
            self.sql_queries['delete_'+table], {'id': db_id})
        self.connector.commit()

    def edit_event(self, table, db_id, column, value):
        self.cursor.execute(
            self.sql_queries['update_'+table], {'id': db_id, 'column': AsIs(column), 'value': value})
        self.connector.commit()

    def __read_sql(self, quory_name: str, values: dict):
        return pd.read_sql(
            self.cursor.mogrify(self.sql_queries[quory_name], values),
            self.connector).reset_index().rename(columns={'id': 'db_id'})
