# import numpy as np
import pandas as pd
# import psycopg2
# from psycopg2.sql import SQL, Identifier
# from psycopg2.extensions import AsIs
import sqlalchemy as sqla
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
    def __init__(self, host, port, user, password, db_name, schema):
        self.connector = sqla.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
        self.schema = schema
        metadata_obj = sqla.MetaData()

        self.tables = {
            'transactions': sqla.Table('transactions', metadata_obj,
                                       sqla.Column('id', sqla.Integer,
                                                   primary_key=True),
                                       sqla.Column('user_id', sqla.Integer),
                                       sqla.Column('date', sqla.Date),
                                       sqla.Column('account_id', sqla.Integer),
                                       sqla.Column('amount', sqla.Numeric),
                                       sqla.Column('category', sqla.String),
                                       sqla.Column('description', sqla.String),
                                       sqla.Column('balance', sqla.Integer),
                                       sqla.Column('is_del', sqla.Boolean),
                                       schema=self.schema
                                       ),
            'regular': sqla.Table('regular', metadata_obj,
                                  sqla.Column('id', sqla.Integer,
                                              primary_key=True),
                                  sqla.Column('user_id', sqla.Integer),
                                  sqla.Column('description', sqla.String),
                                  sqla.Column('search_f', sqla.String),
                                  sqla.Column('arg_sf', sqla.String),
                                  sqla.Column('amount', sqla.Numeric),
                                  sqla.Column('start_date', sqla.Date),
                                  sqla.Column('end_date', sqla.Date),
                                  sqla.Column('d_years', sqla.Integer),
                                  sqla.Column('d_months', sqla.Integer),
                                  sqla.Column('d_days', sqla.Integer),
                                  sqla.Column('adjust_price', sqla.Boolean),
                                  sqla.Column('adjust_date', sqla.Boolean),
                                  sqla.Column('follow_overdue', sqla.Boolean),
                                  sqla.Column('is_del', sqla.Boolean),
                                  schema=self.schema
                                  ),
            'onetime': sqla.Table('onetime', metadata_obj,
                                  sqla.Column('id', sqla.Integer,
                                              primary_key=True),
                                  sqla.Column('user_id', sqla.Integer),
                                  sqla.Column('description', sqla.String),
                                  sqla.Column('amount', sqla.Numeric),
                                  sqla.Column('date', sqla.Date),
                                  sqla.Column('is_del', sqla.Boolean),
                                  schema=self.schema
                                  ),
            'accounts': sqla.Table('accounts', metadata_obj,
                                   sqla.Column('id', sqla.Integer,
                                               primary_key=True),
                                   sqla.Column('user_id', sqla.Integer),
                                   sqla.Column('type', sqla.SmallInteger),
                                   sqla.Column('description', sqla.String),
                                   sqla.Column('credit_limit', sqla.Numeric),
                                   sqla.Column('discharge_day', sqla.SmallInteger),
                                   schema=self.schema
                                   ),

        }

        self.sql_queries = {
            'get_c_rules': sqla.sql.text(f"SELECT key, value FROM {self.schema}.dictionary_categories WHERE user_id = :user_id"),
            'get_last_model': sqla.sql.text(f"SELECT dump FROM {self.schema}.sbs_models WHERE user_id = :user_id ORDER BY id DESC LIMIT 1"),
            'get_regular': self.tables['regular'].select().where(sqla.and_(
                self.tables['regular'].c.user_id == sqla.bindparam('user_id'),
                self.tables['regular'].c.is_del == False
            )).order_by(self.tables['regular'].c.start_date),

            'get_onetime': self.tables['onetime'].select().where(sqla.and_(
                self.tables['onetime'].c.user_id == sqla.bindparam('user_id'),
                self.tables['onetime'].c.is_del == False
            )).order_by(self.tables['onetime'].c.date),

            'get_accounts': self.tables['accounts'].select().where(
                self.tables['accounts'].c.user_id == sqla.bindparam('user_id')
            ).order_by(self.tables['accounts'].c.id),

            'get_transactions': self.tables['transactions'].select().where(sqla.and_(
                self.tables['transactions'].c.user_id == sqla.bindparam(
                    'user_id'),
                self.tables['transactions'].c.is_del == False
            )).order_by(self.tables['transactions'].c.date),

            'add_regular': self.tables['regular'].insert().returning(self.tables['regular'].c.id),
            'add_onetime': self.tables['onetime'].insert().returning(self.tables['onetime'].c.id),
            'add_accounts': self.tables['accounts'].insert().returning(self.tables['accounts'].c.id),

            'delete_transactions': self.tables['transactions'].update().where(self.tables['transactions'].c.user_id == sqla.bindparam('user_id')).values(is_del=True),
            'delete_regular': self.tables['regular'].update().where(self.tables['regular'].c.id.in_(sqla.bindparam('db_id', expanding=True))).values(is_del=True),
            'delete_onetime': self.tables['onetime'].update().where(self.tables['onetime'].c.id.in_(sqla.bindparam('db_id', expanding=True))).values(is_del=True),

            'update_regular': self.tables['regular'].update().where(self.tables['regular'].c.id == sqla.bindparam('db_id')),
            'update_onetime': self.tables['onetime'].update().where(self.tables['onetime'].c.id == sqla.bindparam('db_id')),

        }

    def download_c_rules(self, user_id):
        return self.__read_sql('get_c_rules', {'user_id': user_id})[['key', 'value']].values.tolist()

    def download_regular(self, user_id):
        return self.__read_sql('get_regular', {'user_id': user_id})

    def download_onetime(self, user_id):
        data = self.__read_sql(
            'get_onetime', {'user_id': user_id}, parse_dates=['date'])
        # data['date'] = pd.to_datetime(data['date'])
        return data

    def download_accounts(self, user_id):
        data = self.__read_sql(
            'get_accounts', {'user_id': user_id})
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
        query = self.sql_queries['delete_transactions']
        if end_date == 'end':
            query = query.where(self.tables.c.date > start_date)
        else:
            query = query.where(
                self.tables.c.date.between(start_date, end_date))

        self.connector.execute(self.sql_queries['delete_transactions'], {
                               'user_id': user_id})

    def add_event(self, table: str, data: dict):
        result = self.connector.execute(self.sql_queries['add_'+table], data)
        return result.first()[0]

    def delete_event(self, table, db_id):
        self.connector.execute(
            self.sql_queries['delete_'+table], {'db_id': db_id})

    def edit_event(self, table, db_id, column, value):
        self.connector.execute(
            self.sql_queries['update_'+table], {'db_id': db_id, column: value})

    def __read_sql(self, quory_name: str, values: dict, parse_dates=None):
        return pd.read_sql(
            sql=self.sql_queries[quory_name],
            con=self.connector,
            params=values,
            parse_dates=parse_dates
        ).reset_index().rename(columns={'id': 'db_id'})
