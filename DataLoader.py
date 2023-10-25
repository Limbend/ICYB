import pandas as pd
import sqlalchemy as sqla
import pickle
import re
from datetime import date, datetime


def tinkoff_file_parse(path, db_engine, user_id, account_id=-1):
    df = pd.read_csv(path, sep=';', parse_dates=[
                     0, 1], dayfirst=True, decimal=",", encoding='cp1251')
    df = df[df['Статус'] == 'OK'][[
        'Дата операции',
        'Номер карты',
        'Сумма платежа',
        'Категория',
        'Описание'
    ]]

    if account_id == -1:
        pass
    else:
        df = df[[
            'Дата операции',
            'Сумма платежа',
            'Категория',
            'Описание'
        ]]

    df_for_sql = df.reindex(index=df.index[::-1]).reset_index(drop=True)
    df_for_sql.columns = ['date', 'amount', 'category', 'description']

    for k, v in db_engine.download_c_rules(user_id=user_id):
        df_for_sql.loc[df_for_sql['description'] == k, 'category'] = v
    df_for_sql['account_id'] = account_id

    return df_for_sql[['date', 'account_id', 'amount', 'category', 'description']]


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
    colonC = string.count(':')

    if 4 <= l <= 5 and dotC == 1:
        result = datetime.strptime(string, '%d.%m').replace(
            year=datetime.today().year)
    elif 6 <= l <= 8 and dotC == 2:
        result = datetime.strptime(string, '%d.%m.%y')
    elif 8 <= l <= 10 and dotC == 2:
        result = datetime.strptime(string, '%d.%m.%Y')

    elif 7 <= l <= 11 and dotC == 1 and colonC == 1:
        result = datetime.strptime(string, '%d.%m %H:%M').replace(
            year=datetime.today().year)
    elif 10 <= l <= 14 and dotC == 2 and colonC == 1:
        result = datetime.strptime(string, '%d.%m.%y %H:%M')
    elif l == 16 and dotC == 2 and colonC == 1:
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
                                   sqla.Column('discharge_day',
                                               sqla.SmallInteger),
                                   schema=self.schema),

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

            'add_transactions': self.tables['transactions'].insert().returning(self.tables['transactions'].c.id),
            'add_regular': self.tables['regular'].insert().returning(self.tables['regular'].c.id),
            'add_onetime': self.tables['onetime'].insert().returning(self.tables['onetime'].c.id),
            'add_accounts': self.tables['accounts'].insert().returning(self.tables['accounts'].c.id),

            # 'delete_transactions': self.tables['transactions'].update().where(self.tables['transactions'].c.user_id == sqla.bindparam('user_id')).values(is_del=True),
            'delete_transactions': self.tables['transactions'].update().where(sqla.and_(
                self.tables['transactions'].c.user_id ==
                sqla.bindparam('b_user_id'),
                self.tables['transactions'].c.account_id ==
                sqla.bindparam('account_id')
            )).values(is_del=True),
            'delete_regular': self.tables['regular'].update().where(self.tables['regular'].c.id.in_(sqla.bindparam('db_id', expanding=True))).values(is_del=True),
            'delete_onetime': self.tables['onetime'].update().where(self.tables['onetime'].c.id.in_(sqla.bindparam('db_id', expanding=True))).values(is_del=True),

            'update_regular': self.tables['regular'].update().where(self.tables['regular'].c.id == sqla.bindparam('db_id')),
            'update_onetime': self.tables['onetime'].update().where(self.tables['onetime'].c.id == sqla.bindparam('db_id')),

        }

    def download_c_rules(self, user_id):
        return self.__read_sql('get_c_rules',
                               {'user_id': user_id}, drop_uid=False)[['key', 'value']].values.tolist()

    def download_regular(self, user_id):
        return self.__read_sql('get_regular', {'user_id': user_id})

    def download_onetime(self, user_id):
        data = self.__read_sql(
            'get_onetime', {'user_id': user_id}, parse_dates=['date'])
        # data['date'] = pd.to_datetime(data['date'])
        return data

    def download_accounts(self, user_id):
        return self.__read_sql('get_accounts', {'user_id': user_id})

    def download_transactions(self, user_id):
        return self.__read_sql('get_transactions', {'user_id': user_id})

    def download_last_model(self, user_id):
        df = self.__read_sql('get_last_model',
                             {'user_id': user_id}, drop_uid=False)
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

    def delete_transactions(self, user_id, account_id, start_date, end_date='end'):
        query = self.sql_queries['delete_transactions']
        if end_date == 'end':
            query = query.where(
                self.tables['transactions'].c.date > start_date)
        else:
            query = query.where(
                self.tables['transactions'].c.date.between(start_date, end_date))

        self.connector.execute(self.sql_queries['delete_transactions'], {
                               'user_id': user_id, 'account_id': account_id})

    def add_event(self, table: str, data: dict):
        result = self.connector.execute(self.sql_queries['add_'+table], data)
        return result.first()[0]

    def delete_event(self, table, db_id):
        self.connector.execute(
            self.sql_queries['delete_'+table], {'db_id': db_id})

    def edit_event(self, table, db_id, column, value):
        self.connector.execute(
            self.sql_queries['update_'+table], {'db_id': db_id, column: value})

    def get_users_for_notifications(self):
        result = self.connector.execute(sqla.sql.text(
            f"SELECT user_id FROM {self.schema}.regular \
                WHERE end_date IS null OR end_date > now() \
                    UNION SELECT user_id FROM {self.schema}.onetime \
                        WHERE date > now()"))
        return [r for r, in result]

    def __read_sql(self, quory_name: str, values: dict, parse_dates=None, drop_uid=True):
        data = pd.read_sql(
            sql=self.sql_queries[quory_name],
            con=self.connector,
            params=values,
            parse_dates=parse_dates
        ).reset_index(drop=True).rename(columns={'id': 'db_id'})

        if drop_uid:
            return data.drop('user_id', axis=1)
        return data
