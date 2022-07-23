import time
import DataLoader as dl
import pandas as pd
import numpy as np
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import ML as ml


class User:
    '''Класс для пользователя.

    Attributes:
        id: id пользователя.
        transactions: список транзакций.
        sbs_model: список моделей, под каждую фичу, для прогноза транзакций для этого пользователя.
        regular_list: список регулярных транзакций.
        onetime_transactions: cписок разовых транзакций. 
        predicted_events: рассчитанные регулярные и разовые транзакции до указанной даты.
        predicted_transactions: прогноз транзакций до указанной даты.
    '''

    def __init__(self, id, db_engine):
        '''Загружает всю информацию из базы данных

        Args:
            db_engine: объект для работы с базой данных.
        '''

        self.id = id

        self.transactions = db_engine.download_transactions(self.id)
        self.sbs_model = db_engine.download_last_model(self.id)
        self.regular_list = db_engine.download_regular(self.id)
        self.onetime_transactions = db_engine.download_onetime(self.id)

    def load_from_file(self, db_engine, file_full_name, new_balance):
        '''Загружает, обрабатывает и сохраняет транзакции из файла. Соединяет новую информацию из файла с транзакциями сохраненными в базу до этого

        Args:
            db_engine: объект для работы с базой данных.
            file_full_name: полное имя файла.
            new_balance: текущий баланс пользователя, после последней операции в файле.

        Returns:
            Датафрейм всех транзакций с колонками ['date', 'amount', 'category', 'description', 'balance', 'is_new']
            где is_new == True если эта строка из файла.
        '''
        self.transactions = dl.tinkoff_file_parse(
            file_full_name, db_engine, self.id)
        self.transactions = self.__add_and_merge_transactions(
            self.transactions, new_balance, db_engine, self.id)

        return self.transactions

    def predict_events(self, start_date, end_date):
        '''Прогнозирует регулярные и одноразовые транзакции.

        Args:
            start_date: дата c которой строить прогноз.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм регулярных транзакций с колонками ['amount', 'category', 'description', 'balance']
        '''

        predicted_regular = self.__predict_regular_events(start_date, end_date)

        self.predicted_events = pd.concat([
            predicted_regular,
            self.onetime_transactions[(self.onetime_transactions['date'] >= start_date) & (
                self.onetime_transactions['date'] <= end_date)],
        ]).sort_values('date')

        return self.predicted_events

    def get_comparison_data(self):
        '''Создаст датафрейм прогнозируемого и фактического баланса, полученного из добавленных в базу транзакций.
        Необходим для сравнения прогнозов с фактическими расходами и доходами.

        Returns:
            Датафрейм с колонками ['reab_b', 'predicted_b']
        '''
        is_new = self.transactions[self.transactions['is_new']]
        not_new = self.transactions[~self.transactions['is_new']]

        start_date = pd.to_datetime(not_new.tail(1)['date'].values[0])
        end_date = pd.to_datetime(is_new.tail(1)['date'].values[0])

        predicted_events = self.predict_events(start_date, end_date)

        data = self.__preprocessing_for_ml(not_new)

        predicted_transactions = self.sbs_model.predict(
            data, end_date).to_frame()

        merged_transactions = self.__merge_of_predicts(
            predicted_events, predicted_transactions, not_new['balance'].iloc[-1])

        merged_transactions = merged_transactions[['balance']]
        merged_transactions.columns = ['predicted_b']

        real_full_transactions = is_new.set_index(
            'date')[['balance']].resample('1D').last()
        real_full_transactions.columns = ['reab_b']

        previous_week = not_new.set_index(
            'date')[['balance']].resample('1D').last().tail(7)
        previous_week.columns = ['reab_b']
        previous_week['predicted_b'] = previous_week['reab_b']

        comparison = pd.concat([
            previous_week,
            pd.concat([
                real_full_transactions,
                merged_transactions
            ], axis=1)
        ], axis=0)

        comparison = comparison.fillna(method='ffill')

        return comparison

    def predict_full(self, end_date):
        '''Прогнозирует транзакции. Регулярные и предсказанные транзакции складываются.

        Args:
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'balance']
        '''
        data = self.__preprocessing_for_ml(self.transactions)
        self.predicted_transactions = self.sbs_model.predict(
            data, end_date).to_frame()

        return self.__merge_of_predicts(self.predicted_events, self.predicted_transactions, self.transactions['balance'].iloc[-1])

    def fit_new_model(self, db_engine):
        '''Создает, учит и сохраняет модель для пользователя.

        Args:
            db_engine: объект для работы с базой данных.

        Returns:
            Отчет о обучении модели. Словарь формата {'time', 'event_count', 'ml_event_count'}
            time: время в секундах, потребовавшиеся для обучения модели.
            event_count: всего событий в базе.
            ml_event_count: события участвующие в обучении модели.
        '''
        start_time = time.time()
        data = self.__preprocessing_for_ml(self.transactions)

        self.sbs_model = self.__fit_model(data, self.sbs_model)

        time_passed = time.time() - start_time
        db_engine.upload_model(self.id, self.sbs_model)

        return {'time': time_passed, 'event_count': len(self.transactions), 'ml_event_count': len(data)}

    def add_regular(self, db_engine, start_date, end_date, delta, description, amount, search_f, arg_sf, adjust_price, adjust_date, follow_overdue):
        '''Добавляет регулярное событие.

        Args:
            db_engine: объект для работы с базой данных.
            start_date: дата первой транзакции.
            end_date: дата, после которой перестать прогнозировать данную транзакцию
            delta: сколько лет, месяцев, дней  между транзакциями. Формата - [d_years, d_months, d_days]
            description: описание транзакции.
            amount: сумма транзакции.
            search_f: функция поиска предыдущих транзакций. Одна из - ['description', 'amount_description', 'amount<_description', 'amount_category', 'amount<_category', 'dont_search']
            arg_sf: аргумент для функции поиска. Варианты для различных функций:
                description - описание 
                amount_description - описание, а amount будет равным самой сумме транзакции
                amount<_description - строка формата 'amount,description'
                amount_category - категория, а amount будет равным самой сумме транзакции
                amount<_category - строка формата 'amount,category'
                dont_search - None
            adjust_price: пересчитывать ли сумму транзакции, основываясь на предыдущих.
            adjust_date: пересчитывать ли дату транзакции, основываясь на предыдущих.
            follow_overdue: следить ли за просроченными транзакциями.
        '''
        new_row = {
            'user_id': self.id,
            'description': description,
            'search_f': search_f,
            'arg_sf': arg_sf,
            'amount': amount,
            'start_date': start_date,
            'end_date': end_date,
            'd_years': delta[0],
            'd_months': delta[1],
            'd_days': delta[2],
            'adjust_price': adjust_price,
            'adjust_date': adjust_date,
            'follow_overdue': follow_overdue
        }
        db_index = db_engine.add_regular(new_row)
        new_row['db_id'] = db_index
        del new_row['user_id']

        self.regular_list = pd.concat([
            self.regular_list,
            pd.DataFrame([new_row])
        ], axis=0).reset_index(drop=True)

    def add_onetime(self, db_engine, date, amount, description):
        '''Добавляет однократное событие.

        Args:
            db_engine: объект для работы с базой данных.
            date: дата транзакции.
            amount: сумма транзакции.
            description: описание транзакции.
        '''
        new_row = {'user_id': self.id, 'description': description,
                   'amount': amount, 'date': date}
        db_index = db_engine.add_onetime(new_row)
        new_row['db_id'] = db_index
        del new_row['user_id']

        onetime_transactions = pd.concat([
            onetime_transactions,
            pd.DataFrame([new_row])
        ], axis=0).reset_index(drop=True)

        self.onetime_transactions = onetime_transactions

    def delete_regular(self, db_engine, id):
        '''Удаляет регулярное событие.

        Args:
            db_engine: объект для работы с базой данных.
            id: локальный id события, которое нужно удалить.
        '''
        db_id = tuple(str(i)
                      for i in self.regular_list.loc[id, 'db_id'].values)
        db_engine.delete_regular(db_id)

        self.regular_list = self.regular_list.drop(id).reset_index(drop=True)

    def delete_onetime(self, db_engine, id):
        '''Удаляет однократное событие.

        Args:
            db_engine: объект для работы с базой данных.
            id: локальный id события, которое нужно удалить.
        '''
        db_id = tuple(str(i)
                      for i in self.onetime_transactions.loc[id, 'db_id'].values)
        db_engine.delete_onetime(db_id)

        self.onetime_transactions = self.onetime_transactions.drop(
            id).reset_index(drop=True)

    def __predict_regular_events(self, g_start_date, g_end_date, window_price=3, uniform_distribution=False):
        # !!!
        new_regular_events = self.regular_list.copy()
        result = []
        j_limit = 1000

        new_regular_events['start_date'] = pd.to_datetime(
            new_regular_events['start_date'])
        new_regular_events['end_date'] = pd.to_datetime(
            new_regular_events['end_date'])

        for i, r_event in new_regular_events.iterrows():
            d_date = relativedelta(
                years=int(r_event['d_years']),
                months=int(r_event['d_months']),
                days=int(r_event['d_days'])
            )

            if pd.isna(r_event['end_date']):
                r_event['end_date'] = g_end_date
            else:
                r_event['end_date'] = min(r_event['end_date'], g_end_date)

            # Обновление цены
            if(r_event['adjust_price']):
                amounts = self.transactions[self.__get_markers_regular(
                    self.transactions, r_event)].tail(window_price)['amount']
                if(len(amounts) > 0):
                    if uniform_distribution:
                        r_event['amount'] = amounts.mean()
                    else:
                        window_price = min(window_price, len(amounts))
                        r_event['amount'] = (
                            amounts * [2 / (window_price + window_price**2) * (x + 1) for x in range(window_price)]).sum()

            # Обновление начальной даты
            if(r_event['adjust_date']):
                events = self.transactions[self.__get_markers_regular(
                    self.transactions, self.regular_list.loc[i])]
                if len(events) > 0:
                    r_event['start_date'] = events.iloc[-1]['date'] + d_date

            j = 0
            new_start = r_event['start_date']  # + d_date * j
            while(new_start < g_start_date and new_start < r_event['end_date']):
                j += 1
                if j == j_limit:
                    raise Exception(
                        f'When searching for the start date, the maximum number of iterations was exceeded\n{r_event}')

                new_start = r_event['start_date'] + d_date * j

            # Проверка на просрочку
            j -= 1
            if(r_event['follow_overdue'] and j > 0):
                pay_date_overdue = g_start_date + relativedelta(days=1)

                if(r_event['adjust_date']):
                    # Если между стартовой датой для регулярки r_event['start_date'] и стартовой датой для начала поиска g_start_date помешаются регулярки - они являются просрочкой. Количество поместившихся будет в j.
                    for i_overdue in range(j):
                        result.append((
                            pay_date_overdue,
                            r_event['amount'],
                            f'overdue[{i}-{i_overdue}]',
                            r_event['description'],
                            True
                        ))
                else:
                    # Посчитать сколько должно быть регулярок между стартовой датой r_event['start_date'] и начальной датой поиска g_start_date.
                    # Вычесть из них сколько по факту было.

                    count_overdue = j - sum(self.__get_markers_regular(
                        self.transactions[self.transactions['date'] >= pd.to_datetime(r_event['start_date'])], r_event)) + 1
                    # Если число положительное, то есть просрочки.
                    if(count_overdue > 0):
                        for i_overdue in range(count_overdue):
                            result.append((
                                pay_date_overdue,
                                r_event['amount'],
                                f'overdue[{i}-{i_overdue}]',
                                r_event['description'],
                                True
                            ))

                    # Если отрицательное, то есть оплата зарание. Нужно обновить стартовую дату.
                    elif(count_overdue < 0):
                        new_start = r_event['start_date'] + \
                            d_date * (j - count_overdue)

            j = 0
            date = new_start  # + d_date * j
            while(date < r_event['end_date']):
                result.append((
                    date,
                    r_event['amount'],
                    f'regular[{i}]',
                    r_event['description'],
                    False
                ))

                j += 1
                if j == j_limit:
                    raise Exception(
                        f'The maximum number of iterations has been exceeded\n{r_event}')
                date = new_start + d_date * j

        df_events = pd.DataFrame(result, columns=[
                                 'date', 'amount', 'category', 'description', 'is_overdue']).sort_values('date').reset_index(drop=True)
        df_events['date'] = pd.to_datetime(df_events['date'])
        return df_events

    def __preprocessing_for_ml(self, data, q=0.16):
        cleared_df = data.copy()
        cleared_df = self.__drop_paired(cleared_df, 'amount')

        # Выделяет только транзакции
        markers = cleared_df['amount'] < 0

        for i in self.regular_list.index:
            markers = markers & ~self.__get_markers_regular(
                cleared_df, self.regular_list.loc[i])
        cleared_df = cleared_df[markers]

        cleared_df = self.__drop_outliers(cleared_df, q)
        cleared_df = cleared_df.set_index('date')

        if self.sbs_model is None:
            column_adding_method = self.__get_default_parameters()[
                'column_adding_method']
        else:
            column_adding_method = self.sbs_model.column_adding_method

        if column_adding_method:
            cleared_df = self.__calculate_features(
                cleared_df, method=column_adding_method)
        else:
            cleared_df = cleared_df[['amount']]

        return cleared_df.resample('1D').sum()

    def __add_and_merge_transactions(self, new_transactions, new_balance, db_engine):
        # !!!
        old_transactions = db_engine.download_transactions(
            self.id)  # !!! Зачем 2 раза загружать?
        old_transactions['is_new'] = False
        new_transactions = new_transactions.copy()
        new_transactions['is_new'] = True

        full_transactions = pd.concat([old_transactions, new_transactions]).drop_duplicates(
            subset=['date', 'amount']).sort_values('date')

        new_start_date = full_transactions[full_transactions['is_new']
                                           ].iloc[0]['date']
        if (len(full_transactions[~full_transactions['is_new']]) > 0) and \
            (len(full_transactions[full_transactions['is_new']]) > 0) and \
                (full_transactions[~full_transactions['is_new']].iloc[-1]['date'] > new_start_date):

            print(f"Delete transactions after {new_start_date} in DB")
            db_engine.delete_transactions(
                self.id, full_transactions[full_transactions['is_new']].iloc[0]['date'])

            full_transactions = pd.concat([old_transactions[old_transactions['date'] < new_start_date], new_transactions]).drop_duplicates(
                subset=['date', 'amount']).sort_values('date')

        full_transactions['balance'] = self.__get_balance_past(
            new_balance, full_transactions['amount'])
        db_engine.add_transactions(
            full_transactions[full_transactions['is_new']], user_id=self.id)

        return full_transactions.reset_index(drop=True)

    def __get_balance_past(self, start, transactions):
        # !!!
        result = transactions.cumsum()
        return result + (start - result.iloc[-1])

    def __get_balance_future(self, start, transactions):
        # !!!
        return transactions.cumsum() + start

    def __drop_paired(self, data: pd.DataFrame, by: str):
        # !!!
        sort_values = data[by].sort_values()
        abs_values = sort_values.abs()
        c1 = sort_values.groupby(abs_values).transform(pd.Series.cumsum) > 0
        c2 = sort_values[::-
                         1].groupby(abs_values).transform(pd.Series.cumsum) < 0

        return data[c1 | c2]

    def __drop_outliers(self, data, q=0.16):
        # !!!
        data = data[data['amount'] > data['amount'].quantile(q)]
        return data

    def __get_default_parameters(self):
        # !!!
        return {
            'target_column': 'amount',
            'column_adding_method': False,
            'list_mf_rules': {'amount': [{'column': 'amount', 'lag': [2, 4], 'rm': [2, 1, 4, 3]}]}
        }

    def __fit_model(self, data, sbs_model=None):
        if sbs_model is None:
            sbs_model = ml.SbsModel(**self.__get_default_parameters())

        sbs_model.fit(data)
        return sbs_model

    def __encoder_in_sum(self, data, target_column, sum_column, top_size, sort_ascending=True):
        # !!!
        result = data.groupby(target_column)[sum_column].sum(
        ).sort_values(ascending=sort_ascending)[:top_size]
        result = {name: (top_size-i+1)/(top_size+1)
                  for i, name in enumerate(result.index)}
        return result

    def __encoder_in_count(self, data, target_column, top_size, sort_ascending=False):
        # !!!
        result = data.groupby(target_column)[target_column].count(
        ).sort_values(ascending=sort_ascending)[:top_size]
        result = {name: (top_size-i+1)/(top_size+1)
                  for i, name in enumerate(result.index)}
        return result

    def __calculate_features(self, data, method):
        # !!!
        data = data[['amount', 'category', 'description']]

        if method in ('sum', 'coumt_sum'):
            data['category_n_sum'] = data['category'].map(
                self.__encoder_in_sum(data, 'category', 'amount', 20)
            ).fillna(1./21)
            data['description_n_sum'] = data['description'].map(
                self.__encoder_in_sum(data, 'description', 'amount', 20)
            ).fillna(1./21)

        if method in ('coumt', 'coumt_sum'):
            data['category_n_coumt'] = data['category'].map(
                self.__encoder_in_count(data, 'category', 20)
            ).fillna(1./21)
            data['description_n_coumt'] = data['description'].map(
                self.__encoder_in_count(data, 'description', 20)
            ).fillna(1./21)

        return data.drop(['category', 'description'], axis=1)

    def __get_markers_regular(self, data, event):
        # !!!
        if event['search_f'] == 'description':
            return data['description'] == event['arg_sf']

        elif event['search_f'] == 'amount_description':
            return (data['description'] == event['arg_sf']) & (data['amount'] == event['amount'])

        elif event['search_f'] == 'amount<_description':
            arg_sf = event['arg_sf'].split(',')
            return (data['description'] == arg_sf[1]) & (data['amount'] < int(arg_sf[0]))

        elif event['search_f'] == 'amount_category':
            return (data['category'] == event['arg_sf']) & (data['amount'] == event['amount'])

        elif event['search_f'] == 'amount<_category':
            arg_sf = event['arg_sf'].split(',')
            return (data['category'] == arg_sf[1]) & (data['amount'] < int(arg_sf[0]))

        elif event['search_f'] == 'dont_search':
            return np.full(len(data), False)

        raise Exception(
            f'The search function /"{event["search_f"]}/" does not exist')

    def __merge_of_predicts(self, predicted_events, predicted_transactions, start_balance):
        # !!!
        merged_transactions = pd.concat([
            predicted_events.set_index('date')[['amount']],
            predicted_transactions
        ]).resample('1D').sum()

        merged_transactions['balance'] = self.__get_balance_future(
            start_balance, merged_transactions['amount'])

        return merged_transactions
