import DataLoader as dl
import EventEngine as ee
import time
import Visual


class User:
    '''Класс для пользователя.

    Attributes:
        id: id пользователя.
        transactions: список транзакций.
        sbs_model: список моделей, под каждую фичу, для прогноза транзакций для этого пользователя.
        regular_list: список регулярных транзакций.
        onetime_transactions: cписок разовых транзакций. 
        predicted_events: рассчитанные транзакции до указанной даты.
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
        # self.transactions = dl.tinkoff_file_parse(
        #     file_full_name, db_engine, self.id)
        # self.transactions['balance'] = ee.get_balance_past(
        #     new_balance, self.transactions['amount'])
        # self.transactions = ee.get_all_transactions(
        #     self.transactions, db_engine, self.id)
        # ee.save_new_transactions(self.transactions, db_engine, self.id)
        self.transactions = dl.tinkoff_file_parse(file_full_name, db_engine, self.id)
        self.transactions = ee.add_and_merge_transactions(self.transactions, new_balance, db_engine, self.id)


        return self.transactions

    def predict_events(self, end_date):
        '''Прогнозирует регулярные и одноразовые транзакции.

        Args:
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм регулярных транзакций с колонками ['amount', 'category', 'description', 'balance']
        '''
        self.predicted_events = ee.predict_events(
            self.regular_list, self.onetime_transactions, self.transactions, end_date)

        return self.predicted_events

    def predict_full(self, end_date):
        '''Прогнозирует транзакции. Регулярные и предсказанные транзакции складываются.

        Args:
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'balance']
        '''
        data = ee.preprocessing_for_ml(
            self.transactions, self.regular_list, self.sbs_model)

        return ee.get_full_transactions(
            self.predicted_events,
            data,
            self.transactions['balance'].iloc[-1],
            self.sbs_model,
            end_date
        )

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
        data = ee.preprocessing_for_ml(
            self.transactions, self.regular_list, self.sbs_model)

        self.sbs_model = ee.fit_model(data, self.sbs_model)

        time_passed = time.time() - start_time
        db_engine.upload_model(self.id, self.sbs_model)

        return {'time': time_passed, 'event_count': len(self.transactions), 'ml_event_count': len(data)}


class UserManager:
    '''Класс для управления пользователями.

    Attributes:
        db_engine: объект для работы с базой данных.
        user_list: список пользователей.
    '''

    def __init__(self, db_settings):
        self.db_engine = dl.DB_Engine(**db_settings)
        self.user_list = []

    def get_user(self, user_id):
        '''Ищет и возвращает объект пользователя по его id 

        Args:
            user_id: id пользователя.

        Returns:
            Объект пользователя.
        '''
        for u in self.user_list:
            if u.id == user_id:
                return u
        # if not found:
        new_u = User(user_id, self.db_engine)
        self.user_list.append(new_u)
        return new_u

    def load_from_file(self, user_id, file_full_name, new_balance):
        '''Загружает, обрабатывает и сохраняет транзакции из файла. Соединяет новую информацию из файла с транзакциями сохраненными в базу до этого

        Args:
            user_id: id пользователя.
            file_full_name: полное имя файла.
            new_balance: текущий баланс пользователя, после последней операции в файле.

        Returns:
            Датафрейм всех транзакций с колонками ['date', 'amount', 'category', 'description', 'balance', 'is_new']
            где is_new == True если эта строка из файла.
        '''
        return self.get_user(user_id).load_from_file(self.db_engine, file_full_name, new_balance)

    def predict_events(self, user_id, end_date):
        '''Прогнозирует регулярные и одноразовые транзакции для пользователя.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'category', 'description', 'balance']
        '''
        user = self.get_user(user_id)
        return user.predict_events(end_date)

    def predict_full(self, user_id, end_date):
        '''Прогнозирует транзакции для пользователя. Регулярные и предсказанные транзакции складываются.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'balance']
        '''
        user = self.get_user(user_id)

        return user.predict_full(end_date)

    def fit_new_model(self, user_id):
        '''Создает, учит и сохраняет модели для пользователя.

        Args:
            user_id: id пользователя.

        Returns:
            Отчет о обучении модели. Словарь формата {'time', 'event_count', 'ml_event_count'}
            time: потраченное время в секундах, на обучение модели.
            event_count: всего событий в базе.
            ml_event_count: события участвующие в обучении модели.
        '''
        user = self.get_user(user_id)

        return user.fit_new_model(self.db_engine)

    def get_report_obj(self, user_id, end_date):
        '''Прогнозирует транзакции пользователя, строит графики.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Словарь с изображениями в двоичном формате.
        '''
        events = self.predict_events(user_id, end_date).set_index('date')[
            ['amount', 'description']]

        full_transactions = self.predict_full(user_id, end_date)

        return {
            'transactions': Visual.transactions_plot(full_transactions),
            'events': Visual.df_to_text(events)
        }
