import DataLoader as dl
import EventEngine as ee
import time
from datetime import date, datetime
import Visual


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
        # self.transactions = dl.tinkoff_file_parse(
        #     file_full_name, db_engine, self.id)
        # self.transactions['balance'] = ee.get_balance_past(
        #     new_balance, self.transactions['amount'])
        # self.transactions = ee.get_all_transactions(
        #     self.transactions, db_engine, self.id)
        # ee.save_new_transactions(self.transactions, db_engine, self.id)
        self.transactions = dl.tinkoff_file_parse(
            file_full_name, db_engine, self.id)
        self.transactions = ee.add_and_merge_transactions(
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
        self.predicted_events = ee.predict_events(
            self.regular_list, self.onetime_transactions, self.transactions, start_date, end_date)

        return self.predicted_events

    def get_comparison_data(self):
        '''Создаст датафрейм прогнозируемого и фактического баланса, полученного из добавленных в базу транзакций.
        Необходим для сравнения прогнозов с фактическими расходами и доходами.

        Returns:
            Датафрейм с колонками ['reab_b', 'predicted_b']
        '''
        return ee.get_comparison_data(self.regular_list, self.onetime_transactions, self.transactions, self.sbs_model)

    def predict_full(self, end_date):
        '''Прогнозирует транзакции. Регулярные и предсказанные транзакции складываются.

        Args:
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'balance']
        '''
        data = ee.preprocessing_for_ml(
            self.transactions, self.regular_list, self.sbs_model)
        self.predicted_transactions = ee.predict_transactions(
            data, self.sbs_model, end_date)

        return ee.merge_of_predicts(self.predicted_events, self.predicted_transactions, self.transactions['balance'].iloc[-1])

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

    def add_onetime(self, db_engine, date, amount, description):
        '''Добавляет однократное событие.

        Args:
            db_engine: объект для работы с базой данных.
            date: дата транзакции.
            amount: сумма транзакции.
            description: описание транзакции.
        '''
        self.onetime_transactions = ee.add_onetime(
            db_engine, self.onetime_transactions, self.id, date, amount, description)


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
            {
                'plot': Сравнительный график прогноза с фактическим изменением баланса.
                'message': Ответное сообщение об успешном добавлении данных.
            }
        '''
        user = self.get_user(user_id)

        transactions = user.load_from_file(
            self.db_engine, file_full_name, dl.amount_parser(new_balance))
        comparison_data = user.get_comparison_data()

        return {
            'plot': Visual.comparison_plot(comparison_data),
            'message': Visual.successful_adding_transactions(transactions)
        }

    def predict_events(self, user_id, end_date):
        '''Прогнозирует регулярные и одноразовые транзакции для пользователя.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'category', 'description', 'balance']
        '''
        return self.get_user(user_id).predict_events(datetime.today(), end_date)

    def predict_full(self, user_id, end_date):
        '''Прогнозирует транзакции для пользователя. Регулярные и предсказанные транзакции складываются.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм транзакций с колонками ['amount', 'balance']
        '''
        return self.get_user(user_id).predict_full(end_date)

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
        return self.get_user(user_id).fit_new_model(self.db_engine)

    def report_events_and_transactions(self, user_id, end_date):
        '''Прогнозирует транзакции пользователя, строит графики.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            {
                'plot': График прогноза баланса.
                'message': Список регулярных транзакций и средние расходы в день.
            }
        '''
        events = self.predict_events(user_id, end_date).set_index('date')[
            ['amount', 'description']]

        full_transactions = self.predict_full(user_id, end_date)

        return {
            'plot': Visual.transactions_plot(full_transactions),
            'message': Visual.predict_info(events, self.get_user(user_id).predicted_transactions)
        }

    def add_onetime(self, user_id, date, amount, description):
        '''Добавляет однократное событие.

        Args:
            user_id: id пользователя.
            date: дата транзакции.
            amount: сумма транзакции.
            description: описание транзакции.
        '''
        self.get_user(user_id).add_onetime(self.db_engine, dl.ru_datetime_parser(
            date), dl.amount_parser(amount), description)

    def show_onetime(self, user_id, only_relevant=True):
        '''Добавляет однократное событие.

        Args:
            user_id: id пользователя.
            only_relevant: если True, вернет только будущие собития.
        '''
        user = self.get_user(user_id)
        return Visual.show_onetime(user.onetime_transactions, only_relevant)
