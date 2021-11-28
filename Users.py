import DataLoader as dl
import EventEngine as ee
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import time
import Visual


class User:
    '''Класс для пользователя.

    Attributes:
        id: id пользователя.
        costs: список расходов.
        model: модель для прогноза расходов для этого пользователя.
        regular_list: список регулярных расходов.
        predicted_regular: рассчитанные регулярные расходы до указанной даты.
    '''

    def __init__(self, id, db_engine):
        self.id = id
        self.download_full_info(db_engine)

    def download_full_info(self, db_engine):
        '''Загружает всю информацию из базы данных

        Args:
            db_engine: объект для работы с базой данных.
        '''
        self.costs = db_engine.download_costs(self.id)

        self.regular_list = db_engine.download_regular(self.id)

        self.model = db_engine.download_last_model(self.id)

    def load_from_file(self, db_engine, file_full_name, new_balance):
        '''Загружает, обрабатывает и сохраняет расходы из файла. Соединяет новую информацию из файла с расходами сохраненными в базу до этого

        Args:
            db_engine: объект для работы с базой данных.
            file_full_name: полное имя файла.
            new_balance: текущий баланс пользователя, после последней операции в файле.

        Returns:
            Датафрейм всех расходов с колонками ['date', 'amount', 'category', 'description', 'balance', 'is_new']
            где is_new == True если эта строка из файла.
        '''
        self.costs = dl.tinkoff_file_parse(file_full_name, db_engine, self.id)
        self.costs['balance'] = ee.get_balance_past(
            new_balance, self.costs['amount'])
        self.costs = ee.get_all_costs(self.costs, db_engine, self.id)
        # self.loaded = True
        ee.save_new_costs(self.costs, db_engine, self.id)
        return self.costs

    def predict_regular(self, end_date):
        '''Прогнозирует регулярные расходы.

        Args:
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм регулярных расходов с колонками ['amount', 'category', 'description', 'balance']
        '''
        self.predicted_regular = ee.predict_regular_events(
            self.regular_list, self.costs, end_date)
        return self.predicted_regular

    def predict_full(self, end_date, start_date=datetime(2020, 11, 21)):
        '''Прогнозирует расходы. Регулярные и предсказанные расходы складываются.

        Args:
            end_date: дата до которой строить прогноз.
            start_date: начальная дата с которой использовать данные для обучения.

        Returns:
            Датафрейм расходов с колонками ['amount', 'balance']
        '''
        return ee.get_full_costs(
            self.predicted_regular,
            ee.preprocessing_for_ml(self.costs, self.regular_list, start_date),
            self.costs['balance'].iloc[-1],
            self.model,
            end_date
        )

    def fit_new_model(self, db_engine, start_date=datetime(2020, 11, 21)):
        '''Создает, учит и сохраняет модель для пользователя.

        Args:
            db_engine: объект для работы с базой данных.
            start_date: начальная дата с которой использовать данные для обучения.

        Returns:
            Отчет о обучении модели. Словарь формата {'time', 'event_count', 'ml_event_count'}
            time: время в секундах, потребовавшиеся для обучения модели.
            event_count: всего событий в базе.
            ml_event_count: события участвующие в обучении модели.
        '''
        start_time = time.time()
        data = ee.preprocessing_for_ml(
            self.costs, self.regular_list, start_date)

        self.model = ee.fit_new_model(data)

        time_passed = time.time() - start_time
        db_engine.upload_model(self.id, self.model)

        return {'time': time_passed, 'event_count': len(self.costs), 'ml_event_count': len(data)}


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
        new_u.download_full_info(self.db_engine)
        self.user_list.append(new_u)
        return new_u

    def load_from_file(self, user_id, file_full_name, new_balance):
        '''Загружает, обрабатывает и сохраняет расходы из файла. Соединяет новую информацию из файла с расходами сохраненными в базу до этого

        Args:
            user_id: id пользователя.
            file_full_name: полное имя файла.
            new_balance: текущий баланс пользователя, после последней операции в файле.

        Returns:
            Датафрейм всех расходов с колонками ['date', 'amount', 'category', 'description', 'balance', 'is_new']
            где is_new == True если эта строка из файла.
        '''
        return self.get_user(user_id).load_from_file(self.db_engine, file_full_name, new_balance)

    def predict_regular(self, user_id, end_date):
        '''Прогнозирует регулярные расходы для пользователя.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм регулярных расходов с колонками ['amount', 'category', 'description', 'balance']
        '''
        user = self.get_user(user_id)
        return user.predict_regular(end_date)

    def predict_full(self, user_id, end_date):
        '''Прогнозирует расходы для пользователя. Регулярные и предсказанные расходы складываются.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Датафрейм расходов с колонками ['amount', 'balance']
        '''
        user = self.get_user(user_id)

        return user.predict_full(end_date)

    def fit_new_model(self, user_id):
        '''Создает, учит и сохраняет модель для пользователя.

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
        '''Прогнозирует расходы пользователя, строит графики.

        Args:
            user_id: id пользователя.
            end_date: дата до которой строить прогноз.

        Returns:
            Словарь с изображениями в двоичном формате.
        '''
        regular_events = self.predict_regular(user_id, end_date)
        full_costs = self.predict_full(user_id, end_date)

        return {
            'costs': Visual.costs_plot(full_costs),
            'regular': Visual.df_to_text(regular_events[['amount', 'description']])
            # 'regular': Visual.df_to_image(regular_events[['amount', 'description']], './temp/output.np.png')
        }
