from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
import DataLoader as dl
import shlex
import time
from datetime import date, datetime
import Visual
from Users import User


class BotDialog:
    def __init__(self, user):
        self.cmd_mask = None
        self.user = user
        self.is_wait_answer = False

    def is_suitable(self, cmd):
        if self.cmd_mask is None:
            return False
        return cmd == self.cmd_mask

    def reply_help(self, update: Update, cmd):
        update.message.reply_text(
            text=Visual.reply_help(' '.join([self.cmd_mask, cmd])),
            quote=True, parse_mode='html')

    def new_message(self, update: Update, db_engine: dl.DB_Engine):
        update.message.reply_text(
            text='!!! Стандартный ответ',
            quote=False)


class BotDialogRegular(BotDialog):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = '/regular'

        self.parameters = {
            'description': 'Описание\nМаксимум 25 символов',
            # 'search_f': '',
            # 'arg_sf': '',
            'amount': 'Сумма\nВ формате: 1000.00',
            'start_date': 'Начальная дата\nВ формате: 30.12.2200',
            'end_date': 'Начальная дата\nВ формате: 30.12.2200',
            'd_years': 'Количество лет между транзакциями\nЦелое число',
            'd_months': 'Количество месяцев между транзакциями\nЦелое число',
            'd_days': 'Количество дней между транзакциями\nЦелое число',
            # 'adjust_price': '',
            # 'adjust_date': '',
            # 'follow_overdue': ''
        }

    def reply_table(self, update: Update, columns=['description', 'amount'], only_relevant=True):
        update.message.reply_text(
            text=Visual.show_regular(
                self.user.regular_list, only_relevant, columns),
            quote=False, parse_mode='html')

    def reply_row(self, update: Update, index,
                  columns=[
                      'description',
                      'search_f',
                      'arg_sf',
                      'amount',
                      'start_date',
                      'end_date',
                      'd_years',
                      'd_months',
                      'd_days',
                      'adjust_price',
                      'adjust_date',
                      'follow_overdue'
                  ]):
        update.message.reply_text(
            text=Visual.show_regular(
                self.user.regular_list, False, columns, index),
            quote=False, parse_mode='html')

    def reply_add(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) < 4 or cmd[0] == 'help':
            self.reply_help(update, 'add')
            return

        if '-' in cmd[0]:
            date = cmd[0].split('-')
            start_date = dl.ru_datetime_parser(date[0])
            end_date = dl.ru_datetime_parser(date[1])
            if start_date == 'NaT' or end_date == 'NaT':
                self.reply_help(update, 'add')
                return
        else:
            start_date = dl.ru_datetime_parser(cmd[0])
            end_date = None
            if start_date == 'NaT':
                self.reply_help(update, 'add')
                return

        delta = [int(s) for s in cmd[1].split(',')]
        if len(delta) != 3:
            self.reply_help(update, 'add')
            return

        description = cmd[2]

        amount = dl.amount_parser(cmd[3])

        if len(cmd) >= 6:
            search_f = cmd[4]
            arg_sf = cmd[5]
        else:
            search_f = 'dont_search'
            arg_sf = None

        if len(cmd) == 9:
            adjust_price = cmd[6] == '1' or cmd[6].lower() == 'true'
            adjust_date = cmd[7] == '1' or cmd[7].lower() == 'true'
            follow_overdue = cmd[8] == '1' or cmd[8].lower() == 'true'
        else:
            adjust_price = False
            adjust_date = False
            follow_overdue = False

        self.user.add_regular(db_engine, start_date, end_date, delta,
                              description, amount, search_f, arg_sf, adjust_price, adjust_date, follow_overdue)
        self.reply_row(update, self.user.regular_list.index[-1])

    def reply_delete(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) < 1 or cmd[0] == 'help':
            self.reply_help(update, 'del')
            return
        self.user.delete_regular(
            db_engine, [int(s) for s in cmd[0].split(',')])
        self.reply_table(update)

    def reply_edit(self, update: Update, cmd: list, db_engine: dl.DB_Engine, message: Message = None):
        if len(cmd) < 1:
            self.reply_help(update, 'edit')

        elif len(cmd) == 1:
            # if hasattr(update, 'callback_query'):
            if not update.callback_query is None:
                update.callback_query.message.edit_text(
                    text='Что изменить у этой транзакции', reply_markup=self.__get_edit_menu(id_event=int(cmd[0])))
            else:
                update.message.reply_text(
                    text='Что изменить у этой транзакции', reply_markup=self.__get_edit_menu(id_event=int(cmd[0])))

        elif len(cmd) == 2:
            self.__reply_edit_parameter(update, int(cmd[0]), cmd[1])

            # message.edit_text(query.data)

    def new_message(self, update: Update, db_engine: dl.DB_Engine):
        command = shlex.split(update.message.text)
        if command[0][0] != '/' and self.is_wait_answer:
            self.is_wait_answer = False
            self.wait_answer_func(
                update, command, db_engine, **self.wait_answer_kwargs)
        if len(command) > 1:
            if command[1] == 'show':
                if len(command) == 3:
                    if command[2].isdigit():
                        self.reply_row(update, int(command[2]))
                        return
                    else:
                        self.reply_table(
                            update, only_relevant=not(command[2] == 'all'))
                        return
                else:
                    self.reply_table(update)
                    return

            elif command[1] == 'add':
                self.reply_add(update, command[2:], db_engine)
                return

            elif command[1] == 'del':
                self.reply_delete(update, command[2:], db_engine)
                return

            elif command[1] == 'edit':
                self.reply_edit(update, command[2:], db_engine)
                return

        else:
            self.reply_table(update)
            return

    def keyboard_callback(self, update: Update, db_engine: dl.DB_Engine):
        cmd = update.callback_query.data.split(',')
        if cmd[1] == 'edit':
            self.reply_edit(update, cmd[2:], db_engine,
                            message=update.callback_query.message)

        else:
            raise Exception(
                f"{__class__} does not implement the processing of the '{cmd[1]}' command received from the keyboard.")

    def __get_edit_menu(self, id_event, n_cols=2):
        keyboard_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(name, callback_data=f'{self.cmd_mask},edit,{id_event},{name}')
                for name in self.parameters.keys()][i:i + n_cols]
            for i in range(0, len(self.parameters.keys()), n_cols)])

        return keyboard_markup

    def __reply_edit_parameter(self, update: Update, id_event, parameter):
        self.wait_answer_func = self.__edit_parameter
        self.wait_answer_kwargs = {'id_event': id_event,
                                   'parameter': parameter}
        self.is_wait_answer = True

        keyboard_markup = InlineKeyboardMarkup([[InlineKeyboardButton(
            'Назад', callback_data=f'{self.cmd_mask},edit,{id_event}')]])

        # if hasattr(update, 'callback_query'):
        if not update.callback_query is None:
            update.callback_query.message.edit_text(
                text=f'{self.parameters[parameter]}\n\nТекущее значение: {self.user.regular_list.loc[id_event,parameter]}', reply_markup=keyboard_markup)

        else:
            raise Exception(
                f"The {update.__class__} does not have an atrebut 'callback_query'. The algorithm without a keyboard has not yet been implemente")

    def __edit_parameter(self, update: Update, cmd: list, db_engine: dl.DB_Engine, id_event, parameter):
        try:
            self.user.edit_regular(db_engine, id_event,
                                   parameter, update.message.text)
        except ValueError:
            self.__reply_edit_parameter(update, id_event, parameter)


class BotDialogOnetime(BotDialogRegular):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = '/onetime'

        self.parameters = {
            'description': 'Описание\nМаксимум 25 символов',
            'date': 'Дата\nВ формате: 30.12.2200',
            'amount': 'Сумма\nВ формате: 1000.00',
        }

    def reply_table(self, update: Update, columns=['description', 'date', 'amount'], only_relevant=True):
        update.message.reply_text(
            text=Visual.show_onetime(
                self.user.onetime_transactions, only_relevant, columns),
            quote=False, parse_mode='html')

    def reply_row(self, update: Update, index, columns=['description', 'date', 'amount']):
        update.message.reply_text(
            text=Visual.show_onetime(
                self.user.onetime_transactions, False, columns, index),
            quote=False, parse_mode='html')

    def reply_add(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) != 3 or cmd[0] == 'help':
            self.reply_help(update, 'add')
            return

        date = dl.ru_datetime_parser(cmd[0])
        if date == 'NaT':
            self.reply_help(update, 'add')
            return

        amount = dl.amount_parser(cmd[1])

        description = cmd[2]

        self.user.add_onetime(db_engine, date, amount, description)
        self.reply_row(update, self.user.onetime_transactions.index[-1])

    def reply_delete(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) < 1 or cmd[0] == 'help':
            self.reply_help(update, 'del')
            return
        self.user.delete_onetime(
            db_engine, [int(s) for s in cmd[0].split(',')])
        self.reply_table(update)


class UserManager:
    '''Класс для управления пользователями.

    Attributes:
        db_engine: объект для работы с базой данных.
        user_dict: словарь пользователей.
        bot_dialog_dict: словарь BotDialog для пользовалетей.

    '''

    def __init__(self, db_settings):
        self.db_engine = dl.DB_Engine(**db_settings)
        self.user_dict = {}
        self.bot_dialog_dict = {}

    def get_user(self, user_id):
        '''Ищет и возвращает объект пользователя по его id 

        Args:
            user_id: id пользователя.

        Returns:
            Объект пользователя.
        '''
        if user_id in self.user_dict:
            return self.user_dict[user_id]
        else:
            new_user = User(user_id, self.db_engine)
            self.user_dict[user_id] = new_user
            return new_user

    def get_dialog(self, user_id, cmd):
        '''Ищет и возвращает объект диалога по id пользователя

        Args:
            user_id: id пользователя.
            cmd: команда.

        Returns:
            Объект пользователя.
        '''
        user = self.get_user(user_id)

        if user_id in self.bot_dialog_dict:
            bot_dialog = self.bot_dialog_dict[user_id]
            if cmd[0] == '/' and not(bot_dialog.is_suitable(cmd)):
                bot_dialog = self.__create_bot_dialog(cmd, user)
                self.bot_dialog_dict[user_id] = bot_dialog
        else:
            bot_dialog = self.__create_bot_dialog(cmd, user)
            self.bot_dialog_dict[user_id] = bot_dialog

        return bot_dialog

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
        # return self.get_user(user_id).predict_events(datetime.today(), end_date)
        return self.get_user(user_id).predict_events(datetime(2022, 8, 2), end_date)

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

    # def show_onetime(self, user_id, only_relevant=True):
    #     '''Добавляет однократное событие.

    #     Args:
    #         user_id: id пользователя.
    #         only_relevant: если True, вернет только будущие собития.
    #     '''
    #     user = self.get_user(user_id)
    #     return Visual.show_onetime(user.onetime_transactions, only_relevant)

    def bot_dialog(self, user_id, update):
        self.get_dialog(user_id, update.message.text.split(' ')
                        [0]).new_message(update, self.db_engine)

    def bot_dialog_keyboard(self, user_id, update):
        cmd = update.callback_query.data.split(',')
        self.get_dialog(user_id, cmd[0]).keyboard_callback(
            update, self.db_engine)

    def __create_bot_dialog(self, cmd, user):
        if cmd == '/regular':
            return BotDialogRegular(user)
        elif cmd == '/onetime':
            return BotDialogOnetime(user)

        return BotDialog(user)
