from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
import DataLoader as dl
import shlex
from dateutil.relativedelta import relativedelta
from datetime import date, datetime
import Visual
from Users import User


class BotDialog:
    def __init__(self, user):
        self.cmd_mask = None
        self.user = user
        self.is_wait_answer = False
        self.wait_answer_kwargs = {}

    def is_suitable(self, cmd):
        if self.cmd_mask is None:
            return False

        if type(self.cmd_mask) is list:
            return cmd in self.cmd_mask

        return cmd == self.cmd_mask

    def get_cmd_mask(self):
        if type(self.cmd_mask) is list:
            return self.cmd_mask[0]
        else:
            return self.cmd_mask

    async def reply_help(self, message: Message, cmd, edit_text=False, **kwargs):
        text = Visual.reply_help(' '.join([self.get_cmd_mask(), cmd]))

        if edit_text:
            await message.edit_text(
                text=text, parse_mode='html', **kwargs)
        else:
            await message.reply_text(
                text=text, quote=True, parse_mode='html', **kwargs)

    async def reply_error(self, message: Message, path, error_message, edit_text=False, **kwargs):
        if path == '':
            text = f'{self.get_cmd_mask()}: {error_message}'
        else:
            text = f'{self.get_cmd_mask()} {path}: {error_message}'
        text = Visual.reply_error(text)

        if edit_text:
            await message.edit_text(
                text=text, parse_mode='html', **kwargs)
        else:
            await message.reply_text(
                text=text, quote=True, parse_mode='html', **kwargs)

    async def new_message(self, update: Update, db_engine: dl.DB_Engine, command=''):
        if command == '':
            command = shlex.split(update.message.text)

        if self.is_wait_answer:
            self.is_wait_answer = False
            if command[0][0] != '/':
                self.is_wait_answer = False
                if 'prefix_command' in self.wait_answer_kwargs:
                    command = self.wait_answer_kwargs.pop(
                        'prefix_command') + command
                await self.wait_answer_func(
                    update, command, db_engine, **self.wait_answer_kwargs)
                return False
        else:
            if len(command) > 1 and command[1] == 'help':
                await self.reply_help(update.message, '')
                return False

            if command[0][0] not in ['\\', '/']:
                command.insert(0, '\\')
        return command

    async def keyboard_callback(self, update: Update, db_engine: dl.DB_Engine):
        cmd = shlex.split(update.callback_query.data)
        # update.message = update.effective_message
        await self.new_message(update, db_engine, cmd)


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
            'end_date': ' Конечная дата\nВ формате: 30.12.2200',
            'd_years': 'Количество лет между транзакциями\nЦелое число',
            'd_months': 'Количество месяцев между транзакциями\nЦелое число',
            'd_days': 'Количество дней между транзакциями\nЦелое число',
            # 'adjust_price': '',
            # 'adjust_date': '',
            # 'follow_overdue': ''
        }

    async def reply_table(self, update: Update, columns=['description', 'amount'], only_relevant=True):
        await update.message.reply_text(
            text=Visual.show_regular(
                self.user.regular_list, only_relevant, columns),
            quote=False, parse_mode='html')

    async def reply_row(self, update: Update, index,
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
        await update.message.reply_text(
            text=Visual.show_regular(
                self.user.regular_list, False, columns, index),
            quote=False, parse_mode='html')

    async def reply_add(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) < 4 or cmd[0] == 'help':
            await self.reply_help(update.message, 'add')
            return

        if '-' in cmd[0]:
            date = cmd[0].split('-')
            start_date = dl.ru_datetime_parser(date[0])
            end_date = dl.ru_datetime_parser(date[1])
            if start_date == 'NaT' or end_date == 'NaT':
                await self.reply_help(update.message, 'add')
                return
        else:
            start_date = dl.ru_datetime_parser(cmd[0])
            end_date = None
            if start_date == 'NaT':
                await self.reply_help(update.message, 'add')
                return

        delta = [int(s) for s in cmd[1].split(',')]
        if len(delta) != 3:
            await self.reply_help(update.message, 'add')
            return

        amount = dl.amount_parser(cmd[2])

        description = cmd[3]

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
        await self.reply_row(update, self.user.regular_list.index[-1])

    async def reply_delete(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) < 1 or cmd[0] == 'help':
            await self.reply_help(update.message, 'del')
            return
        self.user.delete_regular(
            db_engine, [int(s) for s in cmd[0].split(',')])
        await self.reply_table(update)

    async def reply_edit(self, update: Update, cmd: list, db_engine: dl.DB_Engine, message: Message = None):
        if len(cmd) < 1 or (len(cmd) == 1 and cmd[0] == 'help'):
            await self.reply_help(update.message, 'edit')

        elif len(cmd) == 1:
            # if hasattr(update, 'callback_query'):
            if not update.callback_query is None:
                await update.callback_query.message.edit_text(
                    text='Что изменить у этой транзакции', reply_markup=self.__get_edit_menu(id_event=int(cmd[0])))
            else:
                await update.message.reply_text(
                    text='Что изменить у этой транзакции', reply_markup=self.__get_edit_menu(id_event=int(cmd[0])))

        elif len(cmd) == 2:
            await self.__reply_edit_parameter(update, int(cmd[0]), cmd[1])

    async def new_message(self, update: Update, db_engine: dl.DB_Engine, command=''):
        command = await BotDialog.new_message(self, update, db_engine, command)
        if command == False:
            return

        if len(command) > 1:
            if command[1] == 'show':
                if len(command) == 3:
                    if command[2].isdigit():
                        await self.reply_row(update, int(command[2]))
                        return
                    else:
                        await self.reply_table(
                            update, only_relevant=not (command[2] == 'all'))
                        return
                else:
                    await self.reply_table(update)
                    return

            elif command[1] == 'add':
                await self.reply_add(update, command[2:], db_engine)
                return

            elif command[1] == 'del':
                await self.reply_delete(update, command[2:], db_engine)
                return

            elif command[1] == 'edit':
                await self.reply_edit(update, command[2:], db_engine)
                return

        else:
            await self.reply_table(update)
            return

    # def keyboard_callback(self, update: Update, db_engine: dl.DB_Engine):
    #     cmd = update.callback_query.data.split(',')
    #     if cmd[1] == 'edit':
    #         self.reply_edit(update, cmd[2:], db_engine,
    #                         message=update.callback_query.message)

    #     else:
    #         raise Exception(
    #             f"{__class__} does not implement the processing of the '{cmd[1]}' command received from the keyboard.")

    def __get_edit_menu(self, id_event, n_cols=2):
        keyboard_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(name, callback_data=f'{self.get_cmd_mask()} edit {id_event} {name}')
                for name in self.parameters.keys()][i:i + n_cols]
            for i in range(0, len(self.parameters.keys()), n_cols)])

        return keyboard_markup

    async def __reply_edit_parameter(self, update: Update, id_event, parameter):
        self.wait_answer_func = self.__edit_parameter
        self.wait_answer_kwargs = {'id_event': id_event,
                                   'parameter': parameter}
        self.is_wait_answer = True

        keyboard_markup = InlineKeyboardMarkup([[InlineKeyboardButton(
            'Назад', callback_data=f'{self.get_cmd_mask()} edit {id_event}')]])

        # if hasattr(update, 'callback_query'):
        if not update.callback_query is None:
            await update.callback_query.message.edit_text(
                text=f'{self.parameters[parameter]}\n\nТекущее значение: {self.user.regular_list.loc[id_event, parameter]}', reply_markup=keyboard_markup)

        else:
            raise Exception(
                f"The {update.__class__} does not have an atrebut 'callback_query'. The algorithm without a keyboard has not yet been implemente")

    async def __edit_parameter(self, update: Update, cmd: list, db_engine: dl.DB_Engine, id_event, parameter):
        try:
            value = update.message.text
            if parameter in ['start_date', 'end_date']:
                value = dl.ru_datetime_parser(update.message.text)
            self.user.edit_regular(db_engine, id_event,
                                   parameter, value)
        except ValueError:
            await self.__reply_edit_parameter(update, id_event, parameter)


class BotDialogOnetime(BotDialogRegular):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = '/onetime'

        self.parameters = {
            'description': 'Описание\nМаксимум 25 символов',
            'date': 'Дата\nВ формате: 30.12.2200',
            'amount': 'Сумма\nВ формате: 1000.00',
        }

    async def reply_table(self, update: Update, columns=['description', 'date', 'amount'], only_relevant=True):
        await update.message.reply_text(
            text=Visual.show_onetime(
                self.user.onetime_transactions, only_relevant, columns),
            quote=False, parse_mode='html')

    async def reply_row(self, update: Update, index, columns=['description', 'date', 'amount']):
        await update.message.reply_text(
            text=Visual.show_onetime(
                self.user.onetime_transactions, False, columns, index),
            quote=False, parse_mode='html')

    async def reply_add(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) != 3 or cmd[0] == 'help':
            await self.reply_help(update.message, 'add')
            return

        date = dl.ru_datetime_parser(cmd[0])
        if date == 'NaT':
            await self.reply_help(update.message, 'add')
            return

        amount = dl.amount_parser(cmd[1])

        description = cmd[2]

        self.user.add_onetime(db_engine, date, amount, description)
        await self.reply_row(update, self.user.onetime_transactions.index[-1])

    async def reply_delete(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) < 1 or cmd[0] == 'help':
            await self.reply_help(update.message, 'del')
            return
        self.user.delete_onetime(
            db_engine, [int(s) for s in cmd[0].split(',')])
        await self.reply_table(update)


class BotDialogAccounts(BotDialogOnetime):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = '/accounts'

        self.parameters = {
            'type': 'Тип счета\n0 для дебетового и 1 для кредитного',
            'description': 'Название\nМаксимум 25 символов',
            'credit_limit': 'Кредитный лимит\nВ формате: 1000.00',
            'discharge_day': 'День выписки\nЦелое число',
        }

    async def reply_table(self, update: Update, columns=['type', 'description', 'credit_limit', 'discharge_day']):
        await update.message.reply_text(
            text=Visual.show_accounts(
                self.user.accounts, columns),
            quote=False, parse_mode='html')

    async def reply_row(self, update: Update, index, columns=['type', 'description', 'credit_limit', 'discharge_day']):
        await update.message.reply_text(
            text=Visual.show_accounts(
                self.user.onetime_transactions, columns, index),
            quote=False, parse_mode='html')

    async def reply_add(self, update: Update, cmd, db_engine: dl.DB_Engine):
        if len(cmd) == 1 and cmd[0] == 'help':
            await self.reply_help(update.message, 'add')
        elif len(cmd) == 0:
            await self.__set_account_description(update)
        elif len(cmd) == 1:
            await self.__set_account_type(update, cmd[0])
        elif len(cmd) == 2 and cmd[1] == 'debit':
            self.user.add_accounts(
                db_engine, account_type=1, description=cmd[0])
        elif len(cmd) == 2 and cmd[1] == 'credit':
            await self.__set_credit_limit(update, cmd[0])
        elif len(cmd) == 3 and cmd[1] == 'credit':
            await self.__set_discharge_day(update, cmd[0], cmd[2])
        elif len(cmd) == 4 and cmd[1] == 'credit':
            self.user.add_accounts(
                db_engine, account_type=2, description=cmd[0], credit_limit=cmd[2], discharge_day=cmd[3])

    async def reply_delete(self, update: Update, cmd, db_engine: dl.DB_Engine):
        pass
        # if len(cmd) < 1 or cmd[0] == 'help':
        #     self.reply_help(update.message, 'del')
        #     return
        # self.user.delete_onetime(
        #     db_engine, [int(s) for s in cmd[0].split(',')])
        # self.reply_table(update)

    async def __set_account_description(self, update: Update):
        names = ['Основной', 'Кредитка']
        keyboard_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(name, callback_data=f'{self.get_cmd_mask()} add {name}') for name in names]])

        self.wait_answer_func = self.reply_add
        self.is_wait_answer = True

        edit_text = not update.callback_query is None
        await self.reply_error(update.effective_message, 'add', 'description empty',
                               edit_text, reply_markup=keyboard_markup)

    async def __set_account_type(self, update: Update, description):
        types = ['debit', 'credit']
        keyboard_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t, callback_data=f'{self.get_cmd_mask()} add {description} {t}') for t in types]])

        edit_text = not update.callback_query is None
        await self.reply_error(update.effective_message, 'add', 'type empty',
                               edit_text, reply_markup=keyboard_markup)

    async def __set_credit_limit(self, update: Update, description):
        self.is_wait_answer = True
        self.wait_answer_func = self.reply_add
        self.wait_answer_kwargs = {'prefix_command': [description, 'credit']}

        await self.reply_error(update.effective_message, 'add', 'credit_limit empty')

    async def __set_discharge_day(self, update: Update, description, credit_limit):
        self.is_wait_answer = True
        self.wait_answer_func = self.reply_add
        self.wait_answer_kwargs = {'prefix_command': [description,
                                                      'credit', credit_limit]}

        await self.reply_error(update.effective_message, 'add', 'discharge_day empty')


class BotDialogTransactions(BotDialog):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = ['/transactions', '/tr']

    async def reply_add(self, update: Update, cmd, db_engine: dl.DB_Engine, file_received=None):
        if len(self.user.accounts) == 0:
            keyboard_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton('Создать счет', callback_data='/accounts add')]])
            await self.reply_error(update.message, 'add', 'accounts empty',
                                   reply_markup=keyboard_markup)
            return

        if file_received is None:
            if not update.callback_query is None and update.callback_query.message.reply_to_message is None:
                message = update.callback_query.message.reply_to_message
            elif not update.effective_message.reply_to_message is None:
                message = update.effective_message.reply_to_message
            else:
                message = update.message
            if not message.document is None:
                file_received = message.document
            else:
                await self.reply_error(update.message, 'add', 'file empty')
                return
        else:
            message = update.message

        if len(cmd) < 1:
            await self.reply_error(update.message, 'add', 'balance empty')
            return

        new_balance = cmd[0]

        if len(cmd) < 2:
            self.is_wait_answer = True
            self.wait_answer_func = self.reply_add
            self.wait_answer_kwargs = {'prefix_command': [new_balance],
                                       'file_received': file_received}

            keyboard_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton(account_name, callback_data=f'{self.get_cmd_mask()} add \'{new_balance}\' {account_name}')]
                 for account_name in self.user.accounts['description']])
            await self.reply_error(message, 'add', 'account not selected',
                                   reply_markup=keyboard_markup)
            return

        account = self.user.accounts['description'] == cmd[1]
        if account.any():
            account_id = self.user.accounts.loc[account, 'db_id'].values[0]
        else:
            account = self.user.accounts['db_id'] == cmd[1]
            if account.any():
                account_id = self.user.accounts.loc[account, 'db_id'].values[0]
            else:
                self.is_wait_answer = True
                self.wait_answer_func = self.reply_add
                self.wait_answer_kwargs = {'file_received': file_received}

                keyboard_markup = InlineKeyboardMarkup(
                    [[InlineKeyboardButton(account_name, callback_data=f'\'{new_balance}\' {account_name}')]
                     for account_name in self.user.accounts['description']])
                await self.reply_error(message, 'add', 'account not found',
                                       reply_markup=keyboard_markup)
                return

        path = f'./temp/{self.user.id}---{file_received.file_name}'
        # TODO Обработать исключение неудачной загрузки
        file = await file_received.get_file()
        await file.download_to_drive(custom_path=path)
        transactions = self.user.load_from_file(
            db_engine, path, account_id, dl.amount_parser(new_balance))
        comparison_data = self.user.get_comparison_data()

        await message.reply_text(
            text=Visual.successful_adding_transactions(transactions), quote=True)
        await message.reply_photo(
            photo=Visual.comparison_plot(comparison_data), quote=False)

    async def new_message(self, update: Update, db_engine: dl.DB_Engine, command=''):
        command = await BotDialog.new_message(self, update, db_engine, command)
        if command == False:
            return

        if len(command) > 1:
            if command[1] == 'add':
                await self.reply_add(update, command[2:], db_engine)
                return


class UserManager:
    '''Класс для управления пользователями.

    Attributes:
        db_engine: объект для работы с базой данных.
        user_dict: словарь пользователей.
        bot_dialog_dict: словарь BotDialog для пользовалетей.

    '''

    def __init__(self, bot, db_settings):
        self.db_engine = dl.DB_Engine(**db_settings)
        self.user_dict = {}
        self.bot_dialog_dict = {}
        self.bot = bot

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
            if cmd[0] == '/' and not (bot_dialog.is_suitable(cmd)):
                bot_dialog = self.__create_bot_dialog(cmd, user)
                self.bot_dialog_dict[user_id] = bot_dialog
        else:
            bot_dialog = self.__create_bot_dialog(cmd, user)
            self.bot_dialog_dict[user_id] = bot_dialog

        return bot_dialog

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
        events = self.predict_events(user_id, end_date).set_index('date')

        full_transactions = self.predict_full(user_id, end_date)

        return {
            'plot': Visual.transactions_plot(full_transactions),
            'message': Visual.predict_info(events, self.get_user(user_id).predicted_transactions)
        }

    async def bot_dialog(self, user_id, update):
        dialog = self.get_dialog(user_id, update.message.text.split(' ')[0])
        await dialog.new_message(update, self.db_engine)

    async def bot_dialog_keyboard(self, user_id, update):
        cmd = update.callback_query.data.split(' ')
        dialog = self.get_dialog(user_id, cmd[0])
        await dialog.keyboard_callback(update, self.db_engine)

    def daily_notice(self):
        print("daily_notice по расписанию")
        users_id = self.db_engine.get_users_for_notifications()
        for id in users_id:
            user = self.get_user(id)
            events_today = user.predict_events(
                datetime.today() - relativedelta(days=1), datetime.today())
            print(events_today)
            events_today = events_today.set_index('date')

            print(events_today)
            self.bot.send_message(id, Visual.show_events(
                events_today), parse_mode='html')

    def __create_bot_dialog(self, cmd, user):
        if cmd in ['/regular', '/re']:
            return BotDialogRegular(user)
        elif cmd in ['/onetime', '/on']:
            return BotDialogOnetime(user)
        elif cmd in ['/transactions', '/tr']:
            return BotDialogTransactions(user)
        elif cmd in ['/accounts', '/ac']:
            return BotDialogAccounts(user)

        return BotDialog(user)
