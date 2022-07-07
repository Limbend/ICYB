from telegram import Update
import DataLoader as dl
import shlex
import Visual


def create_bot_dialog(update: Update, user):
    command = update.message.text.split()
    if command[0] == '/regular':
        return BotDialogRegular(user)
    elif command[0] == '/onetime':
        return BotDialogOnetime(user)

    return BotDialog(user)


class BotDialog:
    def __init__(self, user):
        self.cmd_mask = None
        self.user = user

    def is_not_suitable(self, update: Update):
        if self.cmd_mask is None:
            return False

        command = update.message.text.split()
        return command[0][0] == '/' and command[0] != self.cmd_mask

    def new_message(self, update: Update):
        update.message.reply_text(
            text='!!! Стандартный ответ',
            quote=False)


class BotDialogRegular(BotDialog):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = '/regular'

    def reply_table(self, update: Update, columns=['description', 'amount'], only_relevant=True):
        update.message.reply_text(
            text=Visual.show_regular(
                self.user.regular_list, only_relevant, columns),
            quote=False)

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
            quote=False)

    def new_message(self, update: Update):
        command = shlex.split(update.message.text)
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

        else:
            self.reply_table(update)
            return


class BotDialogOnetime(BotDialogRegular):
    def __init__(self, user):
        BotDialog.__init__(self, user)
        self.cmd_mask = '/onetime'

    def reply_table(self, update: Update, columns=['date', 'description', 'amount'], only_relevant=True):
        update.message.reply_text(
            text=Visual.show_onetime(
                self.user.onetime_transactions, only_relevant, columns),
            quote=False)

    def reply_row(self, update: Update, index, columns=['date', 'description', 'amount']):
        update.message.reply_text(
            text=Visual.show_onetime(
                self.user.onetime_transactions, False, columns, index),
            quote=False)
