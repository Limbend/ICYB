from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext, CallbackQueryHandler, MessageHandler, Filters
import json
import logging
import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from Manager import UserManager


logging.basicConfig(format='%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s',
                    level=logging.INFO, filename='.np.log')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)

L_TYPE = os.getenv('ICYB_L_TYPE', 'TEST')
with open('./settings.np.json') as f:
    settings = json.load(f)

manager = UserManager(settings['db_connector'])


def ping(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(
        f'pong {update.effective_user.first_name}', quote=True)


def reset(update: Update, context: CallbackContext) -> None:
    if update.message.from_user.id == settings['trusted_chat_id']:
        global manager
        manager = UserManager(settings['db_connector'])


def forecast(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    if len(context.args) > 0 and context.args[0].isdigit():
        months = int(context.args[0])
        if months > 9 or months < 1:
            months = 1
    else:
        months = 1

    report_obj = manager.report_events_and_transactions(
        user_id, datetime.today() + relativedelta(months=months))
    update.message.reply_photo(photo=report_obj['plot'], quote=True)
    update.message.reply_text(
        text=report_obj['message'], quote=False, parse_mode='html')


def refit(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    result = manager.fit_new_model(user_id)
    update.message.reply_text(f'OK!\n{result}')


def bot_dialog(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    manager.bot_dialog(user_id, update)


def keyboard_callback(update: Update, context: CallbackContext) -> None:
    user_id = update.callback_query.message.chat_id
    manager.bot_dialog_keyboard(user_id, update)


def message(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    manager.bot_dialog(user_id, update)


updater = Updater(settings[L_TYPE+'-bot_token'])

updater.dispatcher.add_handler(CommandHandler('pred', forecast))
updater.dispatcher.add_handler(CommandHandler('ping', ping))
updater.dispatcher.add_handler(CommandHandler('reset', reset))
updater.dispatcher.add_handler(CommandHandler('refit', refit))
updater.dispatcher.add_handler(
    CommandHandler(['regular', 'onetime', 'accounts', 'transactions', 'tr'], bot_dialog))
updater.dispatcher.add_handler(MessageHandler(Filters.text, message))
updater.dispatcher.add_handler(CallbackQueryHandler(keyboard_callback))

updater.start_polling()
updater.idle()
