from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
import json
import logging

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from Users import UserManager


logging.basicConfig(format='%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s',
                    level=logging.INFO, filename='.np.log')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)


with open('./settings.np.json') as f:
    settings = json.load(f)

manager = UserManager(settings['db_connector'])


def ping(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(f'pong {update.effective_user.first_name}')


def download_file(update: Update, context: CallbackContext) -> None:
    balance = float(context.args[0])
    user_id = update.message.from_user.id

    file_received = update.message.reply_to_message.document
    file_received.get_file().download(
        custom_path='./temp/' + file_received.file_name)
    manager.load_from_file(user_id, './temp/' +
                           file_received.file_name, balance)


def monthly_forecast(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    report_obj = manager.get_report_obj(
        user_id, date.today() + relativedelta(months=1))
    update.message.reply_photo(photo=report_obj['costs'], quote=True)
    # update.message.reply_photo(photo=report_obj['regular'], quote=False)
    update.message.reply_text(text=report_obj['regular'], quote=False)


def refit(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    result = manager.fit_new_model(user_id)
    update.message.reply_text(f'OK!\n{result}')


updater = Updater(settings['bot_token'])

updater.dispatcher.add_handler(CommandHandler('pred', monthly_forecast))
updater.dispatcher.add_handler(CommandHandler('ping', ping))
updater.dispatcher.add_handler(CommandHandler('file', download_file))
updater.dispatcher.add_handler(CommandHandler('refit', refit))

updater.start_polling()
updater.idle()
