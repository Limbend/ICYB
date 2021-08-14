from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
import json
import logging

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

#import DataLoader as dl

#import EventEngine as ee
from Users import UserManager
import Visual


logging.basicConfig(format='%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s',
                    level=logging.INFO, filename='log.np.txt')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)


with open('./public/settings.np.json') as f:
    settings = json.load(f)

manager = UserManager(settings['db_connector'])


def ping(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(f'pong {update.effective_user.first_name}')


def download_file(update: Update, context: CallbackContext) -> None:
    balance = float(context.args[0])
    user_id = update.message.from_user.id

    file_received = update.message.reply_to_message.document
    file_received.get_file().download(
        custom_path='./public/temp/' + file_received.file_name)
    manager.load_from_file(user_id, './public/temp/' +
                           file_received.file_name, balance)


def reply_costs(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    regular_events = manager.predict_regular(user_id, date(2021, 9, 1))

    Visual.regular_events_plot(regular_events, "./public/temp/output.np.png")
    update.message.reply_photo(photo=open(
        './public/temp/output.np.png', 'rb'), quote=True)

    Visual.df_to_image(
        regular_events[['amount', 'description', 'balance']], "./public/temp/output.np.png")
    update.message.reply_photo(photo=open(
        './public/temp/output.np.png', 'rb'), quote=True)


def reply_costs(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    regular_events = manager.predict_regular(user_id, date(2021, 9, 1))
    full_costs = manager.predict_full(user_id, date(2021, 9, 1))

    Visual.regular_events_plot(regular_events, "./public/temp/output.np.png")
    update.message.reply_photo(photo=open(
        './public/temp/output.np.png', 'rb'), quote=True)

    Visual.df_to_image(
        regular_events[['amount', 'description', 'balance']], "./public/temp/output.np.png")
    update.message.reply_photo(photo=open(
        './public/temp/output.np.png', 'rb'), quote=True)

    Visual.full_events_plot(full_costs, "./public/temp/output.np.png")
    update.message.reply_photo(photo=open(
        './public/temp/output.np.png', 'rb'), quote=True)
    Visual.df_to_image(full_costs, "./public/temp/output.np.png")
    update.message.reply_photo(photo=open(
        './public/temp/output.np.png', 'rb'), quote=True)


def refit(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    result = manager.fit_new_model(user_id)
    update.message.reply_text(f'OK!\n{result}')


updater = Updater(settings['bot_token'])

updater.dispatcher.add_handler(CommandHandler('costs', reply_costs))
updater.dispatcher.add_handler(CommandHandler('full', reply_costs))
updater.dispatcher.add_handler(CommandHandler('ping', ping))
updater.dispatcher.add_handler(CommandHandler('file', download_file))
updater.dispatcher.add_handler(CommandHandler('refit', refit))

updater.start_polling()
updater.idle()
