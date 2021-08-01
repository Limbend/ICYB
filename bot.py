from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
import json
import logging

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import DataLoader as dl
import EventEngine as ee
import Visual


logging.basicConfig(format='%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s',
                     level=logging.INFO, filename='log.np.txt')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)-12s - %(name)-12s - %(levelname)-8s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)



with open('./public/settings.np.json') as f:
    settings = json.load(f)

db_engine = dl.DB_Engine(**settings['db_connector'])








def ping(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(f'pong {update.effective_user.first_name}')

def download_file(update: Update, context: CallbackContext) -> None:
    balance = float(context.args[0])
    user_id = update.message.from_user.id

    file_received = update.message.reply_to_message.document
    file_received.get_file().download(custom_path = './public/temp/' + file_received.file_name)
    df = dl.tinkoff_file_parse('./public/temp/' + file_received.file_name, db_engine, user_id)
    df['balance'] = ee.get_balance_past(balance, df['amount'])

    file_rows = len(df)

    df = ee.get_full_costs_list(df, db_engine, user_id)
    added_rows = ee.save_new_costs(df, db_engine, user_id)
    
    update.message.reply_text(f'Из файла загружено:\t{file_rows}\nИз которых новых:\t{added_rows}\nВсего:\t\t\t\t{len(df)}')


    # # !!! TEST
    # regular_events = ee.get_regular_events(
    #     ee.get_updated_regular(
    #         df,
    #         db_engine.download_regular(user_id)
    #     ), date.today(), date(2021,10,1))

    # regular_events['balance'] = ee.get_balance_future(balance, regular_events['amount'])
    
    # Visual.regular_events_plot(regular_events, "./public/temp/output.np.png")
    # update.message.reply_photo(photo=open('./public/temp/output.np.png', 'rb'), quote=True)

    # Visual.df_to_image(regular_events[['date','amount','description','balance']], "./public/temp/output.np.png")
    # update.message.reply_photo(photo=open('./public/temp/output.np.png', 'rb'), quote=True)

def reply_plot(update: Update, context: CallbackContext) -> None:
    update.message.reply_photo(photo=open('output.np.png', 'rb'), quote=True)

updater = Updater(settings['bot_token'])

updater.dispatcher.add_handler(CommandHandler('plot', reply_plot))
updater.dispatcher.add_handler(CommandHandler('ping', ping))
updater.dispatcher.add_handler(CommandHandler('file', download_file))

updater.start_polling()
updater.idle()