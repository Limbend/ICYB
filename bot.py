from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
import json
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

with open('settings.np.json') as f:
    settings = json.load(f)





def ping(update: Update, context: CallbackContext) -> None:
    update.message.reply_text(f'pong {update.effective_user.first_name}')

def reply_plot(update: Update, context: CallbackContext) -> None:
    update.message.reply_photo(photo=open('output.np.png', 'rb'), quote=True)

updater = Updater(settings['bot_token'])

updater.dispatcher.add_handler(CommandHandler('plot', reply_plot))
updater.dispatcher.add_handler(CommandHandler('ping', ping))

updater.start_polling()
updater.idle()