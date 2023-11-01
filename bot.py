from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackContext, CallbackQueryHandler, MessageHandler, filters
import json
import logging
import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from Manager import UserManager


log_format = '%(asctime)-23s - %(levelname)-8s - %(name)-24s - %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO, filename='.np.log')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(logging.Formatter(log_format))
logging.getLogger('').addHandler(console)
logger = logging.getLogger('main_bot')
logging.getLogger("httpx").setLevel(logging.WARNING)

L_TYPE = os.getenv('ICYB_L_TYPE', 'TEST')
with open('./settings.np.json') as f:
    settings = json.load(f)

logger.info(f'{L_TYPE=}')


async def ping(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text(
        f'pong {update.effective_user.first_name}', quote=True)


async def reset(update: Update, context: CallbackContext) -> None:
    if update.message.from_user.id == settings['trusted_chat_id']:
        global manager
        manager = UserManager(app.bot, settings['db_connector'])


async def forecast(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    if len(context.args) > 0 and context.args[0].isdigit():
        months = int(context.args[0])
        if months > 9 or months < 1:
            months = 1
    else:
        months = 1

    report_obj = manager.report_events_and_transactions(
        user_id, datetime.today() + relativedelta(months=months))
    await update.message.reply_photo(photo=report_obj['plot'], quote=True)
    await update.message.reply_text(
        text=report_obj['message'], quote=False, parse_mode='html')


async def refit(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id

    result = manager.fit_new_model(user_id)
    await update.message.reply_text(f'OK!\n{result}')


async def bot_dialog(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    await manager.bot_dialog(user_id, update)


async def keyboard_callback(update: Update, context: CallbackContext) -> None:
    user_id = update.callback_query.message.chat_id
    await manager.bot_dialog_keyboard(user_id, update)


async def message(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    await manager.bot_dialog(user_id, update)


app = ApplicationBuilder().token(settings[L_TYPE+'-bot_token']).build()

app.add_handler(CommandHandler('pred', forecast))
app.add_handler(CommandHandler('ping', ping))
app.add_handler(CommandHandler('reset', reset))
app.add_handler(CommandHandler('refit', refit))
app.add_handler(
    CommandHandler(['help', 'regular', 're', 'onetime', 'on', 'accounts', 'ac', 'transactions', 'tr'], bot_dialog))
app.add_handler(MessageHandler(filters.TEXT, message))
app.add_handler(CallbackQueryHandler(keyboard_callback))
manager = UserManager(app.bot, settings['db_connector'])

app.run_polling()
