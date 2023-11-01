from typing import overload
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.dates import DateFormatter
import seaborn as sns
from datetime import date, datetime
# import dataframe_image as dfi # dataframe-image==0.1.1
import io


FORMATTERS = {
    'date': lambda x: x.strftime('%d.%m.%Y'),
    'start_date': lambda x: x.strftime('%d.%m.%Y'),
    'end_date': lambda x: x.strftime('%d.%m.%Y') if not (pd.isna(x)) else 'NaT',
    'amount': "{:.2f}".format,
    # 'description': "{:<17}".format,
}


def transactions_plot(transactions):
    formatter = DateFormatter('%d.%m.%Y')

    sns.set(font_scale=1.4, style="whitegrid")
    plt.rcParams['figure.figsize'] = (19.5, 9)

    ax = sns.lineplot(data=transactions['balance'], linewidth=4.)
    ax.xaxis.set_major_formatter(formatter)

    ax.hlines(transactions['balance'].min(), transactions.index[0],
              transactions.index[-1], color='r', linewidth=3, linestyle='--')
    ax.text(
        transactions.head(1).index,
        transactions['balance'].min(),
        f"Минимальный баланс: {transactions['balance'].min():.2f}   ({transactions[transactions['balance'] == transactions['balance'].min()].index[0]:%d.%m.%Y})",
        color='w',
        backgroundcolor='b'
    )

    plt.subplots_adjust(left=0.08, right=0.97, top=.98, bottom=0.1)

    plot_b = io.BytesIO()
    plt.savefig(plot_b, format='png')
    plt.close()
    plot_b.seek(0)
    return plot_b


def comparison_plot(comparison):
    data = comparison.copy()
    data.columns = ['Реальный баланс', 'Прогноз баланса']
    formatter = DateFormatter('%d.%m.%Y')

    sns.set(font_scale=1.4, style="whitegrid")
    plt.rcParams['figure.figsize'] = (19.5, 9)

    ax = sns.lineplot(data=data, linewidth=4)
    ax.fill_between(comparison.index,
                    comparison['reab_b'], comparison['predicted_b'],
                    where=(comparison['reab_b'] >= comparison['predicted_b']),
                    facecolor='green',
                    alpha=.4)
    ax.fill_between(comparison.index,
                    comparison['reab_b'], comparison['predicted_b'],
                    where=(comparison['reab_b'] < comparison['predicted_b']),
                    facecolor='red',
                    alpha=.4)

    ax.xaxis.set_major_formatter(formatter)
    plt.subplots_adjust(left=0.08, right=0.97, top=.98, bottom=0.1)

    plot_b = io.BytesIO()
    plt.savefig(plot_b, format='png')
    plt.close()
    plot_b.seek(0)
    return plot_b


# def df_to_image(dataframe, image_full_name):
#     # dfi.export(dataframe, image_full_name, table_conversion='matplotlib') # не красивый вид, но не требует хром
#     dfi.export(dataframe, image_full_name)
#     return open(image_full_name, 'rb')


def show_table(data, columns):
    return '<pre>' + data[columns].to_string(
        formatters={key: FORMATTERS[key] for key in columns if key in FORMATTERS}) + '</pre>'


def show_row(data, index, columns):
    result = data.loc[[index], columns].copy()
    formatters = {key: FORMATTERS[key] for key in columns if key in FORMATTERS}

    result[list(formatters)] = result.apply(formatters)
    return '<pre>' + result.transpose().to_string() + '</pre>'


def show_events(events):
    result = events.copy()
    result.index = result.index.strftime('%d.%m.%Y')
    return 'Регулярные транзакции\n        ' + show_table(result, columns=['amount', 'description'])


def show_regular(regular, only_relevant, columns, index=None):
    result = regular.copy()
    if index is None:
        if only_relevant:
            result = result[(result['end_date'].isna()) | (
                result['end_date'] >= date.today())]
        return 'Регулярные транзакции\n        ' + show_table(result, columns)
    else:
        return f'Регулярная транзакция\n\n        ' + show_row(result, index, columns)


def show_onetime(onetime, only_relevant, columns, index=None):
    result = onetime.copy()
    if index is None:
        if only_relevant:
            result = result[result['date'] >= datetime.today()]
        return 'Разовые транзакции\n        ' + show_table(result, columns)
    else:
        return f'Разовая транзакция\n\n        ' + show_row(result, index, columns)


def show_accounts(accounts, columns, index=None):
    if index is None:
        return 'Счета\n        ' + show_table(accounts, columns)
    else:
        return f'Счет\n\n        ' + show_row(accounts, index, columns)


def successful_adding_transactions(transactions):
    return f"В базу успешно добавлено {len(transactions[transactions['is_new']])} транзакций.\nРазница прогноза и фактического баланса:"


def predict_info(events, predicted_transactions):
    data = events.copy()
    data.loc[data['is_overdue'], 'description'] = data.loc[data['is_overdue'],
                                                           'description'] + ' \u2757'  # ❗
    table = show_events(data)
    return table + f"\n\n\nДополнительно к этим транзакциям, средний расход в день составляет: {predicted_transactions['amount'].mean():.2f}"


HELP_MESSAGE = {
    '/help': 'Доступные команды для бота:\n<code>/help</code> - Данная справка\n<code>/pred</code> - Прогноз транзакций\n<code>/re</code> или <code>/regular</code> - Регулярные транзакции\n<code>/on</code> или <code>/onetime</code> - Разовые транзакции\n<code>/tr</code> или <code>/transactions</code> - Транзакции\n<code>/ac</code> или <code>/accounts</code> - Счета\n\nДля получения дополнительный информации добавьте <code>help</code> в конце команды.',

    '/regular': 'Регулярные транзакции\n<code>/regular show [all]</code> - Отобразить список [всех] транзакций\n<code>/regular show &#60;id&#62;</code> - Отобразить информацию по транзакции\n<code>/regular add</code> - Добавить новую транзакцию\n<code>/regular edit &#60;id&#62;</code> - Изменить транзакцию\n<code>/regular del &#60;id&#62;[,id]</code> - Удалить транзакцию',
    '/regular add': 'Для добавления новой регулярной транзакции введите команду <code>/regular add</code>, а затем, через пробел, укажите:\nначальную дату или начальную-конечную дату\nчерез запятую, без пробела, количество лет, месяцев и дней между транзакциями\nкомментарий\nсумму\n\nПример:\n<pre>/regular add 30.12.2200-30.12.3001 0,1,0 -6500.00 "Рассрочка за холодильник"</pre>\n<pre>/regular add 30.12 0,0,30 -450 "Мобильная связь"</pre>',
    '/regular del': 'Для удаления регулярной транзакции введите команду <code>/regular del</code>, указав номер транзакции или несколько номеров, через запятую, без пробелов.\n\nПример:\n<pre>/regular del 17</pre>\n<pre>/regular del 17,18,25</pre>',

    '/onetime': 'Регулярные транзакции\n<code>/onetime show [all]</code> - Отобразить список [всех] транзакций\n<code>/onetime show &#60;id&#62;</code> - Отобразить информацию по транзакции\n<code>/onetime add</code> - Добавить новую транзакцию\n<code>/onetime edit &#60;id&#62;</code> - Изменить транзакцию\n<code>/onetime del &#60;id&#62;[,id]</code> - Удалить транзакцию',
    '/onetime add': 'Для добавления новой разовой транзакции введите команду <code>/onetime add</code>, а затем, через пробел, укажите дату, сумму и комментарий.\n\nПример:\n<pre>/onetime add 30.12.2200 -652.50 "Вернуть долг"</pre>',
    '/onetime del': 'Для удаления разовой транзакции введите команду <code>/onetime del &#60;id&#62;[,id]</code>, указав номер транзакции или несколько номеров, через запятую, без пробелов.\n\nПример:\n<pre>/onetime del 17</pre>\n<pre>/onetime del 17,18,25</pre>',
}


def reply_help(cmd):
    return HELP_MESSAGE.get(cmd, f'!!! default help message for {cmd}')


ERROR_MESSAGE = {
    '/transactions add: accounts empty': 'Похоже у вас нет ни одного счета. Чтобы создать новый введите комманду <code>/accounts add</code> или нажмите на кнопку ниже.',
    '/transactions add: file empty': 'Укажите файл, ответив на сообщение с ним.',
    '/transactions add: balance empty': 'Необходимо указать баланс по счету.',
    '/transactions add: account not selected': 'Напишите название счета или выберете его ниже.',
    '/transactions add: account not found': 'Такого счета не найдено. Проверьте название или выберете счет ниже.',

    '/accounts add: description empty': 'Напишите название для нового счета или выберете из представленных ниже.',
    '/accounts add: type empty': 'Выберете тип счета.',
    '/accounts add: credit_limit empty': 'Напишите кредитный лимит в формате: 1000.00',
    '/accounts add: discharge_day empty': 'Напишите день выписки. Одним числом.',
}


def reply_error(cmd):
    return ERROR_MESSAGE.get(cmd, f'!!! default error message for {cmd}')
