import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.dates import DateFormatter
import seaborn as sns
from datetime import date, datetime
# import dataframe_image as dfi # dataframe-image==0.1.1
import io

from sqlalchemy import column


FORMATTERS = {
    'date': lambda x: x.strftime('%d.%m.%Y'),
    'start_date': lambda x: x.strftime('%d.%m.%Y'),
    'end_date': lambda x: x.strftime('%d.%m.%Y') if not(pd.isna(x)) else 'NaT',
    'amount': "{:.2f}".format,
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
        transactions['balance'].min()*.7,
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
    return data[columns].to_string(
        formatters={key: FORMATTERS[key] for key in columns if key in FORMATTERS})


def show_row(data, index, columns):
    result = data.loc[[index], columns].copy()
    formatters = {key: FORMATTERS[key] for key in columns if key in FORMATTERS}

    result[list(formatters)] = result.apply(formatters)
    return result.transpose().to_string()


def show_events(events):
    result = events.copy()
    result.index = result.index.strftime('%d.%m.%Y')
    return 'Регулярные транзакции\n        ' + show_table(result, columns=list(result))


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


def successful_adding_transactions(transactions):
    return f"В базу успешно добавлено {len(transactions[transactions['is_new']])} транзакций.\nРащница прогноза и фактического баланса:"


def predict_info(events, predicted_transactions):
    table = show_events(events)
    return table + f"\n\n\nДополнительно к этим транзакциям, средний расход в день составляет: {predicted_transactions['amount'].mean():.2f}"


HELP_MESSAGE = {
    '/regular add': '!!! /regular add help',
    
}
def reply_help(cmd):
    return HELP_MESSAGE.get(cmd, '!!! default help message')
