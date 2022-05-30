import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns
from datetime import date, datetime
# import dataframe_image as dfi # dataframe-image==0.1.1
import io


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


# def df_to_image(dataframe, image_full_name):
#     # dfi.export(dataframe, image_full_name, table_conversion='matplotlib') # не красивый вид, но не требует хром
#     dfi.export(dataframe, image_full_name)
#     return open(image_full_name, 'rb')

def show_events(events):
    result = events.copy()
    result.index = result.index.strftime('%d.%m.%Y')

    result = result.to_string(formatters={
        # 'date': '{:%d.%m.%Y}'.format,
        'amount': "{:.2f}".format,
        # 'balance': "{:.2f}".format,
    })

    return 'Регулярные транзакции\n        ' + result

def show_onetime(onetime, only_relevant):
    result = onetime.copy()
    if only_relevant:
        result = result[result['date']>=datetime.today()]
    result = result.set_index('date')
    result.index = result.index.strftime('%d.%m.%Y')

    result = result.to_string(formatters={
        'amount': "{:.2f}".format,
    })

    return 'Разовые транзакции\n        ' + result
