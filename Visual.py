import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns
import dataframe_image as dfi
import io


def costs_plot(costs):
    formatter = DateFormatter('%d.%m.%Y')

    sns.set(font_scale=1.4, style="whitegrid")
    plt.rcParams['figure.figsize'] = (19.5, 9)

    ax = sns.lineplot(data=costs['balance'], linewidth=4.)
    ax.xaxis.set_major_formatter(formatter)

    ax.hlines(costs['balance'].min(), costs.index[0],
              costs.index[-1], color='r', linewidth=3, linestyle='--')
    ax.text(
        costs[costs['balance'] == costs['balance'].min()].index,
        costs['balance'].min()*.8,
        f"Минимальный баланс: {costs['balance'].min():.2f}   ({costs[costs['balance'] == costs['balance'].min()].index[0]:%d.%m.%Y})",
        color='w',
        backgroundcolor='b'
    )

    plt.subplots_adjust(left=0.08, right=0.97, top=.98, bottom=0.1)

    plot_b = io.BytesIO()
    plt.savefig(plot_b, format='png')
    plt.close()
    plot_b.seek(0)
    return plot_b


def df_to_image(dataframe, image_full_name):
    dfi.export(dataframe, image_full_name)
    return open(image_full_name, 'rb')
