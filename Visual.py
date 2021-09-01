import matplotlib.pyplot as plt
import seaborn as sns
import dataframe_image as dfi


def regular_events_plot(regular_events, image_full_name):
    plt.rcParams['figure.figsize'] = (19.5, 9)
    plot = sns.lineplot(data=regular_events['balance']).get_figure()
    plt.subplots_adjust(left=0.08, right=0.97, top=.98, bottom=0.1)
    plot.savefig(image_full_name, dpi=65.65)
    plt.close()


def full_events_plot(regular_events, image_full_name):
    plt.rcParams['figure.figsize'] = (19.5, 9)
    plot = sns.lineplot(data=regular_events['balance']).get_figure()
    plt.subplots_adjust(left=0.08, right=0.97, top=.98, bottom=0.1)
    plot.savefig(image_full_name, dpi=65.65)
    plt.close()


def df_to_image(dataframe, image_full_name):
    dfi.export(dataframe, image_full_name)
